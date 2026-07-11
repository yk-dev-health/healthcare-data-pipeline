import json
import logging
import os
import uuid
from collections import deque
from datetime import date, datetime, timezone
from typing import Any

import redis
from dotenv import load_dotenv

from worker.data_minimization import (
    PatientPseudonymizer,
    create_minimized_payload,
    create_pii_shadow_record,
    remove_pii_from_dicom_metadata,
    store_sensitive_data_with_ttl,
)
from worker.logger import audit_logger

try:
    from google.cloud import pubsub_v1
except Exception:  # pragma: no cover - fallback for local environments
    pubsub_v1 = None

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

PROJECT_ID = os.getenv("PROJECT_ID", "healthcare-pipeline-yk-01")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "healthcare-sub")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

subscriber = None
subscription_path = None
if pubsub_v1 is not None:
    try:
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    except Exception:  # pragma: no cover - defensive fallback
        subscriber = None
        subscription_path = None

# Redis (dedup store)
r = None
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    r.ping()
except Exception:  # pragma: no cover - fallback when Redis is not running locally
    r = None

DICOM_QUEUE_NAME = "dicom_queue"
DICOM_INDEX: dict[str, dict[str, Any]] = {}
CONSENT_LOG: dict[str, dict[str, Any]] = {}
MEMORY_QUEUE: deque[dict[str, Any]] = deque()
DATA_DIR = os.getenv("DICOM_DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"))
OUTPUT_PATH = os.getenv("DICOM_OUTPUT_PATH", os.path.join(DATA_DIR, "processed_dicom_events.jsonl"))
AUDIT_PATH = os.getenv("DICOM_AUDIT_PATH", os.path.join(DATA_DIR, "audit_log.jsonl"))
os.makedirs(DATA_DIR, exist_ok=True)


def append_jsonl(path: str, payload: dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str) + "\n")


def persist_processed_event(event: dict[str, Any], result: dict[str, Any]) -> None:
    record = {
        "event_id": event.get("event_id"),
        "study_uid": event.get("study_uid"),
        "source": event.get("source"),
        "deidentified": result.get("deidentified"),
        "minimized_payload": result.get("minimized_payload"),
        "processed_at": result.get("processed_at"),
    }
    append_jsonl(OUTPUT_PATH, record)


def persist_audit_log(event: dict[str, Any], result: dict[str, Any]) -> None:
    record = {
        "event_id": event.get("event_id"),
        "consent_reference": event.get("consent_reference"),
        "purpose": event.get("purpose"),
        "source": event.get("source"),
        "deidentified": result.get("deidentified"),
        "processed_at": result.get("processed_at"),
    }
    append_jsonl(AUDIT_PATH, record)


def get_consent_log_entries() -> dict[str, dict[str, Any]]:
    return dict(CONSENT_LOG)


def already_processed(event_id: str) -> bool:
    if r is None:
        return event_id in {item.get("event_id") for item in MEMORY_QUEUE if item.get("event_id")}
    return r.get(event_id) is not None


def mark_processed(event_id: str):
    if r is None:
        return
    r.setex(event_id, 86400, "1")


def parse_date(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    if isinstance(value, date):
        return value
    return None


def enqueue_dicom_event(event: dict) -> dict:
    if r is not None:
        r.lpush(DICOM_QUEUE_NAME, json.dumps(event))
        return {"queue": "redis", "queue_name": DICOM_QUEUE_NAME}

    MEMORY_QUEUE.append(event)
    return {"queue": "memory", "queue_name": DICOM_QUEUE_NAME, "queue_depth": len(MEMORY_QUEUE)}


def process_event(event: dict) -> dict:
    issues = []
    study_date = parse_date(event.get("study_date"))
    if study_date is None:
        issues.append("study_date_invalid")
    elif study_date > date.today():
        issues.append("study_date_in_future")

    slice_thickness = event.get("slice_thickness")
    if slice_thickness is None:
        issues.append("slice_thickness_missing")
    elif not isinstance(slice_thickness, (int, float)):
        issues.append("slice_thickness_type_error")
    elif slice_thickness <= 0:
        issues.append("slice_thickness_nonpositive")
    elif slice_thickness > 50:
        issues.append("slice_thickness_unrealistic")

    modality = event.get("modality")
    if modality not in {"CT", "MRI", "US"}:
        issues.append("modality_invalid")

    patient_id = event.get("patient_id")
    if not patient_id or not isinstance(patient_id, str):
        issues.append("patient_id_missing")
    elif not patient_id.startswith("P"):
        issues.append("patient_id_format")

    quality_score = max(0, 100 - len(issues) * 20)
    return {
        "quality_score": quality_score,
        "issues": issues,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def extract_dicom_metadata(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload

    if isinstance(payload, (bytes, bytearray)):
        try:
            from pydicom import dcmread
            from pydicom.filebase import DicomBytesIO

            ds = dcmread(DicomBytesIO(payload))
            return {
                "patient_name": str(getattr(ds, "PatientName", "")) or None,
                "patient_birth_date": str(getattr(ds, "PatientBirthDate", "")) or None,
                "study_uid": str(getattr(ds, "StudyInstanceUID", "")) or None,
                "modality": str(getattr(ds, "Modality", "")) or None,
                "kVp": float(getattr(ds, "KVP", 0)) if getattr(ds, "KVP", None) not in (None, "") else None,
                "mA": float(getattr(ds, "mA", 0)) if getattr(ds, "mA", None) not in (None, "") else None,
            }
        except Exception as exc:  # pragma: no cover - optional writer environment
            logging.exception(f"failed_to_parse_dicom_bytes error={exc}")
            return {}

    return {}


def build_fhir_resources(event: dict) -> dict[str, dict[str, Any]]:
    study_uid = event.get("study_uid") or "unknown-study"
    patient_id = f"anon-{study_uid.split('.')[-1]}"
    return {
        "Patient": {
            "resourceType": "Patient",
            "id": patient_id,
            "identifier": [{"system": "urn:study-uid", "value": study_uid}],
            "meta": {"tag": [{"system": "urn:privacy", "code": "deidentified"}]},
        },
        "Observation": {
            "resourceType": "Observation",
            "id": f"obs-{patient_id}",
            "status": "final",
            "code": {
                "coding": [{"system": "urn:dicom", "code": "modality"}],
                "text": f"{event.get('modality', 'UNK')} acquisition metadata",
            },
            "valueString": f"source={event.get('source', 'unknown')}|kVp={event.get('kVp')}|mA={event.get('mA')}",
        },
    }


def process_dicom_event(event: dict) -> dict:
    """
    Process DICOM event with GDPR compliance:
    - Principle 3: Data Minimization (remove unnecessary PII)
    - Principle 5: Storage Limitation (TTL management)
    - Principle 7: Accountability (structured audit logging)
    """
    event_id = event.get("event_id") or str(uuid.uuid4())
    patient_id = event.get("patient_id")
    patient_name = event.get("patient_name")
    patient_birth_date = event.get("patient_birth_date")
    study_uid = event.get("study_uid")
    modality = event.get("modality")
    source = event.get("source", "PACS")
    purpose = event.get("purpose", "diagnostic_support")
    consent_logged = event.get("consent_logged", False)

    # Initialize pseudonymizer for audit linkage
    pseudonymizer = PatientPseudonymizer()
    pseudonym_id = pseudonymizer.pseudonymize_patient_id(patient_id) if patient_id else "PS_unknown"

    # Log data ingestion (GDPR Article 13/14: Transparency)
    audit_logger.log_data_ingestion(
        event_id=event_id,
        patient_id=pseudonym_id,
        study_uid=study_uid,
        modality=modality,
        source=source,
        purpose=purpose,
        metadata={"consent_logged": consent_logged},
    )

    # Extract DICOM metadata if provided as bytes
    metadata = extract_dicom_metadata(event.get("dicom_bytes"))
    merged_event = {**metadata, **event}

    # Principle 3: Remove unnecessary PII
    pii_removed_fields = []
    if patient_name:
        pii_removed_fields.append("patient_name")
    if patient_birth_date:
        pii_removed_fields.append("patient_birth_date")

    # Create de-identified payload
    dicom_dict_for_minimization = {
        "patient_name": merged_event.get("patient_name"),
        "patient_birth_date": merged_event.get("patient_birth_date"),
        "patient_id": merged_event.get("patient_id"),
        "study_uid": merged_event.get("study_uid"),
        "modality": merged_event.get("modality"),
        "study_date": merged_event.get("study_date"),
        "institution_name": merged_event.get("institution_name"),
        "referring_physician_name": merged_event.get("referring_physician_name"),
    }
    minimized_metadata = remove_pii_from_dicom_metadata(dicom_dict_for_minimization)

    deidentified = {
        "patient_name": "REDACTED" if merged_event.get("patient_name") else None,
        "patient_birth_date": "REDACTED" if merged_event.get("patient_birth_date") else None,
        "study_uid": merged_event.get("study_uid"),
        "modality": merged_event.get("modality"),
        "kVp": merged_event.get("kVp"),
        "mA": merged_event.get("mA"),
        "source": merged_event.get("source"),
    }

    # Log de-identification (Principle 3: Minimization)
    audit_logger.log_data_deidentification(
        event_id=event_id,
        patient_id=pseudonym_id,
        pseudonym_id=pseudonym_id,
        fields_removed=pii_removed_fields,
        purpose="gdpr_principle_3_minimization",
    )

    # Principle 5: Storage Limitation
    # Create shadow record for audit linkage (90-day retention)
    shadow_record = create_pii_shadow_record(
        patient_id=patient_id,
        patient_name=patient_name,
        patient_birth_date=patient_birth_date,
        study_uid=study_uid,
        event_id=event_id,
        pseudonymizer=pseudonymizer,
    )

    # Store sensitive data with TTL (1-hour for processing)
    if patient_id and patient_name:
        store_sensitive_data_with_ttl(
            key=f"sensitive:patient:{event_id}",
            value={"patient_id": patient_id, "patient_name": patient_name},
            ttl_seconds=3600,  # 1 hour for processing
            purpose="processing",
        )

    # Log retention policy (Principle 5: Storage Limitation)
    audit_logger.log_data_retention(
        event_id=event_id,
        study_uid=study_uid,
        storage_location="redis_ttl_store",
        ttl_seconds=3600,
        retention_policy="automatic_deletion_after_processing",
    )

    # Log consent record (GDPR Article 7: Consent)
    if consent_logged:
        audit_logger.log_consent_record(
            event_id=event_id,
            patient_id=pseudonym_id,
            consent_type="explicit_consent",
            consent_logged=True,
            consent_reference=event.get("consent_reference"),
            purposes=[purpose],
        )

    indexed_fields = {
        "modality": merged_event.get("modality"),
        "source": merged_event.get("source"),
        "study_uid": merged_event.get("study_uid"),
    }

    minimized_payload = create_minimized_payload(merged_event, pseudonym_id)
    fhir_resources = build_fhir_resources(merged_event)

    if merged_event.get("study_uid"):
        DICOM_INDEX[merged_event["study_uid"]] = {
            "deidentified": deidentified,
            "indexed_fields": indexed_fields,
            "minimized_payload": minimized_payload,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

    persist_processed_event(merged_event, {
        "deidentified": deidentified,
        "minimized_payload": minimized_payload,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    })
    persist_audit_log(merged_event, {
        "deidentified": deidentified,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    })

    # Log data access for audit trail
    audit_logger.log_data_access(
        event_id=event_id,
        pseudonym_id=pseudonym_id,
        accessor="worker.process_dicom_event",
        access_type="READ_PROCESS",
        purpose="medical_imaging_analysis",
        result="success",
    )

    return {
        "quality_score": 100,
        "issues": [],
        "event_id": event_id,
        "deidentified": deidentified,
        "indexed_fields": indexed_fields,
        "minimized_payload": minimized_payload,
        "fhir_resources": fhir_resources,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def get_dicom_search_results(study_uid: str | None = None):
    if study_uid:
        return {"study_uid": study_uid, "results": [DICOM_INDEX[study_uid]] if study_uid in DICOM_INDEX else []}
    return {"results": list(DICOM_INDEX.values())}


def callback(message):
    try:
        event = json.loads(message.data.decode("utf-8"))

        event_id = event.get("event_id")
        if not event_id:
            event_id = str(uuid.uuid4())
            event["event_id"] = event_id

        if already_processed(event_id):
            logging.warning(f"duplicate_event_skipped event_id={event_id}")
            message.ack()
            return

        quality_report = process_event(event)

        logging.info(
            f"processing_event event_id={event_id} patient_id={event.get('patient_id')} modality={event.get('modality')} quality_score={quality_report['quality_score']}"
        )

        if quality_report["issues"]:
            logging.warning(
                f"quality_issues event_id={event_id} issues={quality_report['issues']}"
            )

        mark_processed(event_id)
        message.ack()

    except Exception as e:
        logging.exception(f"failed_to_process error={e}")
        message.nack()


def run_worker():
    if subscriber is None or subscription_path is None:
        logging.info("worker_started_without_pubsub")
        return

    logging.info("worker_started")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()


if __name__ == "__main__":
    run_worker()