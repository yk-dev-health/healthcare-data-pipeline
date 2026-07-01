import json
import logging
import os
import uuid
from collections import deque
from datetime import date, datetime, timezone
from typing import Any

import redis
from dotenv import load_dotenv

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
MEMORY_QUEUE: deque[dict[str, Any]] = deque()


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


def process_dicom_event(event: dict) -> dict:
    metadata = extract_dicom_metadata(event.get("dicom_bytes"))
    merged_event = {**metadata, **event}

    deidentified = {
        "patient_name": "REDACTED" if merged_event.get("patient_name") else None,
        "patient_birth_date": "REDACTED" if merged_event.get("patient_birth_date") else None,
        "study_uid": merged_event.get("study_uid"),
        "modality": merged_event.get("modality"),
        "kVp": merged_event.get("kVp"),
        "mA": merged_event.get("mA"),
        "source": merged_event.get("source"),
    }

    indexed_fields = {
        "modality": merged_event.get("modality"),
        "source": merged_event.get("source"),
        "study_uid": merged_event.get("study_uid"),
    }

    if merged_event.get("study_uid"):
        DICOM_INDEX[merged_event["study_uid"]] = {
            "deidentified": deidentified,
            "indexed_fields": indexed_fields,
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

    return {
        "quality_score": 100,
        "issues": [],
        "deidentified": deidentified,
        "indexed_fields": indexed_fields,
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