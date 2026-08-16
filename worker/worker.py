import json
import logging
import os
import signal
from collections import deque
from datetime import date, datetime, timezone
from typing import Any

from dotenv import load_dotenv

from common.config import get_settings
from common.errors import (
    ConsentMissingError,
    PurposeLimitationError,
    RedisUnavailableError,
    TransientError,
)
from common.idempotency import IdempotencyStore, derive_event_id
from common.quarantine import QuarantineSink
from common.redis_support import get_redis_client
from worker.data_minimization import (
    PatientPseudonymizer,
    create_minimized_payload,
    create_pii_shadow_record,
    remove_pii_from_dicom_metadata,
    store_sensitive_data_with_ttl,
)
from worker.logger import audit_logger
from worker.message_handler import HandlerResult, PubSubMessageHandler

try:
    from google.cloud import pubsub_v1
except Exception:  # pragma: no cover - fallback for local environments
    pubsub_v1 = None

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

settings = get_settings()

PROJECT_ID = settings.pubsub.project_id
SUBSCRIPTION_ID = settings.pubsub.subscription_id
REDIS_HOST = settings.redis.host
REDIS_PORT = settings.redis.port
REDIS_DB = settings.redis.db

# Nothing below performs I/O at import time. The previous version built a
# SubscriberClient and pinged Redis here, which cost ~50 s on a host with no
# Redis listening (no socket timeout was set) and left the process permanently
# degraded if either dependency happened to be down during startup.
r = get_redis_client()
_idempotency = IdempotencyStore(r)
_quarantine = QuarantineSink()

DICOM_QUEUE_NAME = "dicom_queue"

#: Purposes this controller has registered for clinical imaging metadata.
#: UK GDPR Art. 5(1)(b): anything outside this set is a purpose-limitation
#: breach and must be refused, not merely logged.
APPROVED_PURPOSES = frozenset({"diagnostic_support"})
DICOM_INDEX: dict[str, dict[str, Any]] = {}
CONSENT_LOG: dict[str, dict[str, Any]] = {}
MEMORY_QUEUE: deque[dict[str, Any]] = deque()
DATA_DIR = os.getenv("DICOM_DATA_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data"))
OUTPUT_PATH = os.getenv("DICOM_OUTPUT_PATH", os.path.join(DATA_DIR, "processed_dicom_events.jsonl"))
AUDIT_PATH = os.getenv("DICOM_AUDIT_PATH", os.path.join(DATA_DIR, "audit_log.jsonl"))
os.makedirs(DATA_DIR, exist_ok=True)


def append_jsonl(path: str, payload: dict[str, Any]) -> None:
    """Append one JSON record.

    An I/O failure here (disk full, volume detached, permissions) is an
    infrastructure problem, not a bad message, so it is raised as a
    :class:`TransientError`. That distinction is what stops the handler from
    quarantining a perfectly valid clinical event because a disk filled up.
    """
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")
    except OSError as exc:
        raise TransientError(
            "failed to append to processing log",
            code="persistence_write_failed",
            context={"path": os.path.basename(path), "error": type(exc).__name__},
        ) from exc


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
    """Read-only duplicate check.

    Retained for callers that only need a hint. The receive path must use
    :meth:`IdempotencyStore.claim` instead: a plain read is a check-then-act
    race, and two subscribers can both observe "not processed" for the same
    event and both proceed.
    """
    return _idempotency.is_processed(event_id)


def mark_processed(event_id: str) -> bool:
    """Mark an event complete outside the claim protocol.

    Only for tooling and backfills. Normal processing commits through the
    claim returned by :meth:`IdempotencyStore.claim`, so the fencing token can
    be verified before the marker is written.
    """
    key = _idempotency.key_for(event_id)
    try:
        return bool(
            r.setex(key, get_settings().idempotency.completed_ttl_seconds, "done")
        )
    except RedisUnavailableError:
        logging.warning("mark_processed_failed event_id=%s reason=redis_unavailable", event_id)
        return False


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
    """Fallback enqueue used when Celery is unreachable.

    The in-memory deque is a *development* convenience and is reported as such
    in the response, because work parked there is lost on restart. Callers
    surface ``queue == "memory"`` so an operator can tell durable enqueueing
    from best-effort.
    """
    try:
        depth = r.lpush(DICOM_QUEUE_NAME, json.dumps(event, default=str))
        return {"queue": "redis", "queue_name": DICOM_QUEUE_NAME, "queue_depth": depth}
    except RedisUnavailableError:
        logging.warning("enqueue_fallback_to_memory queue=%s reason=redis_unavailable",
                        DICOM_QUEUE_NAME)

    MEMORY_QUEUE.append(event)
    return {
        "queue": "memory",
        "queue_name": DICOM_QUEUE_NAME,
        "queue_depth": len(MEMORY_QUEUE),
        "durable": False,
    }


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

    Raises:
        PurposeLimitationError: Requested purpose is outside the registered
            purposes (Art. 5(1)(b)). Permanent — the event is quarantined and
            a breach record is written; retrying could not make it lawful.
        ConsentMissingError: No recorded consent. Permanent, same reasoning.
    """
    event_id = event.get("event_id") or derive_event_id(event)
    patient_id = event.get("patient_id")
    patient_name = event.get("patient_name")
    patient_birth_date = event.get("patient_birth_date")
    study_uid = event.get("study_uid")
    modality = event.get("modality")
    source = event.get("source", "PACS")
    purpose = event.get("purpose", "diagnostic_support")
    consent_logged = event.get("consent_logged", False)
    consent_reference = event.get("consent_reference")

    # -- lawful-basis gate --------------------------------------------------
    # Defence in depth. The API rejects these at ingress, but the worker also
    # consumes from a topic that replay tooling and future producers can write
    # to, and "the caller already checked" is not a control you can show an
    # auditor. Both checks below raise PermanentError: no amount of retrying
    # makes unlawful processing lawful, so the event is quarantined rather
    # than redelivered.
    if purpose not in APPROVED_PURPOSES:
        audit_logger.log_breach_notification(
            breach_id=event_id,
            affected_patients=1,
            breach_type="purpose_limitation_violation",
            description=f"Rejected at worker; purpose not approved: {purpose}",
            remedial_actions=["event_quarantined", "not_processed"],
        )
        raise PurposeLimitationError(
            "processing purpose is not an approved purpose",
            context={"purpose": str(purpose), "approved": sorted(APPROVED_PURPOSES)},
        )

    if not consent_logged and not consent_reference:
        audit_logger.log_consent_record(
            event_id=event_id,
            patient_id="PS_unresolved",
            consent_type="absent",
            consent_logged=False,
            consent_reference=None,
            purposes=[purpose],
        )
        raise ConsentMissingError(
            "no consent record or consent reference present on event",
            context={"purpose": str(purpose)},
        )

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


def route_event(event: dict[str, Any]) -> dict[str, Any]:
    """Dispatch a validated event to the right processor.

    DICOM ingestion payloads carry a ``study_uid``; generic clinical events do
    not. Quality issues found in a generic event are recorded but do not fail
    the message: a low-quality study is still a real clinical fact, and
    discarding it would lose data that the quality report exists to flag.
    """
    if event.get("study_uid"):
        return process_dicom_event(event)

    report = process_event(event)
    if report["issues"]:
        logging.warning(
            "quality_issues event_id=%s score=%s issues=%s",
            event.get("event_id"),
            report["quality_score"],
            report["issues"],
        )
    return report


def build_message_handler(
    processor: Any = None, **overrides: Any
) -> PubSubMessageHandler:
    """Construct the receive-path handler with this module's dependencies."""
    return PubSubMessageHandler(
        processor=processor or route_event,
        idempotency=overrides.pop("idempotency", _idempotency),
        quarantine=overrides.pop("quarantine", _quarantine),
        settings=overrides.pop("settings", get_settings()),
        **overrides,
    )


#: Built once; holds no connections of its own, so this is import-safe.
_handler = build_message_handler()


def callback(message) -> HandlerResult:
    """Pub/Sub streaming-pull callback.

    All delivery semantics live in :class:`~worker.message_handler.PubSubMessageHandler`.
    The only job left here is to guarantee the message is settled even if the
    handler itself throws: an unsettled message silently stalls a flow-control
    slot until the ack deadline expires, which under load looks like the worker
    quietly getting slower for no visible reason.
    """
    try:
        return _handler.handle(message)
    except Exception as exc:  # noqa: BLE001 - last line of defence
        logging.exception("handler_crashed error=%s", type(exc).__name__)
        try:
            message.nack()
        except Exception:  # pragma: no cover - broker already gone
            logging.debug("nack_failed_after_handler_crash", exc_info=True)
        from worker.message_handler import Outcome

        return HandlerResult(Outcome.RETRY, acked=False, error_code="handler_crashed")


def build_subscriber():
    """Create the Pub/Sub subscriber on demand.

    Deliberately not done at import: constructing a ``SubscriberClient``
    resolves credentials and can block for seconds, which turns a missing
    credential into an unexplained import hang instead of a clear startup log
    line.
    """
    if pubsub_v1 is None:
        return None, None
    try:
        subscriber = pubsub_v1.SubscriberClient()
        return subscriber, subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    except Exception as exc:  # pragma: no cover - environment dependent
        logging.error("subscriber_init_failed error=%s", type(exc).__name__)
        return None, None


def run_worker() -> None:
    """Run the streaming pull loop until interrupted."""
    subscriber, subscription_path = build_subscriber()
    if subscriber is None or subscription_path is None:
        logging.info("worker_started_without_pubsub")
        return

    cfg = get_settings().pubsub

    # Flow control caps how much work one subscriber may lease at once. Without
    # it a burst hands the process more messages than it can finish inside the
    # ack deadline; those leases expire, everything is redelivered, and the
    # backlog grows while the worker appears busy.
    flow_control = pubsub_v1.types.FlowControl(
        max_messages=cfg.max_outstanding_messages,
        max_bytes=cfg.max_outstanding_bytes,
    )

    logging.info(
        "worker_started subscription=%s max_messages=%d fail_mode=%s",
        SUBSCRIPTION_ID,
        cfg.max_outstanding_messages,
        get_settings().idempotency.fail_mode,
    )

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback, flow_control=flow_control
    )

    def _shutdown(signum, _frame):
        # Graceful drain: stop pulling new work, let in-flight callbacks finish
        # and settle their messages. Killing mid-callback would leave leases to
        # time out and every in-flight event to be redelivered.
        logging.info("worker_shutdown_requested signal=%s", signum)
        streaming_pull_future.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _shutdown)
        except (ValueError, OSError):  # pragma: no cover - non-main thread
            logging.debug("signal_handler_not_installed signal=%s", sig)

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result(timeout=30)
    finally:
        subscriber.close()
        r.close()
        logging.info("worker_stopped")


if __name__ == "__main__":
    run_worker()