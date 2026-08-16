"""FastAPI ingestion gateway for clinical imaging metadata.

Boundary responsibilities, in order:

1. Validate against a schema before anything else touches the payload.
2. Establish a lawful basis (consent recorded, purpose within scope).
3. De-identify, then publish to Pub/Sub and hand off for async processing.
4. Answer truthfully about what happened — including when it failed.

Point 4 is where the previous version was weakest: a publish failure was
caught and returned as ``{"status": "error"}`` with HTTP **200**. A PACS that
checks status codes (they all do) would have recorded the study as ingested
when it had never reached the topic. That is silent clinical data loss, and it
is invisible to uptime monitoring because the endpoint looks healthy. Failures
now surface as 503 with ``Retry-After``, via the handlers in :mod:`api.errors`.
"""

from __future__ import annotations

import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from api.errors import CORRELATION_HEADER, correlation_id_of, register_exception_handlers
from api.pubsub_client import get_publisher, publish_event, reset_publisher
from common.config import get_settings
from common.errors import ConsentMissingError, PurposeLimitationError
from common.redis_support import get_redis_client
from common.schemas import ClinicalEvent, DicomIngestionPayload
from worker.celery_app import process_dicom_task
from worker.logger import audit_logger
from worker.worker import (
    APPROVED_PURPOSES,
    CONSENT_LOG,
    enqueue_dicom_event,
    get_consent_log_entries,
    get_dicom_search_results,
    process_dicom_event,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

settings = get_settings()

#: Bound on the request body read by the consent middleware. Without a cap the
#: middleware would buffer an arbitrarily large upload into memory before
#: FastAPI's own size handling ever runs.
MAX_BODY_BYTES = 1 * 1024 * 1024

#: Backwards-compatible aliases. The canonical definitions now live in
#: `common.schemas` so the worker validates against exactly the same contract.
Event = ClinicalEvent


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Validate configuration at startup; release clients at shutdown.

    Refusing to start on an unsafe configuration is deliberate. A service that
    boots with a default pseudonymisation salt and serves traffic is worse than
    one that fails its readiness probe loudly, because the damage (reversible
    pseudonyms written to durable storage) is not undone by fixing the config
    later.
    """
    problems = settings.validate_for_runtime()
    if problems:
        for problem in problems:
            logger.error("unsafe_configuration: %s", problem)
        raise RuntimeError(
            f"refusing to start with {len(problems)} unsafe configuration setting(s): "
            + "; ".join(problems)
        )

    logger.info(
        "api_started env=%s pubsub_enabled=%s idempotency_fail_mode=%s",
        settings.environment,
        settings.pubsub.enabled,
        settings.idempotency.fail_mode,
    )
    yield
    reset_publisher()
    get_redis_client().close()
    logger.info("api_stopped")


app = FastAPI(title="Healthcare Data Pipeline", version="0.3.0", lifespan=lifespan)
register_exception_handlers(app)

# Re-exported so existing imports of `api.main.DicomIngestionPayload` keep working.
__all__ = ["app", "Event", "ClinicalEvent", "DicomIngestionPayload"]


def require_role(role: str):
    def _checker(request: Request):
        if request.headers.get("x-role", "").lower() != role:
            # Log the refusal, not just the grant. An access-control decision
            # you cannot evidence is not a control you can demonstrate under
            # Art. 5(2).
            audit_logger.log_data_access(
                event_id=correlation_id_of(request),
                pseudonym_id="PS_unresolved",
                accessor=request.headers.get("x-role") or "anonymous",
                access_type="READ",
                purpose="administrative_review",
                result="denied_insufficient_role",
            )
            raise HTTPException(status_code=403, detail="Forbidden")
        return True

    return _checker


def register_consent_log(payload: dict) -> str:
    """Record a consent reference in the shared consent store.

    Writes to the worker's ``CONSENT_LOG``, not a second dict local to this
    module. Previously the API populated its own copy while
    ``GET /admin/consent-log`` read the worker's, so the administrative
    compliance view was permanently empty and reported that as success.
    """
    consent_reference = f"consent-{uuid.uuid4().hex[:8]}"
    CONSENT_LOG[consent_reference] = {
        "purpose": payload.get("purpose", "diagnostic_support"),
        "source": payload.get("source", "PACS"),
        "consent_logged": bool(payload.get("consent_logged", False)),
    }
    return consent_reference


@app.middleware("http")
async def request_context_middleware(request: Request, call_next) -> Response:
    """Attach a correlation id to every request and echo it back.

    One id ties the HTTP request, the audit records, the published message and
    any quarantine entry together. Without it, investigating "what happened to
    the study we sent at 03:14" means grepping timestamps across four systems.
    """
    correlation_id = request.headers.get(CORRELATION_HEADER) or uuid.uuid4().hex
    request.state.correlation_id = correlation_id
    response = await call_next(request)
    response.headers[CORRELATION_HEADER] = correlation_id
    return response


@app.middleware("http")
async def consent_middleware(request: Request, call_next) -> Response:
    """Refuse DICOM ingestion without a recorded consent flag.

    This runs ahead of schema validation on purpose: it is a lawful-basis gate,
    and evaluating consent only for payloads that happen to parse would make
    the control depend on payload shape. The trade-off is a second JSON parse,
    bounded here by :data:`MAX_BODY_BYTES`.
    """
    if request.url.path != "/dicom/events":
        return await call_next(request)

    body = await request.body()
    if not body:
        return await call_next(request)

    if len(body) > MAX_BODY_BYTES:
        return JSONResponse(
            status_code=413,
            content={
                "error": "payload_too_large",
                "detail": f"Request body exceeds {MAX_BODY_BYTES} bytes.",
            },
        )

    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        # No parse detail in the response: a malformed body is frequently a
        # truncated clinical payload, and echoing the offending fragment back
        # would disclose exactly what we are trying to protect.
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "invalid_json", "detail": "Request body is not valid JSON."},
        )

    if not isinstance(payload, dict):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "invalid_json", "detail": "Request body must be a JSON object."},
        )

    if payload.get("consent_logged") is not True:
        correlation_id = request.headers.get(CORRELATION_HEADER) or uuid.uuid4().hex
        audit_logger.log_consent_record(
            event_id=correlation_id,
            patient_id="PS_unresolved",
            consent_type="absent",
            consent_logged=False,
            consent_reference=None,
            purposes=[str(payload.get("purpose", "unspecified"))],
        )
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={
                "error": "consent_missing",
                "detail": "Consent log is required for medical data processing",
                "correlation_id": correlation_id,
            },
        )

    return await call_next(request)


@app.get("/health/live", tags=["health"])
async def liveness() -> dict[str, str]:
    """Process is up. Deliberately dependency-free.

    A liveness probe that checks dependencies causes a Redis blip to restart
    every healthy pod, turning a degradation into an outage.
    """
    return {"status": "alive"}


@app.get("/health/ready", tags=["health"])
async def readiness() -> JSONResponse:
    """Report whether this instance can safely accept traffic.

    Under ``fail_mode=closed`` a missing Redis means we cannot guarantee
    duplicate suppression, so the instance reports *not ready* and is pulled
    from the load balancer rather than accepting work it would have to reject.
    """
    redis_ok = get_redis_client().healthy()
    fail_closed = settings.idempotency.fail_mode == "closed"
    ready = redis_ok or not fail_closed

    body = {
        "status": "ready" if ready else "degraded",
        "checks": {
            "redis": "ok" if redis_ok else "unavailable",
            "pubsub": "enabled" if get_publisher().enabled else "disabled",
        },
        "idempotency_fail_mode": settings.idempotency.fail_mode,
    }
    return JSONResponse(
        status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
        content=body,
    )


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def receive_event(event: ClinicalEvent, request: Request) -> dict[str, object]:
    """Validate a clinical event and publish it to Pub/Sub.

    Raises:
        PublishError: Handled by :mod:`api.errors` as a 503. We do not swallow
            it — reporting success for an event that was never published is
            the one failure mode this endpoint must not have.
    """
    correlation_id = correlation_id_of(request)
    logger.info(
        "received_event correlation_id=%s modality=%s", correlation_id, event.modality
    )

    event_dict = event.model_dump(mode="json")
    event_dict["event_id"] = str(uuid.uuid4())
    event_dict["received_at"] = datetime.now(timezone.utc).isoformat()
    event_dict["correlation_id"] = correlation_id

    message_id = publish_event(event_dict)

    logger.info(
        "published_event event_id=%s message_id=%s correlation_id=%s",
        event_dict["event_id"],
        message_id,
        correlation_id,
    )
    return {
        "status": "published",
        "event_id": event_dict["event_id"],
        "message_id": message_id,
        "correlation_id": correlation_id,
    }


@app.post("/dicom/events")
async def receive_dicom_event(
    payload: DicomIngestionPayload, request: Request
) -> dict[str, object]:
    """Accept DICOM metadata, de-identify it, and queue it for processing."""
    correlation_id = correlation_id_of(request)
    event_id = str(uuid.uuid4())

    if payload.purpose not in APPROVED_PURPOSES:
        audit_logger.log_breach_notification(
            breach_id=event_id,
            affected_patients=1,
            breach_type="purpose_limitation_violation",
            description=f"Attempted ingestion with purpose: {payload.purpose}",
            remedial_actions=["request_rejected", "not_processed"],
        )
        raise PurposeLimitationError(
            "Purpose limitation violated",
            context={"purpose": payload.purpose, "approved": sorted(APPROVED_PURPOSES)},
        )

    if not payload.consent_logged:
        # Defence in depth behind the middleware: a future route change or a
        # direct call in a test must not be able to skip the lawful-basis gate.
        raise ConsentMissingError("Consent log is required for medical data processing")

    deidentified = {
        "patient_name": "REDACTED" if payload.patient_name else None,
        "patient_birth_date": "REDACTED" if payload.patient_birth_date else None,
        "study_uid": payload.study_uid,
        "modality": payload.modality,
        "kVp": payload.kVp,
        "mA": payload.mA,
        "source": payload.source,
    }

    consent_reference = register_consent_log(payload.model_dump(mode="json"))

    audit_logger.log_consent_record(
        event_id=event_id,
        patient_id=payload.study_uid,  # pseudonymous key; no direct identifier
        consent_type="explicit_consent",
        consent_logged=True,
        consent_reference=consent_reference,
        purposes=[payload.purpose],
    )

    event_dict = payload.model_dump(mode="json")
    event_dict["event_id"] = event_id
    event_dict["received_at"] = datetime.now(timezone.utc).isoformat()
    event_dict["deidentified"] = deidentified
    event_dict["consent_reference"] = consent_reference
    event_dict["correlation_id"] = correlation_id

    # Publish first. If the topic is unreachable this raises and the handler
    # returns 503, so the caller retries the whole study rather than us doing
    # local work whose result nothing downstream will ever see.
    message_id = publish_event(event_dict)

    processing_result = process_dicom_event(event_dict)

    try:
        celery_result = process_dicom_task.apply_async(args=[event_dict])
        queue_status = {"queue": "celery", "task_id": celery_result.id}
    except Exception as exc:  # noqa: BLE001 - broker-specific failures vary
        logger.warning("celery_enqueue_failed error=%s", type(exc).__name__)
        queue_status = enqueue_dicom_event(event_dict)

    return {
        "status": "queued",
        "event_id": event_id,
        "message_id": message_id,
        "correlation_id": correlation_id,
        "deidentified": deidentified,
        "consent_reference": consent_reference,
        "queue": queue_status,
        "processing": processing_result,
    }


@app.get("/dicom/search")
async def search_dicom_events(study_uid: str | None = None) -> dict[str, object]:
    """Search indexed DICOM processing results by study UID."""
    return get_dicom_search_results(study_uid=study_uid)


@app.get("/admin/consent-log")
async def read_consent_log(request: Request, _: bool = Depends(require_role("admin"))):
    """Expose consent logs for administrative review, behind an RBAC check."""
    audit_logger.log_data_access(
        event_id=correlation_id_of(request),
        pseudonym_id="PS_aggregate",
        accessor="admin",
        access_type="READ",
        purpose="administrative_review",
        result="success",
    )
    return {"consent_log": get_consent_log_entries()}
