import json
import logging
import uuid
from datetime import date, datetime
from typing import Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, field_validator

from api.pubsub_client import publish_event
from worker.celery_app import process_dicom_task
from worker.worker import enqueue_dicom_event, get_consent_log_entries, get_dicom_search_results, process_dicom_event

app = FastAPI(title="Healthcare Data Pipeline", version="0.2.0")
CONSENT_LOG: dict[str, dict[str, str | bool]] = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


class Event(BaseModel):
    """
    Medical imaging event schema
    """

    patient_id: str
    modality: Literal["CT", "MRI", "US"]
    study_date: date
    slice_thickness: float
    device_id: str

    @field_validator("slice_thickness")
    def validate_slice_thickness(cls, v):
        if v <= 0:
            raise ValueError("slice_thickness must be > 0")
        if v > 50:
            raise ValueError("slice_thickness is unrealistic")
        return v

    @field_validator("study_date")
    def validate_study_date(cls, v):
        if v > date.today():
            raise ValueError("study_date must not be in the future")
        return v


class DicomIngestionPayload(BaseModel):
    """DICOM ingestion payload for radiology workflows."""

    model_config = ConfigDict(extra="forbid")

    patient_name: str | None = None
    patient_birth_date: date | None = None
    study_uid: str
    modality: Literal["CT", "MRI", "US", "DX", "CR"]
    kVp: float | None = None
    mA: float | None = None
    consent_logged: bool = False
    source: str = "PACS"
    purpose: Literal["diagnostic_support", "research", "marketing"] = "diagnostic_support"

    @field_validator("patient_name")
    def validate_patient_name(cls, v):
        if v is None:
            return v
        if len(v.strip()) < 2:
            raise ValueError("patient_name is too short")
        return v.strip()

    @field_validator("kVp", "mA")
    def validate_technical_values(cls, v):
        if v is not None and v <= 0:
            raise ValueError("technical values must be > 0")
        return v


def require_role(role: str):
    def _checker(request: Request):
        if request.headers.get("x-role", "").lower() != role:
            raise HTTPException(status_code=403, detail="Forbidden")
        return True

    return _checker


def register_consent_log(payload: dict) -> str:
    consent_reference = f"consent-{uuid.uuid4().hex[:8]}"
    CONSENT_LOG[consent_reference] = {
        "purpose": payload.get("purpose", "diagnostic_support"),
        "source": payload.get("source", "PACS"),
        "consent_logged": bool(payload.get("consent_logged", False)),
    }
    return consent_reference


@app.middleware("http")
async def consent_middleware(request: Request, call_next):
    if request.url.path == "/dicom/events":
        try:
            body = await request.body()
            if body:
                payload = json.loads(body.decode("utf-8"))
                if payload.get("consent_logged") is not True:
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "Consent log is required for medical data processing"},
                    )
        except (UnicodeDecodeError, json.JSONDecodeError):
            return JSONResponse(status_code=400, content={"detail": "Invalid JSON body"})
    return await call_next(request)


@app.post("/events")
async def receive_event(event: Event):
    """
    Receive medical event and publish it to Pub/Sub.
    """

    logging.info(
        f"received_event patient_id={event.patient_id} "
        f"modality={event.modality}"
    )

    try:
        event_dict = event.model_dump(mode="json")
        event_dict["event_id"] = str(uuid.uuid4())
        event_dict["received_at"] = datetime.utcnow().isoformat() + "Z"

        message_id = publish_event(event_dict)

        logging.info(
            f"published_event patient_id={event.patient_id} message_id={message_id}"
        )

        return {"status": "published", "message_id": message_id}

    except Exception as e:
        logging.exception(f"failed_to_publish_event error={str(e)}")
        return {"status": "error", "message": "failed to publish event"}


@app.post("/dicom/events")
async def receive_dicom_event(payload: DicomIngestionPayload):
    """Accept DICOM metadata, anonymize it, and queue it for background processing."""

    if payload.purpose != "diagnostic_support":
        return JSONResponse(
            status_code=403,
            content={"status": "rejected", "detail": "Purpose limitation violated"},
        )

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

    event_dict = payload.model_dump(mode="json")
    event_dict["event_id"] = str(uuid.uuid4())
    event_dict["received_at"] = datetime.utcnow().isoformat() + "Z"
    event_dict["deidentified"] = deidentified
    event_dict["consent_reference"] = consent_reference

    publish_event(event_dict)

    processing_result = process_dicom_event(event_dict)

    try:
        celery_result = process_dicom_task.apply_async(args=[event_dict])
        queue_status = {"queue": "celery", "task_id": celery_result.id}
    except Exception as exc:
        logging.warning(f"celery_enqueue_failed error={exc}")
        queue_status = enqueue_dicom_event(event_dict)

    return {
        "status": "queued",
        "event_id": event_dict["event_id"],
        "deidentified": deidentified,
        "consent_reference": consent_reference,
        "queue": queue_status,
        "processing": processing_result,
    }


@app.get("/dicom/search")
async def search_dicom_events(study_uid: str | None = None):
    """Search indexed DICOM processing results by study UID."""

    return get_dicom_search_results(study_uid=study_uid)


@app.get("/admin/consent-log")
async def read_consent_log(request: Request):
    """Expose consent logs for administrative review with a simple RBAC check."""

    require_role("admin")(request)
    return {"consent_log": get_consent_log_entries()}