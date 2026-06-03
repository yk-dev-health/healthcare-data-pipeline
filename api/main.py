from datetime import date, datetime
import logging
import uuid

from fastapi import FastAPI
from pydantic import BaseModel, field_validator
from typing import Literal

from api.pubsub_client import publish_event

app = FastAPI()

# ----------------------------
# Logging configuration
# ----------------------------
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
        # Ensure slice thickness is physically valid
        if v <= 0:
            raise ValueError("slice_thickness must be > 0")
        return v


@app.post("/events")
async def receive_event(event: Event):
    """
    Receive medical event and publish it to Pub/Sub
    """

    # Log incoming request
    logging.info(
        f"received_event patient_id={event.patient_id} "
        f"modality={event.modality}"
    )

    try:
        # Convert Pydantic model to JSON-safe dictionary
        event_dict = event.model_dump(mode="json")
        event_dict["event_id"] = str(uuid.uuid4())
        event_dict["received_at"] = datetime.utcnow().isoformat() + "Z"

        # Publish event to Google Cloud Pub/Sub
        message_id = publish_event(event_dict)

        # Log successful publish
        logging.info(
            f"published_event "
            f"patient_id={event.patient_id} "
            f"message_id={message_id}"
        )

        return {
            "status": "published",
            "message_id": message_id
        }

    except Exception as e:
        # Log full error details
        logging.exception(f"failed_to_publish_event error={str(e)}")

        return {
            "status": "error",
            "message": "failed to publish event"
        }