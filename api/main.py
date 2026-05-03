from fastapi import FastAPI
from pydantic import BaseModel, field_validator
from typing import Literal
import logging

app = FastAPI()

# In-memory queue (temporary, will be replaced by Pub/Sub)
event_queue = []

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
    study_date: str
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
    Receive medical event and enqueue it for processing
    """

    # Log incoming request
    logging.info(
        f"received_event patient_id={event.patient_id} "
        f"modality={event.modality}"
    )

    try:
        # Convert to dict (serialization boundary)
        event_dict = event.model_dump()

        # Temporary queue (will be replaced by Pub/Sub publish)
        event_queue.append(event_dict)

        # Log successful enqueue
        logging.info(
            f"queued_event patient_id={event.patient_id} queue_size={len(event_queue)}"
        )

        return {
            "status": "queued",
            "queue_size": len(event_queue)
        }

    except Exception as e:
        # Error visibility for debugging
        logging.error(f"failed_to_queue_event error={str(e)}")
        return {
            "status": "error",
            "message": "failed to process event"
        }