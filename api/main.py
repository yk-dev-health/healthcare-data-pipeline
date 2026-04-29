from fastapi import FastAPI
from pydantic import BaseModel, field_validator
from typing import Literal
import logging

app = FastAPI()

# In-memory queue to simulate asynchronous processing
event_queue = []

class Event(BaseModel):
    # Unique identifier for the patient
    patient_id: str

    # Imaging modality (restricted to valid types)
    modality: Literal["CT", "MRI", "US"]

    # Study date in ISO format (YYYY-MM-DD)
    study_date: str

    # Slice thickness must be a positive value
    slice_thickness: float

    # Identifier for the imaging device
    device_id: str

    @field_validator("slice_thickness")
    def check_slice(cls, v):
        # Ensure slice thickness is greater than zero
        if v <= 0:
            raise ValueError("slice_thickness must be > 0")
        return v


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

@app.post("/events")
async def receive_event(event: Event):
    logging.info(f"received_event patient_id={event.patient_id} modality={event.modality}")
    event_queue.append(event.model_dump())
    return {"status": "queued"}