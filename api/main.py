from fastapi import FastAPI
from pydantic import BaseModel, field_validator
from typing import Literal

app = FastAPI()

class Event(BaseModel):
    patient_id: str
    modality: Literal["CT", "MRI", "US"]
    study_date: str
    slice_thickness: float
    device_id: str

    @field_validator("slice_thickness")
    def check_slice(cls, v):
        
        if v <= 0:
            raise ValueError("slice_thickness must be > 0")
        return v

@app.post("/events")
async def receive_event(event: Event):
    return {"status": "received", "data": event}