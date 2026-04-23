from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class Event(BaseModel):
    patient_id: str
    modality: str
    study_date: str
    slice_thickness: float
    device_id: str

@app.post("/events")
async def receive_event(event: Event):
    return {"status": "received", "data": event}