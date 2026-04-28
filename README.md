
# Healthcare Data Pipeline

## Overview
This project simulates a healthcare data pipeline that processes medical imaging metadata using an event-driven architecture.

## Goal
- Build a FastAPI backend for receiving medical metadata
- Process data asynchronously using a queue-based pattern
- Prepare for integration with Pub/Sub
- Store structured data in BigQuery (planned)
- Apply data validation based on healthcare domain rules

---

## Architecture

### Current (Local Simulation)

```mermaid
flowchart LR
    Client --> API
    API --> Queue
    Queue --> Worker
````

### Target (Cloud Architecture)

```mermaid
flowchart LR
    Client --> API
    API --> PubSub
    PubSub --> Worker
    Worker --> BigQuery
```

---

## Example JSON Payload

```json
{
  "patient_id": "P123",
  "modality": "CT",
  "study_date": "2026-01-01",
  "slice_thickness": 1.2,
  "device_id": "MRI_001"
}
```

---

## Validation Rules

* modality must be one of: CT / MRI / US
* slice_thickness must be > 0
* study_date must follow YYYY-MM-DD format
* data must be validated before processing

---

## Current Implementation

### API (FastAPI)

* Receives POST /events requests
* Validates medical metadata using Pydantic
* Pushes validated data into an in-memory queue

### Queue (Local Simulation)

* Simple Python list used as a message queue
* Simulates asynchronous processing

### Worker

* Continuously polls the queue
* Processes incoming events

---

## How to Run

### Start API

```bash
cd api
uvicorn main:app --reload
```

### Start Worker

```bash
python worker/worker.py
```

### Send Test Request

```bash
curl -X POST http://127.0.0.1:8000/events \
-H "Content-Type: application/json" \
-d '{"patient_id":"P1","modality":"CT","study_date":"2026-01-01","slice_thickness":1.2,"device_id":"D1"}'
```

---

## Core Concepts

* REST API (FastAPI)
* Event-driven architecture
* Producer / Consumer model
* Message queue (simulated)
* Data validation in healthcare systems

---

## Status

Current phase: **Local event-driven implementation complete**

* FastAPI endpoint implemented
* Validation rules applied
* In-memory queue introduced
* Worker process implemented

Next step: **Replace local queue with Pub/Sub (GCP)**

---

## Future Work

* Integrate Google Cloud Pub/Sub
* Deploy services to Cloud Run
* Store processed data in BigQuery
* Add logging and error handling
