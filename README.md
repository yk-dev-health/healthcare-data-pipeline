# Healthcare Data Pipeline

## Overview
This project simulates a healthcare data pipeline that processes medical imaging metadata using an event-driven architecture.  
It demonstrates the transition from a local queue-based system to a cloud-native Pub/Sub design.

---

## Goal
- Build a FastAPI backend for receiving medical imaging metadata
- Validate healthcare data using domain constraints
- Simulate asynchronous processing using a local queue
- Design migration path to Google Cloud Pub/Sub
- Prepare for downstream storage (BigQuery, future work)

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
* All data is validated before processing

---

## Current Implementation

### API (FastAPI)

* Receives POST `/events` requests
* Validates medical metadata using Pydantic
* Logs ingestion events
* Pushes validated data into an in-memory queue

### Queue (Local Simulation)

* In-memory Python list used as a temporary message buffer
* Simulates asynchronous decoupling between API and processing layer
* Used only for local development and architecture validation

### Worker

* Continuously polls the in-memory queue
* Processes incoming events sequentially
* Represents a future stateless processing service

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
* Message queue (local simulation)
* Data validation in healthcare systems

---

## Status

Current phase: **Local event-driven system complete**

* FastAPI ingestion layer implemented
* Pydantic validation applied
* Logging enabled
* In-memory queue introduced
* Worker process implemented

---

## Migration Plan

The current in-memory queue will be replaced by:

* Google Cloud Pub/Sub (managed message broker)
* Enables durability, scalability, and decoupling
* Supports multiple independent consumers

Future extensions:

* Cloud Run deployment
* BigQuery storage integration
* Observability (Cloud Logging / Tracing)