# Healthcare Data Pipeline

## Overview
This project implements a healthcare data pipeline that processes medical imaging metadata using an event-driven architecture.  
It is designed around Google Cloud Pub/Sub for asynchronous event delivery and worker processing.

---

## Goal
- Build a FastAPI backend for receiving medical imaging metadata
- Validate healthcare metadata using domain constraints
- Apply basic clinical data quality checks
- Use Google Cloud Pub/Sub-style asynchronous worker processing
- Prepare for downstream storage (BigQuery, future work)

---

## Architecture

### Current (Cloud-oriented)

```mermaid
flowchart LR
    Client --> API
    API --> PubSub
    PubSub --> Worker
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
* Publishes validated events to Google Cloud Pub/Sub

### Pub/Sub Integration

* Uses `google-cloud-pubsub` for publish/subscribe topology
* Enables cloud-based decoupling between ingestion and processing

### Worker

* Receives messages from Pub/Sub subscription
* Processes events and performs basic healthcare data quality checks
* Applies Redis-based idempotency for duplicate protection
* Represents a stateless processing service for downstream handling

---

## How to Run

### Start API

```bash
cd api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Start Worker

```bash
python worker/worker.py
```

API will be available at `http://127.0.0.1:8000`.

> Note: Docker support has been removed from this repository. Run the API and worker with native Python commands.

### Run tests

```bash
pytest
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
* Google Cloud Pub/Sub-style asynchronous messaging
* Healthcare metadata validation and quality checks

---

## Status

Current phase: **Cloud Pub/Sub-ready event-driven system**

* FastAPI ingestion layer implemented
* Pydantic validation applied, including domain-specific checks
* Logging enabled
* Healthcare data quality evaluation added in worker
* Google Cloud Pub/Sub integration implemented
* Worker process implemented with Redis-based idempotency
* Docker support removed; repository now targets direct Python execution

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