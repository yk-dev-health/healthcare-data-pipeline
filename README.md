# Healthcare Data Pipeline

## Overview
This project demonstrates the design of a healthcare event-driven data pipeline focused on data validation, quality control, idempotent processing, and operational reliability.

The system uses asynchronous messaging and worker-based processing to decouple data ingestion from downstream processing, reflecting patterns commonly used in production data platforms.

---

## Summary
This repository is a portfolio prototype for healthcare data engineering. It demonstrates a validated clinical metadata ingestion API, asynchronous worker processing, basic healthcare quality checks, and automated test coverage.

---

## Goal
- Build a FastAPI backend for receiving medical imaging metadata
- Validate healthcare metadata using domain constraints
- Apply basic clinical data quality checks for imaging metadata
- Demonstrate asynchronous worker processing with Pub/Sub-style architecture
- Prepare for downstream storage and analytics extension

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
* slice_thickness must be > 0 and within a realistic range
* study_date must follow YYYY-MM-DD format and not be in the future
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
* Computes a quality score and logs issues for invalid metadata
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

Current phase: **Healthcare event pipeline prototype**

* FastAPI ingestion layer implemented
* Pydantic validation applied, including domain-specific checks
* Logging enabled
* Healthcare data quality evaluation added in worker
* Google Cloud Pub/Sub integration implemented
* Worker process implemented with Redis-based idempotency
* pytest coverage added for API and worker validation
* Docker support removed; repository now targets direct Python execution

---

## Scope and limitations

This repository is a portfolio prototype, not a full clinical data quality analytics system.

Included:
* Metadata ingestion and validation
* Asynchronous worker pattern
* Quality scoring for incoming events
* Duplicate protection via Redis

Not included:
* Full clinical reporting or data warehousing
* Production GCP deployment configuration
* Real medical imaging file handling (DICOM/FHIR)
* Complete end-to-end analytics pipeline

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