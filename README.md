# Healthcare Data Pipeline

## Overview
This repository now demonstrates a radiology-focused healthcare ingestion pipeline for DICOM metadata. It combines FastAPI, asynchronous queue processing, and privacy-preserving data handling to show how large medical image payloads can be processed safely without blocking the web API.

---

## What changed
- Added a FastAPI endpoint for DICOM metadata ingestion at /dicom/events.
- Implemented automatic de-identification of patient name and birth date before storage or downstream processing.
- Added a lightweight queue layer for DICOM events with a Redis-backed path and an in-memory fallback for local development.
- Added a searchable index endpoint at /dicom/search for clinical metadata retrieval.
- Added a Docker multi-stage image so the service can run consistently in local or cloud environments.

---

## Architecture

```mermaid
flowchart LR
    Client --> API
    API --> Queue
    Queue --> Worker
    Worker --> Index
```

---

## Example DICOM payload

```json
{
  "patient_name": "John Doe",
  "patient_birth_date": "1980-01-01",
  "study_uid": "1.2.826.0.1.3680043.8.498.123456",
  "modality": "CT",
  "kVp": 120,
  "mA": 250,
  "consent_logged": true,
  "source": "PACS"
}
```

---

## Security and compliance notes
- Consent logging is enforced for DICOM intake.
- Only diagnostic-support use is accepted by default to support purpose limitation.
- Sensitive fields are masked before indexing or downstream processing.
- pydicom is used when DICOM bytes are provided for metadata extraction.

---

## Run locally

```bash
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

```bash
python worker/worker.py
```

### Docker

```bash
docker build -t healthcare-data-pipeline .
docker run -p 8000:8000 healthcare-data-pipeline
```

### Tests

```bash
python -m pytest
```

---

## API endpoints
- POST /events for basic clinical metadata ingestion
- POST /dicom/events for anonymized DICOM metadata ingestion
- GET /dicom/search?study_uid=... for indexed result lookup