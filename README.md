# Healthcare Data Pipeline

## Overview
This project simulates a healthcare data pipeline that processes medical imaging metadata using an event-driven architecture.

## Goal
- Build a FastAPI backend for receiving medical metadata
- Process data asynchronously using Pub/Sub pattern
- Store structured data in BigQuery
- Apply data validation based on healthcare domain rules


## Architecture

```mermaid
flowchart LR
    Client --> API
    API --> PubSub
    PubSub --> Worker
    Worker --> BigQuery
```

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

## 📌 Key Design Rules

* modality must be one of: CT / MR / US
* slice_thickness must be > 0
* study_date must follow YYYY-MM-DD format
* data must be validated before processing

## Core Concepts

* REST API (FastAPI)
* Event-driven architecture
* Producer / Consumer model
* Message queue (Pub/Sub)
* Data warehouse (BigQuery)

## Status

Current phase: **Design phase only**
No implementation yet.

Next step: FastAPI backend implementation