# Healthcare Data Pipeline

## 🧠 Overview
This project simulates a healthcare data pipeline that processes medical imaging metadata using an event-driven architecture.

It is designed to demonstrate backend engineering + data engineering skills using a cloud-style system design.

---

## 🎯 Goal
- Build a FastAPI backend for receiving medical metadata
- Process data asynchronously using Pub/Sub pattern
- Store structured data in BigQuery
- Apply data validation based on healthcare domain rules

---

## 🏗 Architecture

```mermaid
flowchart LR
    Client --> API
    API --> PubSub
    PubSub --> Worker
    Worker --> BigQuery