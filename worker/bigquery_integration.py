"""
BigQuery Integration Module - Healthcare Data Pipeline

Implements GDPR Principle 5 (Storage Limitation) with BigQuery:
- Automatic partition deletion based on retention policies
- De-identified data warehousing
- Compliance audit trails
- Cost optimization through time-based retention

Example usage:
    from worker.bigquery_integration import BigQueryDataWarehouse
    
    bq = BigQueryDataWarehouse(project_id="healthcare-pipeline")
    
    # Store processed event
    bq.insert_processed_event(
        event_id="evt-123",
        pseudonym_id="PS_abc123",
        study_uid="1.2.3.4.5",
        modality="CT",
        created_at="2024-01-15"
    )
    
    # Query recent events
    events = bq.query_events_by_modality(
        modality="CT",
        days_back=7
    )
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from dotenv import load_dotenv

try:
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound, AlreadyExists
except ImportError:
    bigquery = None
    NotFound = None
    AlreadyExists = None

load_dotenv()

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "healthcare-pipeline-yk-01")
DATASET_ID = os.getenv("BQ_DATASET_ID", "healthcare_dicom")
TABLE_PROCESSED_EVENTS = "processed_dicom_events"
TABLE_AUDIT_TRAIL = "gdpr_audit_trail"

# GDPR Principle 5: Storage Limitation
# Retention periods in days
RETENTION_PROCESSED_EVENTS_DAYS = int(os.getenv("BQ_RETENTION_PROCESSED_EVENTS", "90"))
RETENTION_AUDIT_LOGS_DAYS = int(os.getenv("BQ_RETENTION_AUDIT_LOGS", "2555"))  # 7 years for compliance
RETENTION_SHADOW_RECORDS_DAYS = int(os.getenv("BQ_RETENTION_SHADOW_RECORDS", "90"))


class BigQueryDataWarehouse:
    """
    GDPR-compliant BigQuery data warehouse for healthcare DICOM data.
    
    Implements:
    - Principle 5: Automatic data deletion via partition expiration
    - Principle 7: Comprehensive audit logging
    - Data minimization: Only stores de-identified pseudonymized data
    """

    def __init__(self, project_id: str = PROJECT_ID):
        """
        Initialize BigQuery client.
        
        Args:
            project_id: GCP project ID
        """
        if bigquery is None:
            logger.warning("google-cloud-bigquery not installed; BigQuery disabled")
            self.client = None
            return

        try:
            self.client = bigquery.Client(project=project_id)
            self.project_id = project_id
            self._ensure_dataset_and_tables_exist()
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            self.client = None

    def _ensure_dataset_and_tables_exist(self):
        """Create dataset and tables if they don't exist."""
        if self.client is None:
            return

        # Create dataset
        dataset_id_full = f"{self.project_id}.{DATASET_ID}"
        dataset = bigquery.Dataset(dataset_id_full)
        dataset.location = "EU"  # GDPR compliance: EU data center

        try:
            dataset = self.client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {DATASET_ID} ensured to exist")
        except Exception as e:
            logger.error(f"Failed to create dataset: {e}")
            return

        # Create processed events table
        self._create_processed_events_table()
        
        # Create audit trail table
        self._create_audit_trail_table()

    def _create_processed_events_table(self):
        """Create processed_dicom_events table with partition expiration."""
        if self.client is None:
            return

        table_id = f"{self.project_id}.{DATASET_ID}.{TABLE_PROCESSED_EVENTS}"

        schema = [
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pseudonym_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("study_uid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("modality", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("kVp", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("mA", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        
        # GDPR Principle 5: Set partition expiration for automatic deletion
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="created_at",
            expiration_ms=(RETENTION_PROCESSED_EVENTS_DAYS * 24 * 60 * 60 * 1000),
        )

        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Table {TABLE_PROCESSED_EVENTS} ensured with {RETENTION_PROCESSED_EVENTS_DAYS}-day expiration")
        except Exception as e:
            logger.error(f"Failed to create table {TABLE_PROCESSED_EVENTS}: {e}")

    def _create_audit_trail_table(self):
        """Create GDPR audit trail table with 7-year retention."""
        if self.client is None:
            return

        table_id = f"{self.project_id}.{DATASET_ID}.{TABLE_AUDIT_TRAIL}"

        schema = [
            bigquery.SchemaField("audit_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pseudonym_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("action", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("purpose", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("accessor", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("result", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("metadata", "JSON", mode="NULLABLE"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        
        # GDPR Principle 5 & Regulatory requirement: 7-year audit retention
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp",
            expiration_ms=(RETENTION_AUDIT_LOGS_DAYS * 24 * 60 * 60 * 1000),
        )

        try:
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Table {TABLE_AUDIT_TRAIL} ensured with {RETENTION_AUDIT_LOGS_DAYS}-day retention")
        except Exception as e:
            logger.error(f"Failed to create table {TABLE_AUDIT_TRAIL}: {e}")

    def insert_processed_event(
        self,
        event_id: str,
        pseudonym_id: str,
        study_uid: str,
        modality: str,
        source: str,
        kVp: Optional[float] = None,
        mA: Optional[float] = None,
    ) -> bool:
        """
        Insert processed DICOM event into BigQuery.
        
        Data is automatically deleted after RETENTION_PROCESSED_EVENTS_DAYS
        due to partition expiration.
        
        Args:
            event_id: Unique event identifier
            pseudonym_id: De-identified patient pseudonym (not reversible)
            study_uid: DICOM study UID
            modality: Imaging modality (CT, MRI, etc.)
            source: Data source (PACS, etc.)
            kVp: Tube voltage
            mA: Tube current
            
        Returns:
            True if insert successful, False otherwise
        """
        if self.client is None:
            logger.warning("BigQuery client not available")
            return False

        table_id = f"{self.project_id}.{DATASET_ID}.{TABLE_PROCESSED_EVENTS}"
        
        rows_to_insert = [
            {
                "event_id": event_id,
                "pseudonym_id": pseudonym_id,
                "study_uid": study_uid,
                "modality": modality,
                "source": source,
                "kVp": kVp,
                "mA": mA,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
        ]

        try:
            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                logger.error(f"Failed to insert rows: {errors}")
                return False
            logger.info(f"Event {event_id} inserted to BigQuery")
            return True
        except Exception as e:
            logger.error(f"Error inserting row: {e}")
            return False

    def insert_audit_entry(
        self,
        audit_id: str,
        event_type: str,
        event_id: str,
        pseudonym_id: Optional[str] = None,
        action: Optional[str] = None,
        purpose: Optional[str] = None,
        accessor: Optional[str] = None,
        result: str = "success",
        metadata: Optional[dict] = None,
    ) -> bool:
        """
        Insert audit trail entry into BigQuery.
        
        Retained for RETENTION_AUDIT_LOGS_DAYS for regulatory compliance.
        
        Args:
            audit_id: Unique audit entry ID
            event_type: Type of event (data_ingestion, data_deidentification, etc.)
            event_id: Associated event ID
            pseudonym_id: Pseudonym ID (if applicable)
            action: Action taken (READ, WRITE, DELETE, etc.)
            purpose: Purpose of action
            accessor: Who accessed (service name, user ID, etc.)
            result: Result of action (success, failure, denied)
            metadata: Additional metadata
            
        Returns:
            True if insert successful, False otherwise
        """
        if self.client is None:
            logger.warning("BigQuery client not available")
            return False

        table_id = f"{self.project_id}.{DATASET_ID}.{TABLE_AUDIT_TRAIL}"
        
        rows_to_insert = [
            {
                "audit_id": audit_id,
                "event_type": event_type,
                "event_id": event_id,
                "pseudonym_id": pseudonym_id,
                "action": action,
                "purpose": purpose,
                "accessor": accessor,
                "result": result,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": json.dumps(metadata) if metadata else None,
            }
        ]

        try:
            errors = self.client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                logger.error(f"Failed to insert audit entry: {errors}")
                return False
            logger.info(f"Audit entry {audit_id} inserted")
            return True
        except Exception as e:
            logger.error(f"Error inserting audit entry: {e}")
            return False

    def query_events_by_modality(
        self,
        modality: str,
        days_back: int = 7,
    ) -> Optional[list[dict]]:
        """
        Query processed events by modality within a time window.
        
        Args:
            modality: Imaging modality (CT, MRI, US, etc.)
            days_back: Number of days to look back
            
        Returns:
            List of events or None if query failed
        """
        if self.client is None:
            return None

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)

        query = f"""
        SELECT
            event_id,
            pseudonym_id,
            study_uid,
            modality,
            source,
            kVp,
            mA,
            created_at,
            processed_at
        FROM `{self.project_id}.{DATASET_ID}.{TABLE_PROCESSED_EVENTS}`
        WHERE modality = @modality
          AND created_at >= @cutoff_date
        ORDER BY created_at DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("modality", "STRING", modality),
                bigquery.ScalarQueryParameter("cutoff_date", "TIMESTAMP", cutoff_date),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error querying events: {e}")
            return None

    def get_audit_trail_for_event(
        self,
        event_id: str,
    ) -> Optional[list[dict]]:
        """
        Retrieve complete audit trail for a specific event.
        
        Useful for GDPR Article 15 (Right of Access) requests.
        
        Args:
            event_id: Event ID to audit
            
        Returns:
            List of audit entries for the event
        """
        if self.client is None:
            return None

        query = f"""
        SELECT
            audit_id,
            event_type,
            action,
            accessor,
            result,
            timestamp
        FROM `{self.project_id}.{DATASET_ID}.{TABLE_AUDIT_TRAIL}`
        WHERE event_id = @event_id
        ORDER BY timestamp ASC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("event_id", "STRING", event_id),
            ]
        )

        try:
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error retrieving audit trail: {e}")
            return None

    def get_retention_policy_status(self) -> dict[str, Any]:
        """
        Get current retention policy configuration.
        
        Returns:
            Dictionary with retention settings
        """
        return {
            "processed_events_days": RETENTION_PROCESSED_EVENTS_DAYS,
            "audit_logs_days": RETENTION_AUDIT_LOGS_DAYS,
            "shadow_records_days": RETENTION_SHADOW_RECORDS_DAYS,
            "tables": {
                TABLE_PROCESSED_EVENTS: f"{RETENTION_PROCESSED_EVENTS_DAYS} days",
                TABLE_AUDIT_TRAIL: f"{RETENTION_AUDIT_LOGS_DAYS} days (7 years)",
            },
        }


# Integration with existing worker
def process_dicom_event_with_bigquery(event: dict) -> dict:
    """
    Example integration: Process DICOM event and store in BigQuery.
    
    Args:
        event: DICOM event dictionary
        
    Returns:
        Processing result
    """
    bq = BigQueryDataWarehouse()
    
    event_id = event.get("event_id")
    pseudonym_id = event.get("pseudonym_id")
    
    # Insert processed event
    if pseudonym_id:
        bq.insert_processed_event(
            event_id=event_id,
            pseudonym_id=pseudonym_id,
            study_uid=event.get("study_uid"),
            modality=event.get("modality"),
            source=event.get("source", "PACS"),
            kVp=event.get("kVp"),
            mA=event.get("mA"),
        )
    
    # Insert audit entry
    bq.insert_audit_entry(
        audit_id=f"audit-{event_id}",
        event_type="data_ingestion",
        event_id=event_id,
        pseudonym_id=pseudonym_id,
        action="PROCESS",
        purpose=event.get("purpose", "diagnostic_support"),
        accessor="worker.process_dicom_event",
        result="success",
    )
    
    return {"status": "stored_in_bigquery", "event_id": event_id}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Example usage
    bq = BigQueryDataWarehouse()
    
    if bq.client is not None:
        # Test insert
        result = bq.insert_processed_event(
            event_id="test-evt-001",
            pseudonym_id="PS_test123",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
            kVp=120,
            mA=250,
        )
        print(f"Insert result: {result}")
        
        # Test query
        events = bq.query_events_by_modality("CT", days_back=1)
        print(f"Query result: {events}")
        
        # Retention policy
        policy = bq.get_retention_policy_status()
        print(f"Retention policy: {json.dumps(policy, indent=2)}")
    else:
        print("BigQuery client not available (google-cloud-bigquery not installed)")
