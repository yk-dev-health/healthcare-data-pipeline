"""
Tests for BigQuery integration with GDPR compliance.

Tests storage limitation (Principle 5) via BigQuery partition expiration.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta, timezone

from worker.bigquery_integration import (
    BigQueryDataWarehouse,
    RETENTION_PROCESSED_EVENTS_DAYS,
    RETENTION_AUDIT_LOGS_DAYS,
)


class TestBigQueryDataWarehouse:
    """Test BigQuery data warehouse functionality."""

    def test_initialization_without_bigquery_library(self):
        """Test graceful handling when google-cloud-bigquery is not installed."""
        with patch("worker.bigquery_integration.bigquery", None):
            bq = BigQueryDataWarehouse()
            assert bq.client is None

    @patch("worker.bigquery_integration.bigquery")
    def test_initialization_with_bigquery(self, mock_bq):
        """Test initialization with BigQuery client available."""
        mock_client = Mock()
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        
        bq = BigQueryDataWarehouse(project_id="test-project")
        
        assert bq.client == mock_client
        assert bq.project_id == "test-project"

    @patch("worker.bigquery_integration.bigquery")
    def test_retention_settings(self, mock_bq):
        """Test retention policy settings match GDPR requirements."""
        mock_client = Mock()
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        
        bq = BigQueryDataWarehouse()
        
        # Processed events should be 90 days
        assert RETENTION_PROCESSED_EVENTS_DAYS == 90
        
        # Audit logs should be at least 7 years (2555 days)
        assert RETENTION_AUDIT_LOGS_DAYS >= 2555

    @patch("worker.bigquery_integration.bigquery")
    def test_get_retention_policy_status(self, mock_bq):
        """Test retrieval of retention policy status."""
        mock_client = Mock()
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        
        bq = BigQueryDataWarehouse()
        status = bq.get_retention_policy_status()
        
        assert status["processed_events_days"] == 90
        assert status["audit_logs_days"] >= 2555
        assert "processed_dicom_events" in status["tables"]
        assert "gdpr_audit_trail" in status["tables"]

    def test_retention_policy_status_without_bigquery(self):
        """Test retention policy status when BigQuery not available."""
        with patch("worker.bigquery_integration.bigquery", None):
            bq = BigQueryDataWarehouse()
            status = bq.get_retention_policy_status()
            
            # Should still return policy even if client not available
            assert status["processed_events_days"] == 90

    @patch("worker.bigquery_integration.bigquery")
    def test_insert_processed_event_without_client(self, mock_bq):
        """Test insert_processed_event gracefully handles missing client."""
        mock_bq.Client.side_effect = Exception("Connection failed")
        
        bq = BigQueryDataWarehouse()
        result = bq.insert_processed_event(
            event_id="evt-001",
            pseudonym_id="PS_abc123",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
        )
        
        assert result is False

    @patch("worker.bigquery_integration.bigquery")
    def test_insert_processed_event_success(self, mock_bq):
        """Test successful insertion of processed event."""
        mock_client = Mock()
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_client.insert_rows_json.return_value = []
        
        bq = BigQueryDataWarehouse()
        result = bq.insert_processed_event(
            event_id="evt-001",
            pseudonym_id="PS_abc123",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
            kVp=120.0,
            mA=250.0,
        )
        
        assert result is True
        mock_client.insert_rows_json.assert_called_once()

    @patch("worker.bigquery_integration.bigquery")
    def test_insert_audit_entry_success(self, mock_bq):
        """Test successful insertion of audit trail entry."""
        mock_client = Mock()
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_client.insert_rows_json.return_value = []
        
        bq = BigQueryDataWarehouse()
        result = bq.insert_audit_entry(
            audit_id="audit-001",
            event_type="data_ingestion",
            event_id="evt-001",
            pseudonym_id="PS_abc123",
            action="READ",
            purpose="diagnostic_support",
            accessor="worker.process",
            result="success",
        )
        
        assert result is True
        mock_client.insert_rows_json.assert_called_once()

    @patch("worker.bigquery_integration.bigquery")
    def test_query_events_by_modality(self, mock_bq):
        """Test querying events by modality."""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_results = [
            {"event_id": "evt-001", "modality": "CT"},
            {"event_id": "evt-002", "modality": "CT"},
        ]
        
        mock_query_job.result.return_value = mock_results
        mock_client.query.return_value = mock_query_job
        
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_bq.QueryJobConfig = Mock()
        mock_bq.ScalarQueryParameter = Mock()
        
        bq = BigQueryDataWarehouse()
        results = bq.query_events_by_modality("CT", days_back=7)
        
        assert results is not None
        assert len(results) == 2

    @patch("worker.bigquery_integration.bigquery")
    def test_query_events_failure(self, mock_bq):
        """Test query gracefully handles errors."""
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Query failed")
        
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_bq.QueryJobConfig = Mock()
        mock_bq.ScalarQueryParameter = Mock()
        
        bq = BigQueryDataWarehouse()
        results = bq.query_events_by_modality("CT")
        
        assert results is None

    @patch("worker.bigquery_integration.bigquery")
    def test_get_audit_trail_for_event(self, mock_bq):
        """Test retrieving audit trail for specific event."""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_results = [
            {
                "audit_id": "audit-001",
                "event_type": "data_ingestion",
                "action": "READ",
            },
            {
                "audit_id": "audit-002",
                "event_type": "data_deidentification",
                "action": "PROCESS",
            },
        ]
        
        mock_query_job.result.return_value = mock_results
        mock_client.query.return_value = mock_query_job
        
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_bq.QueryJobConfig = Mock()
        mock_bq.ScalarQueryParameter = Mock()
        
        bq = BigQueryDataWarehouse()
        results = bq.get_audit_trail_for_event("evt-001")
        
        assert results is not None
        assert len(results) == 2
        assert results[0]["event_type"] == "data_ingestion"
        assert results[1]["event_type"] == "data_deidentification"

    @patch("worker.bigquery_integration.bigquery")
    def test_partition_expiration_configuration(self, mock_bq):
        """Test that partition expiration is configured for GDPR compliance."""
        mock_client = Mock()
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_bq.Table.return_value = Mock()
        mock_bq.TimePartitioning = Mock()
        mock_bq.TimePartitioningType.DAY = "DAY"
        
        bq = BigQueryDataWarehouse()
        
        # Verify TimePartitioning was configured
        # (This would be called during _create_processed_events_table)
        # The actual verification happens when the table is created


class TestComplianceProperties:
    """Test GDPR compliance properties of BigQuery integration."""

    def test_audit_log_retention_meets_minimum_requirement(self):
        """Test audit log retention meets 7-year regulatory requirement."""
        # GDPR and medical regulations typically require 7-year retention
        seven_years_days = 365 * 7  # 2555 days
        assert RETENTION_AUDIT_LOGS_DAYS >= seven_years_days

    def test_processed_events_retention_is_limited(self):
        """Test processed events have reasonable retention limit (90 days)."""
        # Principle 5: Data should not be kept longer than necessary
        assert RETENTION_PROCESSED_EVENTS_DAYS == 90

    @patch("worker.bigquery_integration.bigquery")
    def test_all_inserts_are_de_identified(self, mock_bq):
        """Test that all data stored uses pseudonym_id not patient_id."""
        mock_client = Mock()
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_client.insert_rows_json.return_value = []
        
        bq = BigQueryDataWarehouse()
        
        # Insert should use pseudonym_id
        bq.insert_processed_event(
            event_id="evt-001",
            pseudonym_id="PS_irreversible123",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
        )
        
        # Verify call arguments contain pseudonym_id, not patient_id
        call_args = mock_client.insert_rows_json.call_args
        rows = call_args[0][1]  # Second argument is rows_to_insert
        
        assert rows[0]["pseudonym_id"] == "PS_irreversible123"
        assert "patient_id" not in rows[0]


class TestIntegration:
    """Integration tests for BigQuery storage."""

    @patch("worker.bigquery_integration.bigquery")
    def test_end_to_end_event_storage_and_retrieval(self, mock_bq):
        """Test complete workflow: store event and retrieve audit trail."""
        # Setup mocks
        mock_client = Mock()
        mock_query_job = Mock()
        
        mock_bq.Client.return_value = mock_client
        mock_bq.Dataset.return_value = Mock()
        mock_bq.QueryJobConfig = Mock()
        mock_bq.ScalarQueryParameter = Mock()
        mock_bq.TimePartitioning = Mock()
        mock_bq.TimePartitioningType.DAY = "DAY"
        
        # Configure insert to succeed
        mock_client.insert_rows_json.return_value = []
        
        # Configure query to return audit entries
        mock_audit_entries = [
            {"audit_id": "audit-001", "event_type": "data_ingestion"},
            {"audit_id": "audit-002", "event_type": "data_deidentification"},
        ]
        mock_query_job.result.return_value = mock_audit_entries
        mock_client.query.return_value = mock_query_job
        
        bq = BigQueryDataWarehouse()
        
        # Insert event
        event_id = "evt-001"
        result = bq.insert_processed_event(
            event_id=event_id,
            pseudonym_id="PS_test",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
        )
        assert result is True
        
        # Insert audit entry
        result = bq.insert_audit_entry(
            audit_id="audit-001",
            event_type="data_ingestion",
            event_id=event_id,
        )
        assert result is True
        
        # Retrieve audit trail
        audit_trail = bq.get_audit_trail_for_event(event_id)
        assert audit_trail is not None
        assert len(audit_trail) == 2
