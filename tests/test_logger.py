"""
Tests for GDPR-compliant structured logging.

Tests Principle 7 (Accountability) and audit trail functionality.
"""

import json
import os
from pathlib import Path
from datetime import datetime

import pytest

from worker.logger import StructuredLogger, audit_logger


class TestStructuredLogger:
    """Test structured logging for GDPR compliance (Principle 7)."""

    @pytest.fixture
    def logger(self, tmp_path):
        """Create a temporary logger instance for testing."""
        # Override audit directory for testing
        import worker.logger
        old_audit_dir = worker.logger.AUDIT_LOG_DIR
        worker.logger.AUDIT_LOG_DIR = str(tmp_path)
        
        logger_instance = StructuredLogger(logger_name="test_audit")
        
        yield logger_instance
        
        # Restore original
        worker.logger.AUDIT_LOG_DIR = old_audit_dir

    def test_logger_initialization(self, logger):
        """Test logger initialization."""
        assert logger.logger_name == "test_audit"
        assert logger.logger is not None

    def test_log_data_ingestion(self, logger, tmp_path):
        """Test data ingestion logging."""
        logger.log_data_ingestion(
            event_id="evt-001",
            patient_id="P12345",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
            purpose="diagnostic_support",
        )
        
        # Check audit file was created
        audit_file = tmp_path / "audit_trail.jsonl"
        assert audit_file.exists()
        
        # Read and verify audit entry
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        assert entry["event_type"] == "data_ingestion"
        assert entry["event_id"] == "evt-001"
        assert entry["modality"] == "CT"
        assert entry["legal_basis"] == "explicit_consent|medical_necessity"

    def test_log_data_deidentification(self, logger, tmp_path):
        """Test de-identification logging (Principle 3 evidence)."""
        logger.log_data_deidentification(
            event_id="evt-002",
            patient_id="P12345",
            pseudonym_id="PS_abc123",
            fields_removed=["patient_name", "patient_birth_date"],
            purpose="gdpr_principle_3_minimization",
        )
        
        # Check audit file
        audit_file = tmp_path / "audit_trail.jsonl"
        assert audit_file.exists()
        
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        assert entry["event_type"] == "data_deidentification"
        assert entry["fields_removed"] == ["patient_name", "patient_birth_date"]
        assert entry["field_count"] == 2
        assert entry["principle"] == "minimization_principle_3"

    def test_log_data_retention_policy(self, logger, tmp_path):
        """Test retention policy logging (Principle 5 evidence)."""
        logger.log_data_retention(
            event_id="evt-003",
            study_uid="1.2.3.4.5",
            storage_location="redis",
            ttl_seconds=3600,
            retention_policy="automatic_deletion_after_processing",
        )
        
        audit_file = tmp_path / "audit_trail.jsonl"
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        assert entry["event_type"] == "data_retention_policy"
        assert entry["ttl_seconds"] == 3600
        assert entry["ttl_formatted"] == "1h"
        assert entry["principle"] == "storage_limitation_principle_5"

    def test_log_consent_record(self, logger, tmp_path):
        """Test consent logging (GDPR Article 7)."""
        logger.log_consent_record(
            event_id="evt-004",
            patient_id="P12345",
            consent_type="explicit_consent",
            consent_logged=True,
            consent_reference="consent-abc123",
            purposes=["diagnostic_support", "treatment"],
        )
        
        audit_file = tmp_path / "audit_trail.jsonl"
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        assert entry["event_type"] == "consent_management"
        assert entry["consent_logged"] is True
        assert len(entry["approved_purposes"]) == 2
        assert entry["legal_basis"] == "explicit_consent"

    def test_log_data_access(self, logger, tmp_path):
        """Test data access logging for audit trail."""
        logger.log_data_access(
            event_id="evt-005",
            pseudonym_id="PS_abc123",
            accessor="worker.process_dicom_event",
            access_type="READ",
            purpose="medical_imaging_analysis",
            result="success",
        )
        
        audit_file = tmp_path / "audit_trail.jsonl"
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        assert entry["event_type"] == "data_access"
        assert entry["accessor"] == "worker.process_dicom_event"
        assert entry["result"] == "success"

    def test_log_data_deletion(self, logger, tmp_path):
        """Test automatic data deletion logging (GDPR Article 17)."""
        logger.log_data_deletion(
            event_id="evt-006",
            study_uid="1.2.3.4.5",
            pseudonym_id="PS_abc123",
            reason="retention_policy_expiry",
            data_amount_mb=125.5,
        )
        
        audit_file = tmp_path / "audit_trail.jsonl"
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        assert entry["event_type"] == "data_deletion"
        assert entry["reason"] == "retention_policy_expiry"
        assert entry["data_amount_mb"] == 125.5
        assert entry["principle"] == "right_to_erasure_article_17"

    def test_log_breach_notification(self, logger, tmp_path):
        """Test security breach logging (GDPR Article 33)."""
        logger.log_breach_notification(
            breach_id="breach-001",
            affected_patients=5,
            breach_type="unauthorized_access",
            description="Unauthorized access to patient records detected",
            remedial_actions=["System shutdown", "Investigation started"],
        )
        
        audit_file = tmp_path / "audit_trail.jsonl"
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        assert entry["event_type"] == "security_breach"
        assert entry["affected_patients"] == 5
        assert entry["requires_notification"] is True
        assert len(entry["remedial_actions"]) == 2

    def test_ttl_formatting(self, logger):
        """Test TTL formatting utility."""
        assert logger._format_ttl(30) == "30s"
        assert logger._format_ttl(300) == "5m"
        assert logger._format_ttl(3600) == "1h"
        assert logger._format_ttl(86400) == "1d"
        assert logger._format_ttl(604800) == "7d"

    def test_audit_file_jsonl_format(self, logger, tmp_path):
        """Test audit file is valid JSONL format."""
        logger.log_data_ingestion(
            event_id="evt-001",
            patient_id="P12345",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
            purpose="diagnostic_support",
        )
        
        logger.log_data_deidentification(
            event_id="evt-002",
            patient_id="P12345",
            pseudonym_id="PS_abc123",
            fields_removed=["patient_name"],
        )
        
        audit_file = tmp_path / "audit_trail.jsonl"
        
        # Read all lines and verify they're valid JSON
        with open(audit_file, "r") as f:
            for line_num, line in enumerate(f):
                entry = json.loads(line)
                assert "event_type" in entry
                assert "timestamp" in entry
        
        # Should have 2 entries
        with open(audit_file, "r") as f:
            line_count = len(f.readlines())
        assert line_count == 2

    def test_audit_fields_consistency(self, logger, tmp_path):
        """Test consistency of audit fields across event types."""
        logger.log_data_ingestion(
            event_id="evt-001",
            patient_id="P12345",
            study_uid="1.2.3.4.5",
            modality="CT",
            source="PACS",
            purpose="diagnostic_support",
        )
        
        audit_file = tmp_path / "audit_trail.jsonl"
        with open(audit_file, "r") as f:
            entry = json.loads(f.readline())
        
        # All entries should have these fields
        required_fields = ["event_type", "timestamp", "action"]
        for field in required_fields:
            assert field in entry


class TestComplianceProperties:
    """Test compliance and regulatory properties."""

    def test_audit_logger_singleton_pattern(self):
        """Test that audit_logger is a module-level singleton."""
        from worker.logger import audit_logger as logger1
        from worker.logger import audit_logger as logger2
        
        # Both should be the same instance
        assert logger1 is logger2

    def test_event_types_cover_gdpr_articles(self):
        """Test that logged event types cover key GDPR articles."""
        expected_event_types = {
            "data_ingestion",           # Art. 13/14 (transparency)
            "data_deidentification",    # Principle 3
            "data_retention_policy",    # Principle 5
            "consent_management",       # Art. 7 (consent)
            "data_access",              # Art. 32 (security)
            "data_deletion",            # Art. 17 (right to erasure)
            "security_breach",          # Art. 33 (breach notification)
        }
        
        logger = StructuredLogger(logger_name="compliance_test")
        
        # All event types should be callable
        assert hasattr(logger, "log_data_ingestion")
        assert hasattr(logger, "log_data_deidentification")
        assert hasattr(logger, "log_data_retention")
        assert hasattr(logger, "log_consent_record")
        assert hasattr(logger, "log_data_access")
        assert hasattr(logger, "log_data_deletion")
        assert hasattr(logger, "log_breach_notification")

    def test_audit_trail_immutability_intent(self, tmp_path):
        """Test that audit trail is write-only (append-only) pattern."""
        logger = StructuredLogger(logger_name="immutable_test")
        import worker.logger
        old_audit_dir = worker.logger.AUDIT_LOG_DIR
        worker.logger.AUDIT_LOG_DIR = str(tmp_path)
        
        try:
            audit_file = tmp_path / "audit_trail.jsonl"
            
            # Write first entry
            logger.log_data_ingestion(
                event_id="evt-001",
                patient_id="P123",
                study_uid="1.2.3",
                modality="CT",
                source="PACS",
                purpose="diagnostic",
            )
            
            with open(audit_file, "r") as f:
                first_content = f.read()
            
            # Write second entry
            logger.log_data_deidentification(
                event_id="evt-002",
                patient_id="P123",
                pseudonym_id="PS_abc",
                fields_removed=["patient_name"],
            )
            
            with open(audit_file, "r") as f:
                second_content = f.read()
            
            # Second content should contain first content (append-only)
            assert first_content in second_content
            assert len(second_content) > len(first_content)
        
        finally:
            worker.logger.AUDIT_LOG_DIR = old_audit_dir
