"""
Tests for GDPR-compliant data minimization and pseudonymization.

Tests Principle 3 (Data Minimization) and Principle 5 (Storage Limitation).
"""

import json
import os
from datetime import date

import pytest
import redis

from worker.data_minimization import (
    PatientPseudonymizer,
    create_minimized_payload,
    create_pii_shadow_record,
    remove_pii_from_dicom_metadata,
    store_sensitive_data_with_ttl,
    get_sensitive_data_from_ttl_store,
    SENSITIVE_DATA_TTL,
    AUDIT_LOG_TTL,
)


class TestPatientPseudonymizer:
    """Test pseudonymization (GDPR Principle 3: Data Minimization)."""

    def test_pseudonymize_patient_id(self):
        """Test patient ID pseudonymization is deterministic and irreversible."""
        pseudo = PatientPseudonymizer(salt="test-salt")
        
        pid1 = pseudo.pseudonymize_patient_id("P12345")
        pid2 = pseudo.pseudonymize_patient_id("P12345")
        
        # Should be deterministic
        assert pid1 == pid2
        
        # Should have PS_ prefix
        assert pid1.startswith("PS_")
        
        # Should not contain original ID
        assert "12345" not in pid1

    def test_pseudonymize_patient_name(self):
        """Test patient name pseudonymization."""
        pseudo = PatientPseudonymizer(salt="test-salt")
        
        pname = pseudo.pseudonymize_patient_name("John Doe")
        
        # Should not contain original name
        assert "John" not in pname
        assert "Doe" not in pname
        
        # Should have PN_ prefix
        assert pname.startswith("PN_")

    def test_pseudonymize_none_patient_name(self):
        """Test handling of None patient name."""
        pseudo = PatientPseudonymizer(salt="test-salt")
        
        pname = pseudo.pseudonymize_patient_name(None)
        
        # Should return None
        assert pname is None

    def test_different_salts_produce_different_pseudonyms(self):
        """Test that different salts produce different pseudonyms."""
        pseudo1 = PatientPseudonymizer(salt="salt1")
        pseudo2 = PatientPseudonymizer(salt="salt2")
        
        pid1 = pseudo1.pseudonymize_patient_id("P12345")
        pid2 = pseudo2.pseudonymize_patient_id("P12345")
        
        # Different salts should produce different pseudonyms
        assert pid1 != pid2


class TestDataMinimization:
    """Test GDPR Principle 3: Data Minimization."""

    def test_remove_unnecessary_pii_fields(self):
        """Test removal of unnecessary PII fields."""
        dicom_data = {
            "patient_name": "John Doe",
            "patient_birth_date": "1980-01-01",
            "patient_id": "P12345",
            "study_uid": "1.2.3.4.5",
            "modality": "CT",
            "institution_name": "Acme Hospital",
            "referring_physician_name": "Dr. Smith",
        }
        
        minimized = remove_pii_from_dicom_metadata(dicom_data)
        
        # PII should be removed
        assert "patient_name" not in minimized
        assert "patient_birth_date" not in minimized
        assert "institution_name" not in minimized
        assert "referring_physician_name" not in minimized
        
        # Essential medical fields should remain
        assert minimized["patient_id"] == "P12345"
        assert minimized["study_uid"] == "1.2.3.4.5"
        assert minimized["modality"] == "CT"

    def test_minimized_payload_excludes_pii(self):
        """Test minimized payload excludes all PII."""
        event = {
            "patient_name": "Jane Smith",
            "patient_birth_date": "1985-06-15",
            "patient_id": "P98765",
            "study_uid": "1.2.3.4.5.6.7.8.9",
            "modality": "MRI",
            "kVp": 110,
            "mA": 200,
            "source": "PACS",
        }
        
        minimized = create_minimized_payload(event, "PS_abc123")
        
        # PII should never appear in minimized payload
        assert "Jane Smith" not in str(minimized)
        assert "1985-06-15" not in str(minimized)
        
        # Should contain pseudonym
        assert minimized["pseudonym_id"] == "PS_abc123"
        
        # Should contain essential fields
        assert minimized["study_uid"] == event["study_uid"]
        assert minimized["modality"] == event["modality"]

    def test_shadow_record_creation(self):
        """Test shadow record creation for audit linkage."""
        pseudo = PatientPseudonymizer(salt="test-salt")
        
        shadow = create_pii_shadow_record(
            patient_id="P12345",
            patient_name="John Doe",
            patient_birth_date=date(1980, 1, 1),
            study_uid="1.2.3.4.5",
            event_id="evt-123",
            pseudonymizer=pseudo,
        )
        
        # Shadow record should contain pseudonyms, not originals
        assert "PS_" in shadow["pseudonym_id"]
        assert shadow["event_id"] == "evt-123"
        assert shadow["study_uid"] == "1.2.3.4.5"
        assert shadow["purpose"] == "audit_linkage"
        assert "created_at" in shadow


class TestStorageLimitation:
    """Test GDPR Principle 5: Storage Limitation (TTL management)."""

    def test_store_sensitive_data_with_ttl_disabled_when_no_redis(self, monkeypatch):
        """Test behavior when Redis is not available."""
        # Mock redis_client as None
        import worker.data_minimization
        monkeypatch.setattr(worker.data_minimization, "redis_client", None)
        
        result = store_sensitive_data_with_ttl(
            key="test:key",
            value={"test": "data"},
            ttl_seconds=3600,
        )
        
        # Should handle gracefully when Redis not available
        assert result is False

    def test_get_sensitive_data_returns_none_when_no_redis(self, monkeypatch):
        """Test retrieval when Redis is not available."""
        import worker.data_minimization
        monkeypatch.setattr(worker.data_minimization, "redis_client", None)
        
        result = get_sensitive_data_from_ttl_store("test:key")
        
        # Should return None when Redis not available
        assert result is None

    def test_ttl_configuration_constants(self):
        """Test that TTL constants are configured correctly."""
        # Sensitive data should be 1 hour
        assert SENSITIVE_DATA_TTL == 3600
        
        # Audit logs should be at least 90 days (7,776,000 seconds)
        assert AUDIT_LOG_TTL == 7776000
        
        # Audit retention > Sensitive retention
        assert AUDIT_LOG_TTL > SENSITIVE_DATA_TTL

    @pytest.mark.skipif(
        os.getenv("SKIP_REDIS_TESTS") == "true",
        reason="Redis not available in test environment"
    )
    def test_store_and_retrieve_sensitive_data(self):
        """Integration test: store and retrieve sensitive data with TTL."""
        try:
            # Try to connect to Redis
            r = redis.Redis(host="localhost", port=6379, db=0)
            r.ping()
        except (redis.ConnectionError, Exception):
            pytest.skip("Redis not available")
        
        # Store data
        key = f"test:sensitive:{id(__name__)}"
        test_data = {"patient_id": "P123", "name": "John"}
        
        result = store_sensitive_data_with_ttl(
            key=key,
            value=test_data,
            ttl_seconds=10,
        )
        
        # Should store successfully
        assert result is True
        
        # Should be retrievable
        retrieved = get_sensitive_data_from_ttl_store(key)
        assert retrieved is not None
        assert retrieved["patient_id"] == "P123"
        
        # Cleanup
        r.delete(key)


class TestIntegration:
    """Integration tests for GDPR-compliant data processing."""

    def test_end_to_end_pii_removal_and_pseudonymization(self):
        """Test complete workflow: PII removal + Pseudonymization."""
        # Original DICOM with PII
        original = {
            "patient_name": "Jane Smith",
            "patient_birth_date": "1985-06-15",
            "patient_id": "P98765",
            "study_uid": "1.2.3.4.5.6.7.8.9",
            "modality": "MRI",
            "kVp": 110,
            "mA": 200,
            "institution_name": "City Hospital",
            "referring_physician_name": "Dr. Jones",
        }
        
        # Step 1: Pseudonymization
        pseudo = PatientPseudonymizer(salt="integration-test-salt")
        pseudonym_id = pseudo.pseudonymize_patient_id(original["patient_id"])
        
        # Step 2: Data Minimization
        minimized = remove_pii_from_dicom_metadata(original)
        
        # Step 3: Create Minimized Payload
        payload = create_minimized_payload(minimized, pseudonym_id)
        
        # Verification
        assert "Jane Smith" not in str(payload)
        assert "1985-06-15" not in str(payload)
        assert "City Hospital" not in str(payload)
        assert "Dr. Jones" not in str(payload)
        
        # Pseudonym should be present
        assert pseudonym_id in str(payload)
        
        # Essential fields should be present
        assert payload["study_uid"] == original["study_uid"]
        assert payload["modality"] == original["modality"]

    def test_compliance_properties(self):
        """Test compliance properties are maintained."""
        # Irreversibility: Cannot recover original from pseudonym
        pseudo = PatientPseudonymizer(salt="test-salt")
        original_id = "P12345"
        pseudonym = pseudo.pseudonymize_patient_id(original_id)
        
        # Pseudonym should not contain original
        assert original_id not in pseudonym
        
        # Determinism: Same input produces same output
        pseudonym2 = pseudo.pseudonymize_patient_id(original_id)
        assert pseudonym == pseudonym2
