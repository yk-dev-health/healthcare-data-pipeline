"""
Structured logging for GDPR compliance (Principle 7: Accountability).

This module implements:
- JSON-based audit trails for regulatory compliance
- Consent tracking and logging
- Data processing event recording
- Automated log retention and archival
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import structlog
except ImportError:
    structlog = None

from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
AUDIT_LOG_DIR = os.getenv("AUDIT_LOG_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs"))
os.makedirs(AUDIT_LOG_DIR, exist_ok=True)


class StructuredLogger:
    """
    Wrapper for structured logging with audit trail support.
    
    Implements UK GDPR Article 5(1)(f): Accountability principle
    - Records all data processing activities
    - Maintains audit trail for regulatory inspection
    - Enables right-to-access data processing records
    """

    def __init__(self, logger_name: str = "healthcare_audit"):
        """Initialize structured logger."""
        self.logger_name = logger_name
        self._setup_structlog()
        self.logger = self._get_logger()

    def _setup_structlog(self):
        """Configure structlog for JSON output."""
        if structlog is None:
            return

        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer(),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

    def _get_logger(self):
        """Get logger instance."""
        if structlog is not None:
            return structlog.get_logger(self.logger_name)
        return logging.getLogger(self.logger_name)

    def log_data_ingestion(
        self,
        event_id: str,
        patient_id: str,
        study_uid: str,
        modality: str,
        source: str,
        purpose: str,
        metadata: Optional[dict[str, Any]] = None,
    ):
        """
        Log data ingestion event (GDPR Article 13/14 transparency).

        Args:
            event_id: Unique event identifier
            patient_id: Patient identifier (will be pseudonymized in log if needed)
            study_uid: DICOM study UID
            modality: Imaging modality (CT, MRI, US, etc.)
            source: Data source (PACS, manual, etc.)
            purpose: Purpose of processing (diagnostic, research, etc.)
            metadata: Additional metadata
        """
        record = {
            "event_type": "data_ingestion",
            "event_id": event_id,
            "patient_id": patient_id,
            "study_uid": study_uid,
            "modality": modality,
            "source": source,
            "purpose": purpose,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "READ",
            "data_category": "medical_imaging_metadata",
            "legal_basis": "explicit_consent|medical_necessity",
        }

        if metadata:
            record["metadata"] = metadata

        self._write_audit_record(record)
        if structlog is not None:
            self.logger.info("data_ingestion", **record)
        else:
            logging.getLogger(self.logger_name).info(json.dumps(record))

    def log_data_deidentification(
        self,
        event_id: str,
        patient_id: str,
        pseudonym_id: str,
        fields_removed: list[str],
        purpose: str = "minimize_unnecessary_pii",
    ):
        """
        Log de-identification operation (Principle 3: Minimization).

        Args:
            event_id: Event identifier
            patient_id: Original patient identifier
            pseudonym_id: Pseudonym assigned for audit
            fields_removed: List of PII fields removed
            purpose: Purpose of de-identification
        """
        record = {
            "event_type": "data_deidentification",
            "event_id": event_id,
            "patient_id": patient_id,
            "pseudonym_id": pseudonym_id,
            "fields_removed": fields_removed,
            "field_count": len(fields_removed),
            "purpose": purpose,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "PROCESS",
            "principle": "minimization_principle_3",
        }

        self._write_audit_record(record)
        if structlog is not None:
            self.logger.info("data_deidentification", **record)
        else:
            logging.getLogger(self.logger_name).info(json.dumps(record))

    def log_data_retention(
        self,
        event_id: str,
        study_uid: str,
        storage_location: str,
        ttl_seconds: int,
        retention_policy: str = "automatic_deletion",
    ):
        """
        Log data retention policy enforcement (Principle 5: Storage Limitation).

        Args:
            event_id: Event identifier
            study_uid: DICOM study UID
            storage_location: Where data is stored (Redis, BigQuery, etc.)
            ttl_seconds: Time-to-live in seconds
            retention_policy: Policy name
        """
        record = {
            "event_type": "data_retention_policy",
            "event_id": event_id,
            "study_uid": study_uid,
            "storage_location": storage_location,
            "ttl_seconds": ttl_seconds,
            "ttl_formatted": self._format_ttl(ttl_seconds),
            "retention_policy": retention_policy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "CONFIGURE",
            "principle": "storage_limitation_principle_5",
        }

        self._write_audit_record(record)
        if structlog is not None:
            self.logger.info("data_retention_policy", **record)
        else:
            logging.getLogger(self.logger_name).info(json.dumps(record))

    def log_consent_record(
        self,
        event_id: str,
        patient_id: str,
        consent_type: str,
        consent_logged: bool,
        consent_reference: Optional[str] = None,
        purposes: Optional[list[str]] = None,
    ):
        """
        Log consent management (GDPR Article 7: Consent documentation).

        Args:
            event_id: Event identifier
            patient_id: Patient identifier
            consent_type: Type of consent (explicit, implied, etc.)
            consent_logged: Whether consent was properly recorded
            consent_reference: Reference to consent document
            purposes: List of approved purposes
        """
        record = {
            "event_type": "consent_management",
            "event_id": event_id,
            "patient_id": patient_id,
            "consent_type": consent_type,
            "consent_logged": consent_logged,
            "consent_reference": consent_reference,
            "approved_purposes": purposes or [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "RECORD",
            "legal_basis": "explicit_consent",
        }

        self._write_audit_record(record)
        if structlog is not None:
            self.logger.info("consent_management", **record)
        else:
            logging.getLogger(self.logger_name).info(json.dumps(record))

    def log_data_access(
        self,
        event_id: str,
        pseudonym_id: str,
        accessor: str,
        access_type: str,
        purpose: str,
        result: str = "success",
    ):
        """
        Log data access for audit trail (GDPR Article 32: Security logging).

        Args:
            event_id: Event identifier
            pseudonym_id: Pseudonym of accessed data
            accessor: Who accessed (service name, user ID, etc.)
            access_type: Type of access (READ, WRITE, DELETE, etc.)
            purpose: Purpose of access
            result: Success/failure/denied
        """
        record = {
            "event_type": "data_access",
            "event_id": event_id,
            "pseudonym_id": pseudonym_id,
            "accessor": accessor,
            "access_type": access_type,
            "purpose": purpose,
            "result": result,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": access_type,
        }

        self._write_audit_record(record)
        if structlog is not None:
            self.logger.info("data_access", **record)
        else:
            logging.getLogger(self.logger_name).info(json.dumps(record))

    def log_data_deletion(
        self,
        event_id: str,
        study_uid: str,
        pseudonym_id: str,
        reason: str = "retention_policy_expiry",
        data_amount_mb: Optional[float] = None,
    ):
        """
        Log automatic data deletion (GDPR Article 5 & 17: Right to erasure).

        Args:
            event_id: Event identifier
            study_uid: DICOM study UID
            pseudonym_id: Pseudonym of deleted data
            reason: Reason for deletion
            data_amount_mb: Size of deleted data in MB
        """
        record = {
            "event_type": "data_deletion",
            "event_id": event_id,
            "study_uid": study_uid,
            "pseudonym_id": pseudonym_id,
            "reason": reason,
            "data_amount_mb": data_amount_mb,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "DELETE",
            "principle": "right_to_erasure_article_17",
        }

        self._write_audit_record(record)
        if structlog is not None:
            self.logger.warning("data_deletion", **record)
        else:
            logging.getLogger(self.logger_name).warning(json.dumps(record))

    def log_breach_notification(
        self,
        breach_id: str,
        affected_patients: int,
        breach_type: str,
        description: str,
        remedial_actions: Optional[list[str]] = None,
    ):
        """
        Log security breach (GDPR Article 33: Breach notification).

        Args:
            breach_id: Unique breach identifier
            affected_patients: Number of affected individuals
            breach_type: Type of breach
            description: Description of incident
            remedial_actions: Actions taken to mitigate
        """
        record = {
            "event_type": "security_breach",
            "breach_id": breach_id,
            "affected_patients": affected_patients,
            "breach_type": breach_type,
            "description": description,
            "remedial_actions": remedial_actions or [],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "INCIDENT",
            "severity": "high",
            "requires_notification": affected_patients > 0,
        }

        self._write_audit_record(record)
        if structlog is not None:
            self.logger.critical("security_breach", **record)
        else:
            logging.getLogger(self.logger_name).critical(json.dumps(record))

    def _write_audit_record(self, record: dict[str, Any]):
        """
        Write audit record to file (JSONL format for compliance).

        Args:
            record: Audit record to write
        """
        audit_file = os.path.join(AUDIT_LOG_DIR, "audit_trail.jsonl")
        try:
            with open(audit_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
        except Exception as e:
            logging.error(f"Failed to write audit record: {e}")

    @staticmethod
    def _format_ttl(seconds: int) -> str:
        """Format TTL in human-readable form."""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            return f"{seconds // 60}m"
        elif seconds < 86400:
            return f"{seconds // 3600}h"
        else:
            return f"{seconds // 86400}d"


# Global logger instance
audit_logger = StructuredLogger("healthcare_audit")
