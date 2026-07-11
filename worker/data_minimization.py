"""
Data minimization and de-identification functions following UK GDPR principle 3.

This module implements:
- Removal of unnecessary PII from DICOM metadata
- Pseudonymization for audit trails
- TTL-based data retention policies
"""

import hashlib
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import pydicom
import redis
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# TTL settings (in seconds)
SENSITIVE_DATA_TTL = int(os.getenv("SENSITIVE_DATA_TTL", "3600"))  # 1 hour
PROCESSED_EVENT_TTL = int(os.getenv("PROCESSED_EVENT_TTL", "604800"))  # 7 days
AUDIT_LOG_TTL = int(os.getenv("AUDIT_LOG_TTL", "7776000"))  # 90 days

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    redis_client.ping()
except Exception:
    redis_client = None


class PatientPseudonymizer:
    """
    Pseudonymize patient identifiers while maintaining linkage for audit.
    Implements GDPR Article 11: Processing of pseudonymised personal data.
    """

    def __init__(self, salt: Optional[str] = None):
        """
        Initialize pseudonymizer.

        Args:
            salt: Optional salt for hashing (should be environment-managed)
        """
        self.salt = salt or os.getenv("PATIENT_HASH_SALT", "default-salt-change-in-production")

    def pseudonymize_patient_id(self, patient_id: str) -> str:
        """
        Generate irreversible pseudonym from patient ID.
        
        Args:
            patient_id: Original patient identifier
            
        Returns:
            Hashed pseudonym (e.g., "PS_a1b2c3d4...")
        """
        hash_input = f"{patient_id}{self.salt}".encode("utf-8")
        hash_hex = hashlib.sha256(hash_input).hexdigest()[:16]
        return f"PS_{hash_hex}"

    def pseudonymize_patient_name(self, patient_name: Optional[str]) -> Optional[str]:
        """
        Generate pseudonym from patient name for audit linkage.
        
        Args:
            patient_name: Original patient name (will be removed from main flow)
            
        Returns:
            Hashed pseudonym or None if name is None
        """
        if patient_name is None:
            return None
        hash_input = f"{patient_name}{self.salt}".encode("utf-8")
        hash_hex = hashlib.sha256(hash_input).hexdigest()[:12]
        return f"PN_{hash_hex}"


def remove_pii_from_dicom_metadata(dicom_metadata: dict[str, Any]) -> dict[str, Any]:
    """
    Remove sensitive PII from DICOM metadata (Principle 3: Minimization).

    According to DICOM Standard, the following tags should be removed:
    - PatientName (0010,0010)
    - PatientBirthDate (0010,0030)
    - PatientID (0010,0020) - kept for medical necessity
    - PatientAge (0010,1010)
    - PatientSex (0010,0040) - kept for medical necessity
    - StudyDate (0008,0020) - kept for medical necessity but consider offset
    - InstitutionName (0008,0080)
    - ReferringPhysicianName (0008,0090)

    Args:
        dicom_metadata: Raw DICOM metadata dict

    Returns:
        De-identified DICOM metadata dict
    """
    minimized = {}
    
    # PII to always remove (Principle 3: Data Minimization)
    pii_tags_to_remove = {
        "patient_name",
        "PatientName",
        "patient_birth_date",
        "PatientBirthDate",
        "patient_age",
        "PatientAge",
        "institution_name",
        "InstitutionName",
        "referring_physician_name",
        "ReferringPhysicianName",
        "operator_name",
        "OperatorName",
    }

    # Medically necessary tags to retain
    essential_tags = {
        "patient_id",
        "PatientID",
        "study_uid",
        "StudyUID",
        "modality",
        "Modality",
        "patient_sex",
        "PatientSex",
        "study_date",
        "StudyDate",
        "accession_number",
        "AccessionNumber",
        "series_description",
        "SeriesDescription",
    }

    for key, value in dicom_metadata.items():
        if key in pii_tags_to_remove:
            logger.info(f"Removing PII field: {key}")
            continue
        if key in essential_tags or key not in pii_tags_to_remove:
            minimized[key] = value

    return minimized


def create_pii_shadow_record(
    patient_id: str,
    patient_name: Optional[str],
    patient_birth_date: Optional[date],
    study_uid: str,
    event_id: str,
    pseudonymizer: Optional[PatientPseudonymizer] = None,
) -> dict[str, Any]:
    """
    Create a separately stored linkage record for audit trails.
    
    This record maintains the ability to re-identify for:
    - Regulatory audit (GDPR Article 15: Right of access)
    - Clinical error correction
    - Breach notification
    
    Stored in Redis with strict TTL (90 days for audit purposes).
    
    Args:
        patient_id: Original patient identifier
        patient_name: Original patient name
        patient_birth_date: Original birth date
        study_uid: DICOM study UID
        event_id: Event identifier
        pseudonymizer: PatientPseudonymizer instance
        
    Returns:
        Shadow record dict with pseudonymized links
    """
    if pseudonymizer is None:
        pseudonymizer = PatientPseudonymizer()

    pseudonym_id = pseudonymizer.pseudonymize_patient_id(patient_id)
    pseudonym_name = pseudonymizer.pseudonymize_patient_name(patient_name)

    shadow_record = {
        "event_id": event_id,
        "study_uid": study_uid,
        "pseudonym_id": pseudonym_id,
        "pseudonym_name": pseudonym_name,
        "original_patient_id": patient_id,  # Encrypted would be better in production
        "original_patient_name": patient_name,
        "original_birth_date": str(patient_birth_date) if patient_birth_date else None,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "purpose": "audit_linkage",
    }

    # Store in Redis with strict TTL
    if redis_client is not None:
        try:
            key = f"shadow:audit:{event_id}"
            redis_client.setex(
                key,
                AUDIT_LOG_TTL,  # 90 days
                json.dumps(shadow_record, default=str),
            )
            logger.info(f"Shadow record stored with {AUDIT_LOG_TTL}s TTL: {key}")
        except Exception as e:
            logger.error(f"Failed to store shadow record: {e}")

    return shadow_record


def store_sensitive_data_with_ttl(
    key: str,
    value: Any,
    ttl_seconds: Optional[int] = None,
    purpose: str = "processing",
) -> bool:
    """
    Store sensitive data in Redis with explicit TTL.
    
    Implements Principle 5: Storage Limitation.
    
    Args:
        key: Redis key (e.g., "sensitive:patient_id:P12345")
        value: Data to store
        ttl_seconds: TTL in seconds (defaults to SENSITIVE_DATA_TTL)
        purpose: Purpose of storage for audit (e.g., "processing", "validation")
        
    Returns:
        True if stored successfully, False otherwise
    """
    if redis_client is None:
        logger.warning("Redis not available; sensitive data not stored")
        return False

    ttl = ttl_seconds or SENSITIVE_DATA_TTL
    try:
        redis_client.setex(key, ttl, json.dumps({"value": value, "purpose": purpose}, default=str))
        logger.info(f"Sensitive data stored with {ttl}s TTL: {key}")
        return True
    except Exception as e:
        logger.error(f"Failed to store sensitive data: {e}")
        return False


def get_sensitive_data_from_ttl_store(key: str) -> Optional[Any]:
    """
    Retrieve sensitive data from TTL-managed store.
    
    Args:
        key: Redis key
        
    Returns:
        Retrieved data or None if expired/missing
    """
    if redis_client is None:
        return None

    try:
        data = redis_client.get(key)
        if data:
            obj = json.loads(data)
            return obj.get("value")
        return None
    except Exception as e:
        logger.error(f"Failed to retrieve sensitive data: {e}")
        return None


def create_minimized_payload(
    event: dict[str, Any],
    pseudonym_id: str,
) -> dict[str, Any]:
    """
    Create a minimized payload for storage (Principle 3).
    
    Args:
        event: Original event data
        pseudonym_id: Pseudonymized patient identifier
        
    Returns:
        Minimized payload suitable for long-term storage
    """
    minimized = {
        "pseudonym_id": pseudonym_id,
        "study_uid": event.get("study_uid"),
        "modality": event.get("modality"),
        "study_date": str(event.get("study_date")),
        "source": event.get("source"),
        "purpose": event.get("purpose"),
        "kVp": event.get("kVp"),
        "mA": event.get("mA"),
    }
    return minimized


# Example integration test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test pseudonymization
    pseudo = PatientPseudonymizer(salt="test-salt")
    p_id = pseudo.pseudonymize_patient_id("P12345")
    p_name = pseudo.pseudonymize_patient_name("John Doe")
    print(f"Pseudonym ID: {p_id}")
    print(f"Pseudonym Name: {p_name}")

    # Test de-identification
    test_dicom = {
        "patient_name": "John Doe",
        "patient_birth_date": "1980-01-01",
        "patient_id": "P12345",
        "study_uid": "1.2.3.4.5",
        "modality": "CT",
        "study_date": "2024-01-15",
        "institution_name": "Acme Hospital",
    }
    minimized = remove_pii_from_dicom_metadata(test_dicom)
    print(f"Minimized: {minimized}")
