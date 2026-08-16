"""
Data minimization and de-identification functions following UK GDPR principle 3.

This module implements:
- Removal of unnecessary PII from DICOM metadata
- Pseudonymization for audit trails
- TTL-based data retention policies
"""

import hashlib
import hmac
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from dotenv import load_dotenv

from common.config import DEFAULT_SALT, env_int, get_settings
from common.errors import RedisUnavailableError
from common.redis_support import get_redis_client

load_dotenv()

logger = logging.getLogger(__name__)

_settings = get_settings()

REDIS_HOST = _settings.redis.host
REDIS_PORT = _settings.redis.port
REDIS_DB = _settings.redis.db

# TTL settings (in seconds)
SENSITIVE_DATA_TTL = env_int("SENSITIVE_DATA_TTL", 3600)  # 1 hour
PROCESSED_EVENT_TTL = env_int("PROCESSED_EVENT_TTL", 604800)  # 7 days
AUDIT_LOG_TTL = env_int("AUDIT_LOG_TTL", 7776000)  # 90 days

# Lazily connected; constructing this performs no I/O. The previous version
# called `.ping()` at import with no socket timeout, which blocked module
# import for ~50 s whenever Redis was not listening.
redis_client = get_redis_client()


class PatientPseudonymizer:
    """
    Pseudonymize patient identifiers while maintaining linkage for audit.
    Implements GDPR Article 11: Processing of pseudonymised personal data.
    """

    def __init__(self, salt: Optional[str] = None):
        """
        Initialize pseudonymizer.

        Args:
            salt: Keying material. Defaults to ``PATIENT_HASH_SALT``.

        Raises:
            ValueError: The built-in development salt is in use while
                ``APP_ENV`` is production. A publicly-known key makes every
                pseudonym reversible by brute force over the patient-ID space,
                which are typically short and structured (``P00001``...), so
                this is not a theoretical attack.
        """
        settings = get_settings()
        self.salt = salt or settings.patient_hash_salt
        if settings.is_production and self.salt == DEFAULT_SALT:
            raise ValueError(
                "PATIENT_HASH_SALT is the built-in default; refusing to generate "
                "reversible pseudonyms in production"
            )
        self._key = self.salt.encode("utf-8")

    def _digest(self, value: str, prefix: str, length: int) -> str:
        """Keyed HMAC-SHA256 digest.

        HMAC rather than ``sha256(value + salt)``. A plain salted hash is
        vulnerable to length-extension and, more practically here, offers no
        formal guarantee once the salt is treated as a secret key. HMAC is the
        construction that is actually designed for keyed digests, and the
        cost is identical.
        """
        mac = hmac.new(self._key, value.encode("utf-8"), hashlib.sha256)
        return f"{prefix}_{mac.hexdigest()[:length]}"

    def pseudonymize_patient_id(self, patient_id: Optional[str]) -> str:
        """
        Generate an irreversible pseudonym from a patient ID.

        Deterministic by design: the same patient must map to the same
        pseudonym so that records can be linked for clinical audit without
        holding the identifier.

        A DICOM ingestion payload legitimately carries no ``patient_id`` — the
        study UID is the key — so ``None`` maps to a fixed sentinel rather than
        raising. Returning a sentinel keeps audit records well-formed; hashing
        the empty string would instead produce a plausible-looking pseudonym
        that silently collides across every patient-less event.

        Args:
            patient_id: Original patient identifier, or None.

        Returns:
            Keyed pseudonym (e.g., "PS_a1b2c3d4...") or ``"PS_unknown"``.
        """
        if patient_id is None or str(patient_id).strip() == "":
            return "PS_unknown"
        return self._digest(str(patient_id), "PS", 16)

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
        return self._digest(patient_name, "PN", 12)


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
        except RedisUnavailableError as e:
            # Best-effort: losing the audit-linkage record degrades our ability
            # to answer an Art. 15 access request, but it is not a reason to
            # fail the clinical event, which is already de-identified.
            logger.error("shadow_record_store_failed event_id=%s code=%s", event_id, e.code)

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
    if ttl <= 0:
        # A non-positive TTL on a SETEX is a Redis error, but more importantly
        # an unbounded lifetime for sensitive data would silently defeat
        # Principle 5. Refuse rather than fall back to a persistent key.
        logger.error("refusing to store sensitive data without a positive TTL: key=%s", key)
        return False

    try:
        redis_client.setex(key, ttl, json.dumps({"value": value, "purpose": purpose}, default=str))
        logger.info("sensitive_data_stored key=%s ttl=%ds purpose=%s", key, ttl, purpose)
        return True
    except RedisUnavailableError as e:
        logger.error("sensitive_data_store_failed key=%s code=%s", key, e.code)
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
    except (RedisUnavailableError, json.JSONDecodeError) as e:
        logger.error("sensitive_data_read_failed key=%s error=%s", key, type(e).__name__)
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
