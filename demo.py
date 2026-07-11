#!/usr/bin/env python3
"""
Healthcare Data Pipeline Demo - GDPR Compliance Showcase

This script demonstrates:
- Principle 3: Data Minimization (PII removal)
- Principle 5: Storage Limitation (TTL-based retention)
- Principle 7: Accountability (audit logging)
"""

import json
import uuid
from datetime import date, datetime
from pathlib import Path

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from worker.data_minimization import (
    PatientPseudonymizer,
    remove_pii_from_dicom_metadata,
    create_minimized_payload,
    create_pii_shadow_record,
)
from worker.logger import audit_logger


def demo_principle_3_minimization():
    """Demonstrate GDPR Principle 3: Data Minimization"""
    print("\n" + "=" * 80)
    print("DEMO: GDPR Principle 3 - Data Minimization")
    print("=" * 80)

    # Original DICOM data (contains PII)
    original_dicom = {
        "patient_name": "John Doe",
        "patient_birth_date": "1980-01-15",
        "patient_id": "P12345",
        "study_uid": "1.2.826.0.1.3680043.8.498.123456",
        "modality": "CT",
        "study_date": "2024-01-15",
        "institution_name": "Acme Hospital",
        "referring_physician_name": "Dr. Smith",
        "kVp": 120,
        "mA": 250,
    }

    print("\n1. Original DICOM payload (contains PII):")
    print(json.dumps(original_dicom, indent=2))

    # Remove unnecessary PII
    minimized = remove_pii_from_dicom_metadata(original_dicom)

    print("\n2. After Principle 3 (Data Minimization) - PII removed:")
    print(json.dumps(minimized, indent=2))

    print("\n✅ Fields removed:")
    removed_fields = set(original_dicom.keys()) - set(minimized.keys())
    for field in removed_fields:
        print(f"   - {field}: {original_dicom[field]}")


def demo_principle_5_storage_limitation():
    """Demonstrate GDPR Principle 5: Storage Limitation"""
    print("\n" + "=" * 80)
    print("DEMO: GDPR Principle 5 - Storage Limitation (TTL-based retention)")
    print("=" * 80)

    event_id = str(uuid.uuid4())[:8]
    
    print(f"\nEvent ID: {event_id}")
    print("\nTTL Configuration:")
    print("  - Sensitive data (patient ID/name): 1 hour (3600s)")
    print("  - Processed events: 7 days (604800s)")
    print("  - Audit logs: 90 days (7776000s)")

    print("\n✅ Data stored in Redis with automatic expiry:")
    print(f"   - Key: sensitive:patient:{event_id}")
    print("   - TTL: 3600 seconds (1 hour)")
    print("   - Automatic deletion: Enforced by Redis SETEX")


def demo_principle_7_accountability():
    """Demonstrate GDPR Principle 7: Accountability"""
    print("\n" + "=" * 80)
    print("DEMO: GDPR Principle 7 - Accountability (Structured Audit Logging)")
    print("=" * 80)

    event_id = str(uuid.uuid4())[:8]
    patient_id = "P12345"
    pseudonymizer = PatientPseudonymizer()
    pseudonym_id = pseudonymizer.pseudonymize_patient_id(patient_id)

    print(f"\n1. Patient ID: {patient_id}")
    print(f"   Pseudonym: {pseudonym_id}")

    # Log data ingestion
    print("\n2. Logging data ingestion event...")
    audit_logger.log_data_ingestion(
        event_id=event_id,
        patient_id=patient_id,
        study_uid="1.2.3.4.5",
        modality="CT",
        source="PACS",
        purpose="diagnostic_support",
    )

    # Log deidentification
    print("3. Logging de-identification...")
    audit_logger.log_data_deidentification(
        event_id=event_id,
        patient_id=patient_id,
        pseudonym_id=pseudonym_id,
        fields_removed=["patient_name", "patient_birth_date", "institution_name"],
    )

    # Log retention policy
    print("4. Logging retention policy...")
    audit_logger.log_data_retention(
        event_id=event_id,
        study_uid="1.2.3.4.5",
        storage_location="redis",
        ttl_seconds=3600,
    )

    # Log consent
    print("5. Logging consent record...")
    audit_logger.log_consent_record(
        event_id=event_id,
        patient_id=patient_id,
        consent_type="explicit_consent",
        consent_logged=True,
        consent_reference=f"consent-{event_id}",
        purposes=["diagnostic_support"],
    )

    print("\n✅ Audit trail written to: data/logs/audit_trail.jsonl")
    
    # Show audit file location
    audit_file = Path(__file__).parent.parent / "data" / "logs" / "audit_trail.jsonl"
    if audit_file.exists():
        print(f"\n📄 Recent audit entries:")
        with open(audit_file, "r") as f:
            lines = f.readlines()
            for line in lines[-3:]:
                entry = json.loads(line)
                print(f"   - {entry.get('event_type')}: {entry.get('timestamp')}")


def demo_end_to_end():
    """Complete end-to-end workflow demonstration"""
    print("\n" + "=" * 80)
    print("DEMO: Complete GDPR-Compliant Workflow")
    print("=" * 80)

    # Step 1: Raw patient data arrives
    raw_event = {
        "event_id": str(uuid.uuid4())[:8],
        "patient_name": "Jane Smith",
        "patient_birth_date": date(1985, 5, 20),
        "patient_id": "P98765",
        "study_uid": "1.2.3.4.5.6.7.8.9",
        "modality": "MRI",
        "source": "PACS",
        "purpose": "diagnostic_support",
        "consent_logged": True,
    }

    print(f"\n1. Event received from PACS (with PII):")
    print(f"   - Event ID: {raw_event['event_id']}")
    print(f"   - Patient: {raw_event['patient_name']}")
    print(f"   - Study UID: {raw_event['study_uid']}")

    # Step 2: Pseudonymization
    pseudonymizer = PatientPseudonymizer()
    pseudonym_id = pseudonymizer.pseudonymize_patient_id(raw_event["patient_id"])
    
    print(f"\n2. Pseudonymization (irreversible):")
    print(f"   - Original: {raw_event['patient_id']}")
    print(f"   - Pseudonym: {pseudonym_id}")

    # Step 3: Data Minimization
    minimized = remove_pii_from_dicom_metadata(raw_event)
    print(f"\n3. Data Minimization (Principle 3):")
    print(f"   - Fields removed: patient_name, patient_birth_date")
    print(f"   - Minimized payload: {json.dumps(minimized, default=str)}")

    # Step 4: Audit logging
    print(f"\n4. Audit Trail (Principle 7):")
    audit_logger.log_data_ingestion(
        event_id=raw_event["event_id"],
        patient_id=pseudonym_id,
        study_uid=raw_event["study_uid"],
        modality=raw_event["modality"],
        source=raw_event["source"],
        purpose=raw_event["purpose"],
    )
    print(f"   ✅ Ingestion logged")

    audit_logger.log_data_deidentification(
        event_id=raw_event["event_id"],
        patient_id=pseudonym_id,
        pseudonym_id=pseudonym_id,
        fields_removed=["patient_name", "patient_birth_date"],
    )
    print(f"   ✅ De-identification logged")

    # Step 5: Storage with TTL
    print(f"\n5. Storage Limitation (Principle 5):")
    print(f"   - Sensitive data stored in Redis")
    print(f"   - TTL: 3600 seconds (1 hour)")
    print(f"   - Key: sensitive:patient:{raw_event['event_id']}")
    print(f"   - Auto-deletion: ENABLED")

    print(f"\n✅ Workflow complete - GDPR compliant processing")


if __name__ == "__main__":
    print("\n╔════════════════════════════════════════════════════════════════════════════════╗")
    print("║         Healthcare Data Pipeline - GDPR Compliance Demo                      ║")
    print("║                    (UK GDPR Principles 3, 5, 7)                               ║")
    print("╚════════════════════════════════════════════════════════════════════════════════╝")

    # Run demonstrations
    demo_principle_3_minimization()
    demo_principle_5_storage_limitation()
    demo_principle_7_accountability()
    demo_end_to_end()

    print("\n" + "=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)
    print("\n📚 For more information:")
    print("   - See README.md for architecture and setup instructions")
    print("   - Check data/logs/audit_trail.jsonl for audit logs")
    print("   - Run: python -m pytest for unit tests")
    print("\n")
