from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from api.main import Event, app


def test_event_model_valid():
    event = Event(
        patient_id="P123",
        modality="CT",
        study_date="2026-01-01",
        slice_thickness=1.2,
        device_id="MRI_001"
    )

    assert event.patient_id == "P123"
    assert event.modality == "CT"
    assert event.study_date == date(2026, 1, 1)


def test_event_model_future_date():
    future_date = (date.today() + timedelta(days=1)).isoformat()

    with pytest.raises(ValueError):
        Event(
            patient_id="P123",
            modality="CT",
            study_date=future_date,
            slice_thickness=1.2,
            device_id="MRI_001"
        )


def test_dicom_payload_is_anonymized_and_accepted():
    client = TestClient(app)
    response = client.post(
        "/dicom/events",
        json={
            "patient_name": "John Doe",
            "patient_birth_date": "1980-01-01",
            "study_uid": "1.2.826.0.1.3680043.8.498.123456",
            "modality": "CT",
            "kVp": 120,
            "mA": 250,
            "consent_logged": True,
            "source": "PACS"
        }
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "queued"
    assert payload["deidentified"]["patient_name"] == "REDACTED"
    assert payload["deidentified"]["patient_birth_date"] == "REDACTED"
    assert payload["deidentified"]["study_uid"] == "1.2.826.0.1.3680043.8.498.123456"


def test_admin_consent_log_requires_admin_role():
    client = TestClient(app)

    forbidden = client.get("/admin/consent-log")
    assert forbidden.status_code == 403

    allowed = client.get("/admin/consent-log", headers={"x-role": "admin"})
    assert allowed.status_code == 200
    assert "consent_log" in allowed.json()
