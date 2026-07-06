from worker.celery_app import process_dicom_task
from worker.worker import build_fhir_resources, process_dicom_event, process_event


def test_celery_task_is_registered():
    assert process_dicom_task.name == "worker.process_dicom_task"


def test_process_event_valid():
    event = {
        "patient_id": "P123",
        "modality": "CT",
        "study_date": "2026-01-01",
        "slice_thickness": 1.2,
        "device_id": "MRI_001"
    }

    result = process_event(event)

    assert result["quality_score"] == 100
    assert result["issues"] == []


def test_process_event_quality_issues():
    event = {
        "patient_id": "123",
        "modality": "XRAY",
        "study_date": "3026-01-01",
        "slice_thickness": 100.0,
        "device_id": "MRI_001"
    }

    result = process_event(event)

    assert result["quality_score"] < 100
    assert "study_date_in_future" in result["issues"]
    assert "modality_invalid" in result["issues"]
    assert "slice_thickness_unrealistic" in result["issues"]
    assert "patient_id_format" in result["issues"]


def test_process_dicom_event_redacts_sensitive_fields():
    event = {
        "patient_name": "Jane Doe",
        "patient_birth_date": "1985-05-17",
        "study_uid": "1.2.826.0.1.3680043.8.498.123456",
        "modality": "CT",
        "kVp": 120,
        "mA": 250,
        "source": "PACS"
    }

    result = process_dicom_event(event)

    assert result["deidentified"]["patient_name"] == "REDACTED"
    assert result["deidentified"]["patient_birth_date"] == "REDACTED"
    assert result["deidentified"]["study_uid"] == "1.2.826.0.1.3680043.8.498.123456"
    assert result["indexed_fields"]["modality"] == "CT"
    assert result["minimized_payload"]["modality"] == "CT"
    assert "patient_name" not in result["minimized_payload"]


def test_build_fhir_resources_returns_minimal_resources():
    event = {
        "study_uid": "1.2.826.0.1.3680043.8.498.123456",
        "modality": "CT",
        "kVp": 120,
        "mA": 250,
        "source": "PACS"
    }

    resources = build_fhir_resources(event)

    assert resources["Patient"]["resourceType"] == "Patient"
    assert resources["Observation"]["resourceType"] == "Observation"
    assert resources["Observation"]["code"]["text"] == "CT acquisition metadata"
