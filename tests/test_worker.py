from worker.worker import process_event


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
