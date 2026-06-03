from datetime import date, timedelta

import pytest

from api.main import Event


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
