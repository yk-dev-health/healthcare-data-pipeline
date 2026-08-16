"""Schema contracts and PHI-safe rendering of validation failures.

The clinical-plausibility tests are ordinary. The interesting half of this file
is ``TestValidationErrorsArePHIFree`` — it asserts that a rejected value never
reaches the error output, because Pydantic's default ``ValidationError.errors()``
includes the input and the framework's stock 422 body is built from exactly
that.
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import pytest
from pydantic import ValidationError

from common.schemas import (
    PHI_FIELDS,
    ClinicalEvent,
    DicomIngestionPayload,
    resolve_schema_name,
    safe_validation_errors,
    scrub_message,
)


def errors_for(model, payload):
    with pytest.raises(ValidationError) as excinfo:
        model.model_validate(payload)
    return excinfo.value


class TestClinicalEvent:
    def test_valid_event(self):
        event = ClinicalEvent(
            patient_id="P123",
            modality="CT",
            study_date="2026-01-01",
            slice_thickness=1.2,
            device_id="CT_001",
        )

        assert event.study_date == date(2026, 1, 1)

    def test_future_study_date_is_rejected(self):
        tomorrow = (date.today() + timedelta(days=1)).isoformat()

        exc = errors_for(
            ClinicalEvent,
            {
                "patient_id": "P123",
                "modality": "CT",
                "study_date": tomorrow,
                "slice_thickness": 1.2,
                "device_id": "CT_001",
            },
        )

        assert any(e["field"] == "study_date" for e in safe_validation_errors(exc))

    @pytest.mark.parametrize(
        "thickness", [0, -1.0, 50.1, 1000.0], ids=["zero", "negative", "just_over", "absurd"]
    )
    def test_implausible_slice_thickness_is_rejected(self, thickness):
        """Type-correct but clinically impossible values corrupt every downstream aggregate."""
        exc = errors_for(
            ClinicalEvent,
            {
                "patient_id": "P123",
                "modality": "CT",
                "study_date": "2026-01-01",
                "slice_thickness": thickness,
                "device_id": "CT_001",
            },
        )

        assert any(e["field"] == "slice_thickness" for e in safe_validation_errors(exc))

    def test_unknown_field_is_rejected(self):
        """An upstream typo must fail loudly, not become a silently missing field."""
        exc = errors_for(
            ClinicalEvent,
            {
                "patient_id": "P123",
                "modality": "CT",
                "study_date": "2026-01-01",
                "slice_thickness": 1.2,
                "device_id": "CT_001",
                "patient_ID": "P123",
            },
        )

        assert any(e["type"] == "extra_forbidden" for e in safe_validation_errors(exc))


class TestDicomIngestionPayload:
    def test_valid_payload(self, dicom_payload):
        payload = DicomIngestionPayload.model_validate(dicom_payload)

        assert payload.modality == "CT"
        assert payload.consent_logged is True

    @pytest.mark.parametrize(
        "study_uid",
        [
            pytest.param("not-a-uid", id="free_text"),
            pytest.param("1.2.3.abc", id="alpha_component"),
            pytest.param("", id="empty"),
            pytest.param("1." * 40, id="too_long"),
        ],
    )
    def test_invalid_study_uid_is_rejected(self, dicom_payload, study_uid):
        """study_uid becomes an index key and part of a pseudonym; it must be well-formed."""
        dicom_payload["study_uid"] = study_uid

        exc = errors_for(DicomIngestionPayload, dicom_payload)

        assert any(e["field"] == "study_uid" for e in safe_validation_errors(exc))

    def test_future_birth_date_is_rejected(self, dicom_payload):
        dicom_payload["patient_birth_date"] = (date.today() + timedelta(days=1)).isoformat()

        exc = errors_for(DicomIngestionPayload, dicom_payload)

        assert any(e["field"] == "patient_birth_date" for e in safe_validation_errors(exc))

    @pytest.mark.parametrize("field", ["kVp", "mA"])
    def test_non_positive_technical_values_are_rejected(self, dicom_payload, field):
        dicom_payload[field] = 0

        exc = errors_for(DicomIngestionPayload, dicom_payload)

        assert any(e["field"] == field for e in safe_validation_errors(exc))

    def test_optional_patient_fields_may_be_absent(self, dicom_payload):
        """De-identified submissions are valid; the identifiers are optional by design."""
        dicom_payload.pop("patient_name")
        dicom_payload.pop("patient_birth_date")

        payload = DicomIngestionPayload.model_validate(dicom_payload)

        assert payload.patient_name is None


class TestValidationErrorsArePHIFree:
    def test_rejected_patient_name_is_never_echoed(self, dicom_payload):
        """The disclosure this whole mechanism exists to prevent.

        Pydantic would put this exact string in ``errors()[0]["input"]``, and
        the framework's default 422 body is built from ``errors()``.
        """
        dicom_payload["patient_name"] = "Wolfeschlegelsteinhausenbergerdorff " * 10

        rendered = json.dumps(
            safe_validation_errors(errors_for(DicomIngestionPayload, dicom_payload))
        )

        assert "Wolfeschlegel" not in rendered

    def test_rejected_birth_date_value_is_not_echoed(self, dicom_payload):
        dicom_payload["patient_birth_date"] = "1985-13-45"

        rendered = json.dumps(safe_validation_errors(errors_for(DicomIngestionPayload, dicom_payload)))

        assert "1985" not in rendered
        assert "13-45" not in rendered

    def test_phi_fields_get_a_generic_message(self, dicom_payload):
        dicom_payload["patient_birth_date"] = "not-a-date"

        errors = safe_validation_errors(errors_for(DicomIngestionPayload, dicom_payload))
        entry = next(e for e in errors if e["field"] == "patient_birth_date")

        assert entry["message"] == (
            f"invalid value for restricted field (constraint: {entry['type']})"
        )

    def test_non_phi_fields_keep_a_useful_message(self, dicom_payload):
        """Scrubbing must not make errors useless: clinical fields keep their constraint text."""
        dicom_payload["modality"] = "PET"

        errors = safe_validation_errors(errors_for(DicomIngestionPayload, dicom_payload))
        entry = next(e for e in errors if e["field"] == "modality")

        assert "CT" in entry["message"], "the caller still needs to know the allowed values"

    def test_error_entries_never_contain_an_input_key(self, dicom_payload):
        dicom_payload["patient_name"] = "A"

        for entry in safe_validation_errors(errors_for(DicomIngestionPayload, dicom_payload)):
            assert set(entry) == {"field", "type", "message"}

    def test_custom_validator_message_is_scrubbed(self):
        """Defence in depth against a validator that interpolates the value into `msg`."""
        assert scrub_message("bad value Jane Doe here", "Jane Doe") == "bad value [redacted] here"

    def test_short_values_are_left_alone(self):
        """Stripping a 1-2 char value would mangle unrelated text and is not identifying."""
        assert scrub_message("Input should be 'CT' or 'MRI'", "CT") == "Input should be 'CT' or 'MRI'"

    def test_phi_field_list_covers_the_dicom_identifiers(self):
        for field in ("patient_name", "patient_id", "patient_birth_date"):
            assert field in PHI_FIELDS


class TestSchemaResolution:
    def test_explicit_attribute_wins(self):
        assert resolve_schema_name({"schema": "clinical_event"}, {"study_uid": "1.2.3"}) == (
            "clinical_event"
        )

    def test_unknown_attribute_falls_back_to_structure(self):
        assert resolve_schema_name({"schema": "nope"}, {"study_uid": "1.2.3"}) == "dicom_ingestion"

    def test_structural_fallback_keeps_legacy_messages_valid(self):
        """Messages published before attributes existed must not be quarantined en masse."""
        assert resolve_schema_name(None, {"study_uid": "1.2.3"}) == "dicom_ingestion"
        assert resolve_schema_name(None, {"patient_id": "P1"}) == "clinical_event"
