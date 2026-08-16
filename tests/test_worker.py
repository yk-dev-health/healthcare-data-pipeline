from io import BytesIO

import pytest
from pydicom.dataset import Dataset, FileDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

import worker.worker as worker_module
from common.errors import ConsentMissingError, PurposeLimitationError
from worker.celery_app import process_dicom_task
from worker.worker import build_fhir_resources, process_dicom_event, process_event, route_event


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
        "source": "PACS",
        # The worker now enforces the lawful-basis gate itself rather than
        # trusting the API to have done it (see test_worker_lawful_basis).
        "consent_logged": True,
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


def test_process_dicom_event_parses_synthetic_dicom_bytes(tmp_path, monkeypatch):
    output_path = tmp_path / "processed.jsonl"
    audit_path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(worker_module, "OUTPUT_PATH", str(output_path))
    monkeypatch.setattr(worker_module, "AUDIT_PATH", str(audit_path))

    dataset = Dataset()
    dataset.PatientName = "Synthetic^Patient"
    dataset.PatientBirthDate = "19800101"
    dataset.StudyInstanceUID = generate_uid()
    dataset.Modality = "CT"
    dataset.KVP = 120
    dataset.mA = 250

    file_meta = Dataset()
    file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = "1.2.3"

    synthetic_dicom = FileDataset("synthetic.dcm", dataset, file_meta=file_meta, preamble=b"\x00" * 128)
    buffer = BytesIO()
    synthetic_dicom.save_as(buffer, write_like_original=False)

    event = {
        "study_uid": "1.2.826.0.1.3680043.8.498.654321",
        "modality": "CT",
        "source": "PACS",
        # A consent_reference satisfies the lawful-basis gate on its own: it is
        # a pointer to the recorded consent, which is stronger evidence than
        # the boolean flag.
        "consent_reference": "consent-test",
        "dicom_bytes": buffer.getvalue(),
    }

    result = process_dicom_event(event)

    assert result["deidentified"]["patient_name"] == "REDACTED"
    assert result["deidentified"]["patient_birth_date"] == "REDACTED"
    assert output_path.exists()
    assert audit_path.exists()


class TestWorkerLawfulBasisGate:
    """Defence in depth: the worker enforces the lawful basis itself.

    The API rejects these at ingress, but the worker also consumes from a topic
    that replay tooling and future producers can write to. "The caller already
    checked" is not a control that can be demonstrated to an auditor, and the
    subscription is a wider trust boundary than the HTTP endpoint.

    Both failures are permanent by design: no amount of retrying makes
    unlawful processing lawful, so the handler quarantines rather than
    redelivers.
    """

    def _event(self, **overrides):
        event = {
            "study_uid": "1.2.826.0.1.3680043.8.498.111",
            "modality": "CT",
            "source": "PACS",
            "consent_logged": True,
            "purpose": "diagnostic_support",
        }
        event.update(overrides)
        return event

    @pytest.mark.parametrize("purpose", ["research", "marketing", "resale"])
    def test_unapproved_purpose_is_permanently_rejected(self, purpose):
        with pytest.raises(PurposeLimitationError) as excinfo:
            process_dicom_event(self._event(purpose=purpose))

        assert excinfo.value.retryable is False

    def test_missing_consent_is_permanently_rejected(self):
        with pytest.raises(ConsentMissingError) as excinfo:
            process_dicom_event(self._event(consent_logged=False))

        assert excinfo.value.retryable is False

    def test_consent_reference_alone_satisfies_the_gate(self):
        result = process_dicom_event(
            self._event(consent_logged=False, consent_reference="consent-abc")
        )

        assert result["deidentified"]["study_uid"] == "1.2.826.0.1.3680043.8.498.111"

    def test_rejection_context_carries_no_patient_data(self):
        with pytest.raises(PurposeLimitationError) as excinfo:
            process_dicom_event(
                self._event(purpose="research", patient_name="Jane Doe", patient_id="P123")
            )

        assert "Jane Doe" not in str(excinfo.value.to_dict())
        assert "P123" not in str(excinfo.value.to_dict())


class TestRouteEvent:
    """Schema-based dispatch on the receive path."""

    def test_dicom_payloads_route_to_the_dicom_processor(self):
        result = route_event(
            {
                "study_uid": "1.2.826.0.1.3680043.8.498.222",
                "modality": "CT",
                "consent_logged": True,
            }
        )

        assert "deidentified" in result

    def test_clinical_events_route_to_the_quality_processor(self):
        result = route_event(
            {
                "patient_id": "P123",
                "modality": "CT",
                "study_date": "2026-01-01",
                "slice_thickness": 1.2,
                "device_id": "CT_001",
            }
        )

        assert result["quality_score"] == 100

    def test_low_quality_events_are_flagged_not_discarded(self):
        """A low-quality study is still a real clinical fact; dropping it loses data."""
        result = route_event(
            {
                "patient_id": "123",
                "modality": "XRAY",
                "study_date": "2026-01-01",
                "slice_thickness": 1.2,
                "device_id": "CT_001",
            }
        )

        assert result["issues"], "quality problems must be reported"
        assert result["quality_score"] < 100


class TestDeidentificationEvidence:
    """The audit record must describe what actually happened.

    `fields_removed` used to be a hand-written two-item list covering only
    patient_name and patient_birth_date, while remove_pii_from_dicom_metadata
    strips up to twelve DICOM tags. Under Art. 5(2) the audit trail is the
    evidence that minimisation occurred, so evidence that under-reports the
    operation is worse than none: it is trusted.
    """

    def _deident_record(self, tmp_path, monkeypatch, event):
        import json

        import worker.logger as logger_module

        monkeypatch.setattr(logger_module, "AUDIT_LOG_DIR", str(tmp_path))
        process_dicom_event(event)

        entries = [
            json.loads(line)
            for line in (tmp_path / "audit_trail.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return next(e for e in entries if e["event_type"] == "data_deidentification")

    def test_every_stripped_field_is_reported(self, tmp_path, monkeypatch):
        record = self._deident_record(
            tmp_path,
            monkeypatch,
            {
                "study_uid": "1.2.826.0.1.3680043.8.498.777",
                "modality": "CT",
                "consent_logged": True,
                "patient_name": "Jane Doe",
                "patient_birth_date": "1985-05-17",
                "patient_id": "P999",
                "institution_name": "Acme Hospital",
                "referring_physician_name": "Dr Smith",
            },
        )

        assert set(record["fields_removed"]) == {
            "patient_name",
            "patient_birth_date",
            "institution_name",
            "referring_physician_name",
        }
        assert record["field_count"] == 4

    def test_absent_fields_are_not_claimed_as_removed(self, tmp_path, monkeypatch):
        """Over-reporting is the mirror-image failure and equally misleading."""
        record = self._deident_record(
            tmp_path,
            monkeypatch,
            {
                "study_uid": "1.2.826.0.1.3680043.8.498.778",
                "modality": "CT",
                "consent_logged": True,
                "patient_name": "Jane Doe",
            },
        )

        assert record["fields_removed"] == ["patient_name"]

    def test_medically_necessary_fields_are_retained(self, tmp_path, monkeypatch):
        record = self._deident_record(
            tmp_path,
            monkeypatch,
            {
                "study_uid": "1.2.826.0.1.3680043.8.498.779",
                "modality": "CT",
                "consent_logged": True,
                "patient_id": "P999",
                "patient_name": "Jane Doe",
            },
        )

        assert "study_uid" not in record["fields_removed"]
        assert "modality" not in record["fields_removed"]
        assert "patient_id" not in record["fields_removed"]
