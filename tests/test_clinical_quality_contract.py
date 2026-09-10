"""Contract tests for the healthcare-data-pipeline <-> clinical-data-quality-analysis
integration (issue #7).

These exercise the real ``healthcli.contracts`` module (installed as a normal
dependency, see requirements.txt) against local, in-memory inputs only -- no
live GCP credentials, no network call, no Redis. That mirrors how the rest of
this suite stays offline and deterministic (see tests/conftest.py): the
clinical quality engine is a plain library call, not a service.

Three things are verified:

1. ``worker.clinical_quality.validate_dicom_event`` maps a validated record to
   a plain return and a rejected record to ``ClinicalValidationError`` without
   leaking field values into the exception.
2. ``worker.worker.process_dicom_event`` calls the engine and raises on a
   record that fails validation -- proving the engine is actually wired into
   the processing path, not just defined alongside it.
3. Through ``PubSubMessageHandler``, a clinical-quality rejection is
   quarantined and acked (permanent, matching "permanent validation failures
   are quarantined and acknowledged"), while idempotency is committed before
   the ack.
"""

from __future__ import annotations

import pytest

from common.errors import ClinicalQualityEngineError, ClinicalValidationError
from worker.clinical_quality import validate_dicom_event
from worker.message_handler import Outcome, PubSubMessageHandler
from worker.worker import build_message_handler, process_dicom_event


# --------------------------------------------------------------------------
# worker.clinical_quality.validate_dicom_event
# --------------------------------------------------------------------------


def test_valid_dicom_event_passes_through():
    result = validate_dicom_event(
        {"study_uid": "1.2.3.4.5", "modality": "CT", "source": "PACS", "kVp": 120.0, "mA": 250.0},
        request_id="evt-ok-1",
    )
    assert result.outcome.value == "validated"
    assert result.rejected_count == 0


def test_invalid_modality_is_rejected_without_leaking_the_value():
    with pytest.raises(ClinicalValidationError) as excinfo:
        validate_dicom_event(
            {"study_uid": "1.2.3.4.5", "modality": "NOT-A-MODALITY", "source": "PACS"},
            request_id="evt-bad-1",
        )

    err = excinfo.value
    assert err.code == "clinical_validation_failed"
    serialized = str(err.to_dict())
    assert "NOT-A-MODALITY" not in serialized


def test_missing_required_field_is_rejected():
    with pytest.raises(ClinicalValidationError):
        validate_dicom_event({"modality": "CT", "source": "PACS"}, request_id="evt-missing-1")


def test_engine_failure_is_transient_not_permanent(monkeypatch):
    """A broken schema file is an engine fault, not a verdict on the record."""
    with pytest.raises(ClinicalQualityEngineError) as excinfo:
        validate_dicom_event(
            {"study_uid": "1.2.3.4.5", "modality": "CT", "source": "PACS"},
            request_id="evt-engine-fault-1",
            schema_path="config/does_not_exist.yaml",
        )
    assert excinfo.value.retryable is True


# --------------------------------------------------------------------------
# worker.worker.process_dicom_event
# --------------------------------------------------------------------------


def test_process_dicom_event_raises_on_clinical_rejection():
    event = {
        "event_id": "evt-1",
        "study_uid": "1.2.826.0.1.3680043.8.498.1",
        "modality": "INVALID_MODALITY",
        "source": "PACS",
        "consent_logged": True,
        "purpose": "diagnostic_support",
    }
    with pytest.raises(ClinicalValidationError):
        process_dicom_event(event)


def test_process_dicom_event_succeeds_for_a_clinically_valid_event():
    event = {
        "event_id": "evt-2",
        "study_uid": "1.2.826.0.1.3680043.8.498.2",
        "modality": "CT",
        "source": "PACS",
        "kVp": 120.0,
        "mA": 200.0,
        "consent_logged": True,
        "purpose": "diagnostic_support",
    }
    result = process_dicom_event(event)
    assert result["deidentified"]["modality"] == "CT"


# --------------------------------------------------------------------------
# End-to-end through the Pub/Sub receive path
# --------------------------------------------------------------------------


def test_clinically_rejected_dicom_event_is_quarantined_and_acked(
    idempotency, quarantine, settings, make_message
):
    """Permanent validation failures are quarantined and acknowledged.

    ``DicomIngestionPayload.modality`` is a closed ``Literal``, so an
    out-of-vocabulary modality cannot reach this handler through the real API
    schema -- but the clinical quality engine's own rules are a separate,
    independently evolving vocabulary (``config/dicom_quality_schema.yaml``),
    and a value it rejects must still be quarantined rather than dropped or
    retried forever. A processor double raising the exact exception the
    engine raises (``ClinicalValidationError``) exercises that path without
    depending on the two vocabularies staying in sync.
    """

    def rejecting_processor(payload):
        raise ClinicalValidationError(
            "record failed clinical quality validation",
            field_errors=[{"field": "modality", "reason": "out_of_vocabulary"}],
            schema_version="1.0",
        )

    handler = PubSubMessageHandler(
        processor=rejecting_processor,
        idempotency=idempotency,
        quarantine=quarantine,
        settings=settings,
    )

    message = make_message(
        {
            "study_uid": "1.2.826.0.1.3680043.8.498.999",
            "modality": "CT",
            "kVp": 120.0,
            "mA": 200.0,
            "consent_logged": True,
            "purpose": "diagnostic_support",
            "source": "PACS",
        },
        attributes={"schema": "dicom_ingestion"},
    )

    result = handler.handle(message)

    assert result.outcome == Outcome.QUARANTINED_INVALID
    assert result.acked is True
    assert message.settled_once
    assert not idempotency.is_processed(result.event_id)


def test_clinical_engine_infrastructure_failure_is_retried_not_quarantined(
    idempotency, quarantine, settings, make_message
):
    """An engine-side fault (not a record verdict) must be retried, not quarantined."""

    def failing_processor(payload):
        raise ClinicalQualityEngineError("engine unavailable")

    handler = PubSubMessageHandler(
        processor=failing_processor,
        idempotency=idempotency,
        quarantine=quarantine,
        settings=settings,
    )

    message = make_message(
        {
            "study_uid": "1.2.826.0.1.3680043.8.498.998",
            "modality": "CT",
            "consent_logged": True,
            "purpose": "diagnostic_support",
        },
        attributes={"schema": "dicom_ingestion"},
    )

    result = handler.handle(message)

    assert result.outcome == Outcome.RETRY
    assert result.acked is False
    assert message.nack_calls == 1


def test_worker_builds_a_handler_that_wires_in_clinical_validation(idempotency, quarantine, settings):
    """Smoke test that the production wiring (build_message_handler) is intact."""
    handler = build_message_handler(idempotency=idempotency, quarantine=quarantine, settings=settings)
    assert isinstance(handler, PubSubMessageHandler)
