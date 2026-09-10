"""Bridge to the clinical quality engine owned by clinical-data-quality-analysis.

That repository owns tabular clinical quality rules, FHIR validation, and
quality metrics; this pipeline owns event-driven ingestion, Pub/Sub delivery,
Redis idempotency, retries, and quarantine. The two are connected through the
typed contract in ``healthcli.contracts`` (``ProcessingRequest`` /
``ProcessingResult`` / ``RecordOutcome``) rather than by duplicating clinical
validation logic here.

Nothing in ``healthcli.contracts`` imports Pub/Sub, Redis, or BigQuery -- it
is a plain function call. This module is the seam that:

1. Maps one de-identified DICOM event to the single-record ``ProcessingRequest``
   the engine expects.
2. Calls ``process_request`` and maps ``RecordOutcome.REJECTED`` to
   :class:`~common.errors.ClinicalValidationError` (permanent: quarantine and
   ack -- the same input will fail again) and any unexpected failure of the
   call itself to :class:`~common.errors.ClinicalQualityEngineError`
   (transient: nack and retry -- the engine may be temporarily unreachable,
   e.g. a schema file mid-deploy).
3. Never passes ``patient_name`` or ``patient_birth_date`` to the engine, and
   never lets a rejection reason carry a field *value* -- only field names and
   error types, matching the PHI-minimization rule the rest of this pipeline
   follows (see common/errors.py, common/quarantine.py).

``healthcli`` is an optional dependency (see requirements.txt): importing it
lazily, inside :func:`validate_dicom_event`, keeps every existing test and the
API/worker import path working in an environment where the sibling repository
is not installed. Calling this function without it installed is treated as a
transient engine failure, not a silent pass-through -- skipping quality
validation is a decision an operator must be able to see in metrics, not one
this module makes quietly.
"""

from __future__ import annotations

from typing import Any

from common.errors import ClinicalQualityEngineError, ClinicalValidationError

__all__ = ["DICOM_QUALITY_SCHEMA_PATH", "validate_dicom_event"]

#: De-identified DICOM acquisition metadata schema, independent of the
#: tabular clinical-trial schema that ships with clinical-data-quality-analysis.
DICOM_QUALITY_SCHEMA_PATH = "config/dicom_quality_schema.yaml"

#: Fields from a worker event dict that are safe to hand to the quality
#: engine. An allowlist, not a denylist: a field added to the event envelope
#: in the future is excluded by default rather than forwarded by accident.
_ALLOWED_FIELDS = frozenset({"study_uid", "modality", "kVp", "mA", "source"})


def _to_engine_record(event: dict[str, Any]) -> dict[str, Any]:
    return {field: event[field] for field in _ALLOWED_FIELDS if event.get(field) is not None}


def validate_dicom_event(
    event: dict[str, Any],
    *,
    request_id: str,
    schema_path: str = DICOM_QUALITY_SCHEMA_PATH,
) -> "ProcessingResult":  # noqa: F821 - forward ref to the optional dependency's type
    """Validate one DICOM event through the clinical quality engine.

    Raises:
        ClinicalValidationError: The engine ran and rejected the record.
            Permanent -- the caller should quarantine and ack.
        ClinicalQualityEngineError: The engine could not be invoked (not
            installed, or raised unexpectedly). Transient -- the caller
            should nack and let Pub/Sub redeliver.
    """
    try:
        from healthcli.contracts import ProcessingRequest, RecordOutcome, process_request
    except ImportError as exc:
        raise ClinicalQualityEngineError(
            "clinical quality engine (healthcli) is not installed",
            code="clinical_quality_engine_not_installed",
        ) from exc

    request = ProcessingRequest(
        request_id=request_id,
        records=[_to_engine_record(event)],
    )

    try:
        result = process_request(request, schema_path=schema_path)
    except ClinicalQualityEngineError:
        raise
    except Exception as exc:  # noqa: BLE001 - any engine-side fault is transient here
        raise ClinicalQualityEngineError(
            "clinical quality engine raised while processing the request",
            context={"error": type(exc).__name__},
        ) from exc

    if result.outcome is RecordOutcome.REJECTED:
        raise ClinicalValidationError(
            "record failed clinical quality validation",
            field_errors=[
                {"field": r.error_type, "reason": r.reason} for r in result.rejected_records
            ],
            schema_version=result.schema_version,
        )

    return result
