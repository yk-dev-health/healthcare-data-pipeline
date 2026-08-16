"""Canonical Pydantic contracts, shared by the producer and the consumer.

Two things live here that are easy to get wrong in a clinical pipeline.

**Schema-first on both sides of the queue.** The API validates on ingress, but
a subscriber that trusts the queue is a subscriber that crashes on the first
message written by a different producer, an older deploy, or a replay tool.
The same models are therefore applied again on receipt, selected by the
``schema`` message attribute so payload shapes can evolve independently.

**Validation errors must not leak PHI.** This is the subtle one. Pydantic's
``ValidationError.errors()`` includes the *rejected input value* by default, so
naively returning ``exc.errors()`` from a FastAPI handler — which is what the
framework's stock 422 body does — publishes patient names and dates of birth
into HTTP responses, access logs, and any error tracker in the path. That is a
personal-data disclosure caused by a validation bug, which is exactly the class
of incident UK GDPR Art. 33 asks you to report.

:func:`safe_validation_errors` produces field paths, error types and
constraint messages with the values stripped, and applies a second, stricter
rule to fields that are known to carry identifiers.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Iterable, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

__all__ = [
    "PHI_FIELDS",
    "ClinicalEvent",
    "DicomIngestionPayload",
    "SCHEMA_REGISTRY",
    "DEFAULT_SCHEMA",
    "resolve_schema_name",
    "safe_validation_errors",
    "scrub_message",
]

#: Field names that carry direct or strong indirect identifiers. Errors on
#: these fields never echo a constraint message that could contain the value.
PHI_FIELDS: frozenset[str] = frozenset(
    {
        "patient_name",
        "patient_id",
        "patient_birth_date",
        "patient_age",
        "patient_address",
        "patient_telephone",
        "institution_name",
        "referring_physician_name",
        "operator_name",
        "accession_number",
        "other_patient_ids",
    }
)

_REDACTED = "[redacted]"


# --------------------------------------------------------------------------
# PHI-safe rendering of validation failures
# --------------------------------------------------------------------------


def scrub_message(message: str, value: Any) -> str:
    """Remove any occurrence of ``value`` from a validation message.

    Defence in depth. We already ask Pydantic not to include inputs, but a
    hand-written validator such as ``raise ValueError(f"bad kVp {v}")`` would
    reintroduce the value through the message itself. Rather than audit every
    validator forever, we strip the value here as well.
    """
    if value is None:
        return message
    rendered = value if isinstance(value, str) else str(value)
    if len(rendered) < 3:
        # Too short to strip safely: substring-replacing "1" or "CT" would
        # mangle unrelated text, and a 1-2 character value is not identifying.
        return message
    return re.sub(re.escape(rendered), _REDACTED, message)


def safe_validation_errors(
    exc: ValidationError,
    *,
    phi_fields: Iterable[str] = PHI_FIELDS,
) -> list[dict[str, Any]]:
    """Render a Pydantic error as a PHI-free, machine-readable list.

    Each entry has ``field`` (dotted path), ``type`` (stable Pydantic error
    code, safe to branch on) and ``message`` (human-readable constraint).
    Rejected values are never included.
    """
    phi = frozenset(phi_fields)
    rendered: list[dict[str, Any]] = []

    # `input` is requested but never emitted: we need the rejected value in
    # hand so `scrub_message` can strip any copy a hand-written validator baked
    # into `msg`. Only `field`, `type` and the scrubbed `message` are returned.
    for error in exc.errors(include_url=False, include_context=False):
        location = tuple(str(part) for part in error.get("loc", ()))
        field_path = ".".join(location) or "__root__"
        leaf = location[-1] if location else ""
        error_type = str(error.get("type", "value_error"))

        if leaf in phi or field_path in phi:
            # Strictest rule: for identifier-bearing fields we publish only
            # that the field was rejected and by which constraint.
            message = f"invalid value for restricted field (constraint: {error_type})"
        else:
            message = scrub_message(str(error.get("msg", "invalid value")), error.get("input"))

        rendered.append({"field": field_path, "type": error_type, "message": message})

    return rendered


# --------------------------------------------------------------------------
# Domain models
# --------------------------------------------------------------------------


class ClinicalEvent(BaseModel):
    """Generic medical imaging acquisition event.

    ``extra="forbid"`` is deliberate: silently accepting unknown keys in a
    clinical ingress means an upstream typo (``patient_ID``) becomes a
    permanently missing field instead of a loud rejection, and it lets
    unexpected identifiers ride along into storage untracked.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    patient_id: str = Field(min_length=1, max_length=64)
    modality: Literal["CT", "MRI", "US"]
    study_date: date
    slice_thickness: float
    device_id: str = Field(min_length=1, max_length=64)

    @field_validator("slice_thickness")
    @classmethod
    def validate_slice_thickness(cls, v: float) -> float:
        # Clinical plausibility, not just type safety. A negative or 500 mm
        # slice is a miscalibrated modality or a unit error upstream, and
        # letting it through corrupts every downstream aggregate.
        if v <= 0:
            raise ValueError("slice_thickness must be > 0")
        if v > 50:
            raise ValueError("slice_thickness is unrealistic (> 50 mm)")
        return v

    @field_validator("study_date")
    @classmethod
    def validate_study_date(cls, v: date) -> date:
        if v > date.today():
            raise ValueError("study_date must not be in the future")
        return v


class DicomIngestionPayload(BaseModel):
    """DICOM metadata submitted by a PACS or ingestion gateway."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    patient_name: str | None = Field(default=None, max_length=256)
    patient_birth_date: date | None = None
    study_uid: str = Field(min_length=1, max_length=64)
    modality: Literal["CT", "MRI", "US", "DX", "CR"]
    kVp: float | None = None
    mA: float | None = None
    consent_logged: bool = False
    source: str = Field(default="PACS", max_length=64)
    purpose: Literal["diagnostic_support", "research", "marketing"] = "diagnostic_support"

    @field_validator("patient_name")
    @classmethod
    def validate_patient_name(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if len(v.strip()) < 2:
            # No value in the message: this field is in PHI_FIELDS, but the
            # rule is cheap to hold everywhere rather than only where it is
            # currently enforced.
            raise ValueError("patient_name is too short")
        return v.strip()

    @field_validator("study_uid")
    @classmethod
    def validate_study_uid(cls, v: str) -> str:
        # DICOM PS3.5 defines a UID as dot-separated numeric components, max
        # 64 characters. Enforcing it here stops a free-text value from being
        # used as an index key and as part of a pseudonym.
        if not re.fullmatch(r"[0-9]+(\.[0-9]+)*", v):
            raise ValueError("study_uid must be a dot-separated numeric DICOM UID")
        return v

    @field_validator("patient_birth_date")
    @classmethod
    def validate_birth_date(cls, v: date | None) -> date | None:
        if v is not None and v > date.today():
            raise ValueError("patient_birth_date must not be in the future")
        return v

    @field_validator("kVp", "mA")
    @classmethod
    def validate_technical_values(cls, v: float | None) -> float | None:
        if v is not None and v <= 0:
            raise ValueError("technical acquisition values must be > 0")
        return v


#: Schema identifier -> model. The identifier travels in the Pub/Sub message
#: attribute ``schema`` so a subscriber can pick the right contract without
#: parsing the body first, and so both shapes can share one topic.
SCHEMA_REGISTRY: dict[str, type[BaseModel]] = {
    "clinical_event": ClinicalEvent,
    "dicom_ingestion": DicomIngestionPayload,
}

DEFAULT_SCHEMA = "clinical_event"

#: Bumped when a model changes in a way subscribers must notice. Published as
#: the ``schema_version`` message attribute.
SCHEMA_VERSION = "1"


def resolve_schema_name(
    attributes: Mapping[str, str] | None,
    payload: Mapping[str, Any] | None = None,
) -> str:
    """Pick the schema identifier for an incoming message.

    Prefers the explicit ``schema`` attribute. Falls back to a structural hint
    so that messages published before attributes were introduced still
    validate rather than being quarantined en masse during a rollout.
    """
    if attributes:
        declared = attributes.get("schema")
        if declared in SCHEMA_REGISTRY:
            return declared

    if payload and "study_uid" in payload:
        return "dicom_ingestion"

    return DEFAULT_SCHEMA
