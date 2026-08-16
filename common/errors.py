"""Error taxonomy for the healthcare ingestion pipeline.

Every failure in an at-least-once pipeline has to answer one operational
question: **should this message be redelivered?** Getting that answer wrong is
expensive in both directions.

* Retrying a permanently broken message forever pins a subscriber on a poison
  payload, inflates the backlog, and (with a dead-letter policy attached)
  silently multiplies downstream load.
* Acking a transient failure destroys a clinical event. In a regulated
  pipeline that is unrecoverable data loss, not a blip.

So exceptions are split into exactly two families, and the Pub/Sub handler
dispatches on the *family* rather than on the concrete type. New error types
can be added without touching the ack/nack decision logic.

PHI safety
----------
Exception messages end up in logs, in the quarantine sink, and occasionally in
HTTP responses. None of those are appropriate stores for patient data, so the
rule for this module is: an exception carries **field names, error codes and
counts — never field values**. ``context`` is validated against that rule by
:func:`assert_phi_free` in the tests.
"""

from __future__ import annotations

from typing import Any, Mapping

__all__ = [
    "PipelineError",
    "PermanentError",
    "TransientError",
    "MessageDecodeError",
    "SchemaValidationError",
    "PayloadTooLargeError",
    "ConsentMissingError",
    "PurposeLimitationError",
    "RetryBudgetExhaustedError",
    "RedisUnavailableError",
    "IdempotencyBackendError",
    "PublishError",
    "DownstreamUnavailableError",
]


class PipelineError(Exception):
    """Base class for every error this pipeline raises deliberately.

    Attributes:
        code: Stable machine-readable identifier. Safe to alert on, safe to
            put in a dashboard, and guaranteed not to contain patient data.
        context: PHI-free structured detail (field names, counts, durations).
    """

    code: str = "pipeline_error"
    #: Whether the Pub/Sub handler should let the message be redelivered.
    retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code
        self.context: dict[str, Any] = dict(context or {})

    def to_dict(self) -> dict[str, Any]:
        """Render the error for logs and the quarantine sink."""
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "context": self.context,
        }

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"{type(self).__name__}(code={self.code!r}, message={self.message!r})"


class PermanentError(PipelineError):
    """The message can never succeed; redelivering it only wastes capacity.

    The handler acks these and writes a PHI-free record to the quarantine sink
    so that an operator can investigate the upstream producer.
    """

    code = "permanent_error"
    retryable = False


class TransientError(PipelineError):
    """A dependency is temporarily unavailable; the same message may succeed later.

    The handler nacks these so Pub/Sub redelivers with its own backoff, up to
    the configured delivery-attempt budget.
    """

    code = "transient_error"
    retryable = True


# --------------------------------------------------------------------------
# Permanent failures - bad input. Quarantine, do not retry.
# --------------------------------------------------------------------------


class MessageDecodeError(PermanentError):
    """Message body is not UTF-8, or not a JSON object."""

    code = "message_decode_failed"


class SchemaValidationError(PermanentError):
    """Payload did not satisfy the Pydantic contract for its schema.

    ``field_errors`` holds one entry per rejected field in the PHI-free shape
    produced by :func:`common.schemas.safe_validation_errors`.
    """

    code = "schema_validation_failed"

    def __init__(
        self,
        message: str = "payload failed schema validation",
        *,
        field_errors: list[dict[str, Any]] | None = None,
        schema: str | None = None,
    ) -> None:
        self.field_errors = list(field_errors or [])
        self.schema = schema
        super().__init__(
            message,
            context={
                "schema": schema,
                "error_count": len(self.field_errors),
                "fields": [e.get("field") for e in self.field_errors],
            },
        )

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        payload["field_errors"] = self.field_errors
        return payload


class PayloadTooLargeError(PermanentError):
    """Payload exceeds the transport or policy size limit."""

    code = "payload_too_large"


class ConsentMissingError(PermanentError):
    """No recorded consent for this event (UK GDPR Art. 6/9, Art. 7)."""

    code = "consent_missing"


class PurposeLimitationError(PermanentError):
    """Requested processing purpose is outside the registered purposes.

    UK GDPR Art. 5(1)(b): personal data may only be processed for the specified,
    explicit and legitimate purposes it was collected for.
    """

    code = "purpose_limitation_violated"


class RetryBudgetExhaustedError(PermanentError):
    """A retryable failure kept failing until the delivery budget ran out.

    Promoted from transient to permanent by the handler so the message stops
    circulating and lands in quarantine with its attempt count intact.
    """

    code = "retry_budget_exhausted"


# --------------------------------------------------------------------------
# Transient failures - infrastructure. Redeliver.
# --------------------------------------------------------------------------


class RedisUnavailableError(TransientError):
    """Redis could not be reached, or the command failed at the transport level."""

    code = "redis_unavailable"


class IdempotencyBackendError(TransientError):
    """The idempotency backend is unusable and the fail mode is ``closed``.

    Processing without a working dedup store risks duplicate clinical records,
    so the safe default is to stop consuming rather than proceed blind.
    """

    code = "idempotency_backend_unavailable"


class PublishError(TransientError):
    """Publishing to Pub/Sub failed after the client's own retries."""

    code = "publish_failed"


class DownstreamUnavailableError(TransientError):
    """A downstream sink (BigQuery, object store, index) rejected the write."""

    code = "downstream_unavailable"
