"""Pub/Sub receive path: decode, validate, deduplicate, dispatch, ack.

This module holds the whole ack/nack decision and nothing else. It is
deliberately decoupled from ``SubscriberClient`` — :meth:`PubSubMessageHandler.handle`
takes anything with ``.data``, ``.attributes``, ``.ack()`` and ``.nack()``, so
the entire delivery-semantics matrix can be unit-tested without a broker, an
emulator, or a network.

The decision table
------------------

=========================================  ========  ==============================
Situation                                  Action    Rationale
=========================================  ========  ==============================
Body is not UTF-8 / not a JSON object      ack       Cannot ever parse. Quarantine.
Payload fails schema validation            ack       Cannot ever validate. Quarantine.
Purpose limitation / consent violated      ack       Must not be processed at all.
Already completed (``DUPLICATE``)          ack       Work was done. Drop silently.
Another worker holds the lease             nack      Do not race; let it finish.
Dedup backend down, fail_mode=closed       nack      Never process unprotected.
Processing raised a transient error        nack      Retry, until the budget runs out.
Delivery budget exhausted                  ack       Stop the poison loop. Quarantine.
Processing succeeded                       ack       Commit the dedup marker first.
Unexpected exception                       nack      Assume transient; budget applies.
=========================================  ========  ==============================

The last row matters. An unknown exception is treated as *transient* rather
than permanent, because discarding a clinical event on a bug we have not
classified is unrecoverable, whereas retrying one is merely wasteful — and the
delivery budget bounds that waste. The bias is towards keeping data.

Logging discipline: this module logs ``event_id``, error codes, digests and
counters. It never logs the message body, and never interpolates a payload
value into a log line.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Mapping, Protocol

from pydantic import BaseModel, ValidationError

from common.config import Settings, get_settings
from common.errors import (
    ConsentMissingError,
    MessageDecodeError,
    PermanentError,
    PipelineError,
    RetryBudgetExhaustedError,
    SchemaValidationError,
    TransientError,
)
from common.idempotency import ClaimState, IdempotencyStore, derive_event_id
from common.quarantine import QuarantineSink
from common.schemas import SCHEMA_REGISTRY, resolve_schema_name, safe_validation_errors

__all__ = ["Outcome", "HandlerResult", "PubSubMessageHandler", "decode_message_body"]

logger = logging.getLogger(__name__)


class Outcome(str, Enum):
    """What the handler did with a message. Emitted for metrics."""

    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    IN_FLIGHT = "in_flight"
    QUARANTINED_INVALID = "quarantined_invalid"
    QUARANTINED_EXHAUSTED = "quarantined_exhausted"
    RETRY = "retry"


@dataclass(frozen=True)
class HandlerResult:
    """Structured record of one delivery. Returned for tests and metrics."""

    outcome: Outcome
    acked: bool
    event_id: str | None = None
    error_code: str | None = None
    delivery_attempt: int | None = None
    degraded: bool = False

    @property
    def nacked(self) -> bool:
        return not self.acked


class SupportsAck(Protocol):
    """The slice of ``pubsub_v1.subscriber.message.Message`` we depend on."""

    data: bytes
    attributes: Mapping[str, str]

    def ack(self) -> None: ...
    def nack(self) -> None: ...


def decode_message_body(raw: bytes) -> dict[str, Any]:
    """Decode a Pub/Sub body into a JSON object.

    Raises:
        MessageDecodeError: Permanent. Not UTF-8, not JSON, or not an object.
            Note the error context deliberately records *why* and *how big*,
            never *what* — a decode failure often means the body is binary or
            truncated PHI, and echoing it into logs is the same disclosure the
            rest of the pipeline works to avoid.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MessageDecodeError(
            "message body is not valid UTF-8",
            context={"bytes": len(raw), "error": "UnicodeDecodeError"},
        ) from exc

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise MessageDecodeError(
            "message body is not valid JSON",
            # Position is a safe diagnostic: it is an offset, not content.
            context={"bytes": len(raw), "error": "JSONDecodeError", "position": exc.pos},
        ) from exc

    if not isinstance(payload, dict):
        raise MessageDecodeError(
            "message body must be a JSON object",
            context={"bytes": len(raw), "json_type": type(payload).__name__},
        )

    return payload


class PubSubMessageHandler:
    """Turns a delivered message into exactly one ack or nack.

    Args:
        processor: The business operation. Receives the validated payload as a
            dict and may raise :class:`~common.errors.PipelineError` subclasses
            to steer the retry decision.
        idempotency: Duplicate-suppression store.
        quarantine: Sink for permanently-failed messages.
        settings: Runtime configuration.
        validate: Injection point overriding schema validation.
        on_result: Optional metrics/audit hook, called once per delivery.
    """

    def __init__(
        self,
        *,
        processor: Callable[[dict[str, Any]], Any],
        idempotency: IdempotencyStore | None = None,
        quarantine: QuarantineSink | None = None,
        settings: Settings | None = None,
        validate: Callable[[dict[str, Any], Mapping[str, str]], BaseModel] | None = None,
        on_result: Callable[[HandlerResult], None] | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._processor = processor
        self._idempotency = idempotency or IdempotencyStore()
        self._quarantine = quarantine or QuarantineSink()
        self._validate = validate or self._default_validate
        self._on_result = on_result

    # -- entry point --------------------------------------------------------

    def handle(self, message: SupportsAck) -> HandlerResult:
        """Process one delivery. Always acks or nacks exactly once."""
        raw: bytes = getattr(message, "data", b"") or b""
        attributes: Mapping[str, str] = dict(getattr(message, "attributes", {}) or {})
        attempt = self._delivery_attempt(message)

        # -- 1. decode and validate: permanent failures leave the queue -----
        try:
            payload = decode_message_body(raw)
            schema_name = resolve_schema_name(attributes, payload)
            validated = self._validate(payload, attributes)
        except PermanentError as exc:
            return self._quarantine_and_ack(
                message,
                raw=raw,
                attributes=attributes,
                error=exc,
                event_id=attributes.get("event_id"),
                schema=attributes.get("schema"),
                delivery_attempt=attempt,
                outcome=Outcome.QUARANTINED_INVALID,
            )

        event_id = derive_event_id(payload, raw)

        # -- 2. claim the event ---------------------------------------------
        # A transient backend failure here propagates out of the try below as a
        # nack: never process a clinical event without duplicate protection.
        try:
            claim = self._idempotency.claim(event_id)
        except TransientError as exc:
            return self._retry_or_exhaust(
                message,
                raw=raw,
                attributes=attributes,
                error=exc,
                event_id=event_id,
                schema=schema_name,
                delivery_attempt=attempt,
            )

        if claim.state is ClaimState.DUPLICATE:
            logger.info("duplicate_event_skipped event_id=%s", event_id)
            message.ack()
            return self._finish(
                HandlerResult(Outcome.DUPLICATE, acked=True, event_id=event_id,
                              delivery_attempt=attempt)
            )

        if claim.state is ClaimState.IN_FLIGHT:
            # Another subscriber holds an unexpired lease. Nacking rather than
            # dropping keeps the guarantee intact if that worker dies: the
            # lease expires and a redelivery picks the work up.
            logger.info("event_in_flight_elsewhere event_id=%s", event_id)
            message.nack()
            return self._finish(
                HandlerResult(Outcome.IN_FLIGHT, acked=False, event_id=event_id,
                              delivery_attempt=attempt)
            )

        # -- 3. process -----------------------------------------------------
        try:
            self._processor(self._to_payload(validated, payload))

        except PermanentError as exc:
            # Business-rule rejection (consent, purpose limitation). The event
            # is valid JSON but must not be processed, now or ever.
            self._idempotency.release(claim)
            return self._quarantine_and_ack(
                message,
                raw=raw,
                attributes=attributes,
                error=exc,
                event_id=event_id,
                schema=schema_name,
                delivery_attempt=attempt,
                outcome=Outcome.QUARANTINED_INVALID,
            )

        except TransientError as exc:
            self._idempotency.release(claim)
            return self._retry_or_exhaust(
                message,
                raw=raw,
                attributes=attributes,
                error=exc,
                event_id=event_id,
                schema=schema_name,
                delivery_attempt=attempt,
            )

        except Exception as exc:  # noqa: BLE001 - unclassified: keep the data
            logger.exception(
                "unclassified_processing_error event_id=%s error=%s",
                event_id,
                type(exc).__name__,
            )
            self._idempotency.release(claim)
            wrapped = TransientError(
                "unclassified processing error",
                code="unclassified_error",
                context={"error": type(exc).__name__},
            )
            return self._retry_or_exhaust(
                message,
                raw=raw,
                attributes=attributes,
                error=wrapped,
                event_id=event_id,
                schema=schema_name,
                delivery_attempt=attempt,
            )

        # -- 4. commit, then ack --------------------------------------------
        # Commit first: if the process dies between commit and ack the message
        # is redelivered and correctly recognised as a duplicate. The reverse
        # order would ack work that dedup has no record of.
        self._idempotency.commit(claim)
        message.ack()
        logger.info(
            "event_processed event_id=%s schema=%s attempt=%s degraded=%s",
            event_id,
            schema_name,
            attempt,
            claim.degraded,
        )
        return self._finish(
            HandlerResult(
                Outcome.PROCESSED,
                acked=True,
                event_id=event_id,
                delivery_attempt=attempt,
                degraded=claim.degraded,
            )
        )

    # -- helpers ------------------------------------------------------------

    def _default_validate(
        self, payload: dict[str, Any], attributes: Mapping[str, str]
    ) -> BaseModel:
        """Re-validate on receipt against the schema named by the message.

        Producer-side validation is not enough. Replay tooling, an older
        deploy, or a second producer can all put a shape on the topic that this
        consumer was never built for, and the failure mode of finding out
        mid-processing is a partial write.
        """
        schema_name = resolve_schema_name(attributes, payload)
        model = SCHEMA_REGISTRY[schema_name]

        # Envelope fields added by the producer are metadata, not part of the
        # clinical contract; the models are `extra="forbid"` so they must be
        # separated out before validation rather than widening every model.
        body = {k: v for k, v in payload.items() if k not in _ENVELOPE_FIELDS}

        try:
            return model.model_validate(body)
        except ValidationError as exc:
            raise SchemaValidationError(
                f"payload does not satisfy schema {schema_name!r}",
                field_errors=safe_validation_errors(exc),
                schema=schema_name,
            ) from exc

    @staticmethod
    def _to_payload(validated: BaseModel, original: dict[str, Any]) -> dict[str, Any]:
        """Hand the processor validated fields plus the original envelope."""
        merged = dict(original)
        merged.update(validated.model_dump(mode="json"))
        return merged

    @staticmethod
    def _delivery_attempt(message: SupportsAck) -> int | None:
        """Read Pub/Sub's redelivery counter.

        Only populated when the subscription has a dead-letter policy attached;
        ``None`` elsewhere, in which case the budget cannot be enforced and the
        handler retries indefinitely (matching Pub/Sub's own semantics).
        """
        attempt = getattr(message, "delivery_attempt", None)
        return attempt if isinstance(attempt, int) else None

    def _retry_or_exhaust(
        self,
        message: SupportsAck,
        *,
        raw: bytes,
        attributes: Mapping[str, str],
        error: PipelineError,
        event_id: str | None,
        schema: str | None,
        delivery_attempt: int | None,
    ) -> HandlerResult:
        """Nack a transient failure, unless the delivery budget is spent."""
        budget = self._settings.pubsub.max_delivery_attempts

        if delivery_attempt is not None and delivery_attempt >= budget:
            exhausted = RetryBudgetExhaustedError(
                f"gave up after {delivery_attempt} delivery attempts",
                context={"attempts": delivery_attempt, "budget": budget, "cause": error.code},
            )
            return self._quarantine_and_ack(
                message,
                raw=raw,
                attributes=attributes,
                error=exhausted,
                event_id=event_id,
                schema=schema,
                delivery_attempt=delivery_attempt,
                outcome=Outcome.QUARANTINED_EXHAUSTED,
            )

        logger.warning(
            "transient_failure_nacked event_id=%s code=%s attempt=%s/%s",
            event_id,
            error.code,
            delivery_attempt,
            budget,
        )
        message.nack()
        return self._finish(
            HandlerResult(
                Outcome.RETRY,
                acked=False,
                event_id=event_id,
                error_code=error.code,
                delivery_attempt=delivery_attempt,
            )
        )

    def _quarantine_and_ack(
        self,
        message: SupportsAck,
        *,
        raw: bytes,
        attributes: Mapping[str, str],
        error: PipelineError,
        event_id: str | None,
        schema: str | None,
        delivery_attempt: int | None,
        outcome: Outcome,
    ) -> HandlerResult:
        self._quarantine.record(
            reason=outcome.value,
            error_code=error.code,
            error_message=error.message,
            raw=raw,
            event_id=event_id,
            schema=schema,
            attributes=attributes,
            delivery_attempt=delivery_attempt,
            subscription=self._settings.pubsub.subscription_id,
            field_errors=getattr(error, "field_errors", None),
        )
        # Ack *after* the sink call so a crash in between leaves the message on
        # the subscription rather than dropping it with no record anywhere.
        message.ack()
        return self._finish(
            HandlerResult(
                outcome,
                acked=True,
                event_id=event_id,
                error_code=error.code,
                delivery_attempt=delivery_attempt,
            )
        )

    def _finish(self, result: HandlerResult) -> HandlerResult:
        if self._on_result is not None:
            try:
                self._on_result(result)
            except Exception:  # pragma: no cover - metrics must never break flow
                logger.debug("on_result_hook_failed", exc_info=True)
        return result


#: Producer-added envelope keys, stripped before model validation.
_ENVELOPE_FIELDS = frozenset(
    {
        "event_id",
        "received_at",
        "schema",
        "schema_version",
        "consent_reference",
        "deidentified",
        "dicom_bytes",
        "correlation_id",
    }
)
