"""Pub/Sub receive-path delivery semantics.

Each test pins one row of the ack/nack decision table in
:mod:`worker.message_handler`. The invariant checked everywhere is that a
message is settled **exactly once**: an unsettled message silently holds a
flow-control slot until its ack deadline expires, which presents as a worker
that mysteriously slows down under load.
"""

from __future__ import annotations

import json

import pytest

from common.errors import (
    ConsentMissingError,
    DownstreamUnavailableError,
    PurposeLimitationError,
    TransientError,
)
from common.idempotency import IdempotencyStore
from worker.message_handler import (
    Outcome,
    PubSubMessageHandler,
    decode_message_body,
)


@pytest.fixture
def processed():
    """Collects every payload the processor was handed."""
    return []


@pytest.fixture
def handler(idempotency, quarantine, settings, processed):
    def processor(event):
        processed.append(event)
        return {"ok": True}

    return PubSubMessageHandler(
        processor=processor,
        idempotency=idempotency,
        quarantine=quarantine,
        settings=settings,
    )


def make_handler(idempotency, quarantine, settings, processor):
    return PubSubMessageHandler(
        processor=processor,
        idempotency=idempotency,
        quarantine=quarantine,
        settings=settings,
    )


def quarantine_records(quarantine):
    import os

    if not os.path.exists(quarantine.path):
        return []
    with open(quarantine.path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


# --------------------------------------------------------------------------
# Decoding
# --------------------------------------------------------------------------


class TestDecoding:
    def test_valid_json_object_decodes(self):
        assert decode_message_body(b'{"modality":"CT"}') == {"modality": "CT"}

    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(b"\xff\xfe not utf-8", id="invalid_utf8"),
            pytest.param(b"{not json", id="malformed_json"),
            pytest.param(b"[1, 2, 3]", id="json_array_not_object"),
            pytest.param(b'"a bare string"', id="json_scalar"),
        ],
    )
    def test_undecodable_bodies_are_permanent(self, raw):
        from common.errors import MessageDecodeError

        with pytest.raises(MessageDecodeError) as excinfo:
            decode_message_body(raw)

        assert excinfo.value.retryable is False

    def test_decode_error_context_carries_no_payload_content(self):
        """A malformed body is often truncated PHI; the diagnostics must be metadata only."""
        from common.errors import MessageDecodeError

        with pytest.raises(MessageDecodeError) as excinfo:
            decode_message_body(b'{"patient_name": "Jane Doe", broken')

        rendered = json.dumps(excinfo.value.to_dict())
        assert "Jane Doe" not in rendered
        assert "patient_name" not in rendered


# --------------------------------------------------------------------------
# Poison messages -> ack + quarantine
# --------------------------------------------------------------------------


class TestPoisonMessages:
    def test_malformed_json_is_acked_and_quarantined(
        self, handler, quarantine, make_message
    ):
        """Nacking this forever would pin a subscriber on a message that can never parse."""
        message = make_message(b"{not json at all}")

        result = handler.handle(message)

        assert result.outcome is Outcome.QUARANTINED_INVALID
        assert message.ack_calls == 1 and message.nack_calls == 0
        assert len(quarantine_records(quarantine)) == 1

    def test_schema_violation_is_acked_and_quarantined(
        self, handler, quarantine, make_message, dicom_payload
    ):
        dicom_payload["modality"] = "PET"  # not in the accepted Literal

        result = handler.handle(make_message(dicom_payload))

        assert result.outcome is Outcome.QUARANTINED_INVALID
        assert result.error_code == "schema_validation_failed"

        record = quarantine_records(quarantine)[0]
        assert [e["field"] for e in record["field_errors"]] == ["modality"]

    def test_quarantine_record_holds_a_digest_not_the_payload(
        self, handler, quarantine, make_message, dicom_payload
    ):
        """The whole point of the sink: diagnose the failure, do not copy the PHI."""
        dicom_payload["modality"] = "PET"
        raw = json.dumps(dicom_payload).encode("utf-8")

        handler.handle(make_message(raw))

        record = quarantine_records(quarantine)[0]
        blob = json.dumps(record)
        assert "Jane Doe" not in blob
        assert "1985-05-17" not in blob
        assert len(record["payload_sha256"]) == 64
        assert record["payload_bytes"] == len(raw)
        assert "payload" not in record

    def test_unknown_extra_field_is_rejected(self, handler, make_message, dicom_payload):
        """`extra="forbid"` turns an upstream typo into a loud rejection."""
        dicom_payload["patient_ID"] = "P123"

        result = handler.handle(make_message(dicom_payload))

        assert result.outcome is Outcome.QUARANTINED_INVALID

    def test_business_rule_rejection_is_permanent(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        """Purpose-limitation breaches are quarantined, not retried: no retry makes them lawful."""

        def processor(_event):
            raise PurposeLimitationError("purpose not approved")

        handler = make_handler(idempotency, quarantine, settings, processor)

        result = handler.handle(make_message(dicom_payload))

        assert result.outcome is Outcome.QUARANTINED_INVALID
        assert result.error_code == "purpose_limitation_violated"

    def test_permanent_business_failure_releases_the_claim(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        """The event must not be left marked as in-flight after a permanent rejection."""

        def processor(_event):
            raise ConsentMissingError("no consent")

        handler = make_handler(idempotency, quarantine, settings, processor)
        handler.handle(make_message(dicom_payload))

        from common.idempotency import derive_event_id

        event_id = derive_event_id(dicom_payload, json.dumps(dicom_payload).encode())
        assert idempotency.claim(event_id).acquired


# --------------------------------------------------------------------------
# Duplicates
# --------------------------------------------------------------------------


class TestDuplicates:
    def test_redelivery_is_acked_without_reprocessing(
        self, handler, make_message, dicom_payload, processed
    ):
        first = make_message(dicom_payload)
        second = make_message(dicom_payload)

        assert handler.handle(first).outcome is Outcome.PROCESSED
        assert handler.handle(second).outcome is Outcome.DUPLICATE

        assert len(processed) == 1, "the duplicate must not reach the processor"
        assert second.ack_calls == 1

    def test_duplicate_detection_survives_a_missing_event_id(
        self, handler, make_message, dicom_payload, processed
    ):
        """Regression: the old uuid4() fallback broke dedup for id-less messages."""
        dicom_payload.pop("event_id", None)

        handler.handle(make_message(dicom_payload))
        handler.handle(make_message(dicom_payload))

        assert len(processed) == 1

    def test_in_flight_event_is_nacked_not_dropped(
        self, handler, idempotency, make_message, dicom_payload
    ):
        """Another worker holds the lease. Nack so the work is not lost if it dies."""
        from common.idempotency import derive_event_id

        raw = json.dumps(dicom_payload).encode()
        idempotency.claim(derive_event_id(dicom_payload, raw))

        message = make_message(raw)
        result = handler.handle(message)

        assert result.outcome is Outcome.IN_FLIGHT
        assert message.nack_calls == 1 and message.ack_calls == 0


# --------------------------------------------------------------------------
# Transient failures and the retry budget
# --------------------------------------------------------------------------


class TestTransientFailures:
    def test_transient_processing_failure_is_nacked(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        def processor(_event):
            raise DownstreamUnavailableError("bigquery timeout")

        handler = make_handler(idempotency, quarantine, settings, processor)
        message = make_message(dicom_payload, delivery_attempt=1)

        result = handler.handle(message)

        assert result.outcome is Outcome.RETRY
        assert message.nack_calls == 1
        assert quarantine_records(quarantine) == []

    def test_transient_failure_releases_the_claim_for_the_retry(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        """Without release, the redelivery hits its own stale lease and wastes every attempt."""
        attempts = []

        def processor(event):
            attempts.append(event)
            if len(attempts) == 1:
                raise DownstreamUnavailableError("first attempt fails")
            return {"ok": True}

        handler = make_handler(idempotency, quarantine, settings, processor)

        assert handler.handle(make_message(dicom_payload, delivery_attempt=1)).outcome is Outcome.RETRY
        assert handler.handle(make_message(dicom_payload, delivery_attempt=2)).outcome is Outcome.PROCESSED
        assert len(attempts) == 2

    def test_budget_exhaustion_quarantines_instead_of_looping(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        def processor(_event):
            raise DownstreamUnavailableError("still down")

        handler = make_handler(idempotency, quarantine, settings, processor)
        message = make_message(
            dicom_payload, delivery_attempt=settings.pubsub.max_delivery_attempts
        )

        result = handler.handle(message)

        assert result.outcome is Outcome.QUARANTINED_EXHAUSTED
        assert message.ack_calls == 1, "the poison loop must stop"
        record = quarantine_records(quarantine)[0]
        assert record["error_code"] == "retry_budget_exhausted"
        assert record["delivery_attempt"] == settings.pubsub.max_delivery_attempts

    def test_missing_delivery_attempt_retries_indefinitely(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        """`delivery_attempt` is None without a dead-letter policy; match Pub/Sub's own semantics."""

        def processor(_event):
            raise DownstreamUnavailableError("down")

        handler = make_handler(idempotency, quarantine, settings, processor)
        message = make_message(dicom_payload, delivery_attempt=None)

        assert handler.handle(message).outcome is Outcome.RETRY
        assert quarantine_records(quarantine) == []

    def test_unclassified_exception_is_retried_not_discarded(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        """Bias towards keeping clinical data when we have not classified the failure."""

        def processor(_event):
            raise KeyError("some_unexpected_key")

        handler = make_handler(idempotency, quarantine, settings, processor)
        message = make_message(dicom_payload, delivery_attempt=1)

        result = handler.handle(message)

        assert result.outcome is Outcome.RETRY
        assert result.error_code == "unclassified_error"
        assert message.nack_calls == 1

    def test_dedup_backend_outage_nacks_and_does_not_process(
        self, handler, fake_redis, make_message, dicom_payload, processed
    ):
        """fail_mode=closed: never process a clinical event without duplicate protection."""
        fake_redis.set_failing(True)
        message = make_message(dicom_payload, delivery_attempt=1)

        result = handler.handle(message)

        assert result.outcome is Outcome.RETRY
        assert result.error_code == "idempotency_backend_unavailable"
        assert message.nack_calls == 1
        assert processed == []


# --------------------------------------------------------------------------
# Happy path and cross-cutting invariants
# --------------------------------------------------------------------------


class TestSuccessPath:
    def test_valid_message_is_processed_and_acked(
        self, handler, make_message, dicom_payload, processed
    ):
        message = make_message(dicom_payload)

        result = handler.handle(message)

        assert result.outcome is Outcome.PROCESSED
        assert message.ack_calls == 1
        assert processed[0]["study_uid"] == dicom_payload["study_uid"]

    def test_commit_happens_before_ack(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        """Ordering matters: a crash between the two must yield a duplicate, not a loss."""
        observed = {}

        def processor(event):
            return event

        handler = make_handler(idempotency, quarantine, settings, processor)
        message = make_message(dicom_payload)

        original_ack = message.ack

        def ack_and_observe():
            from common.idempotency import derive_event_id

            observed["committed_before_ack"] = idempotency.is_processed(
                derive_event_id(dicom_payload, message.data)
            )
            original_ack()

        message.ack = ack_and_observe
        handler.handle(message)

        assert observed["committed_before_ack"] is True

    def test_processor_receives_validated_and_envelope_fields(
        self, handler, make_message, dicom_payload, processed
    ):
        dicom_payload["event_id"] = "evt-abc"
        dicom_payload["consent_reference"] = "consent-123"

        handler.handle(make_message(dicom_payload))

        event = processed[0]
        assert event["event_id"] == "evt-abc"
        assert event["consent_reference"] == "consent-123"
        assert event["modality"] == "CT"

    def test_clinical_event_schema_is_selected_by_attribute(
        self, idempotency, quarantine, settings, make_message
    ):
        seen = []
        handler = make_handler(idempotency, quarantine, settings, seen.append)
        payload = {
            "patient_id": "P123",
            "modality": "CT",
            "study_date": "2026-01-01",
            "slice_thickness": 1.2,
            "device_id": "CT_001",
        }

        result = handler.handle(
            make_message(payload, attributes={"schema": "clinical_event"})
        )

        assert result.outcome is Outcome.PROCESSED
        assert seen[0]["patient_id"] == "P123"


class TestSettlementInvariant:
    @pytest.mark.parametrize(
        "payload,attempt",
        [
            pytest.param(b"{broken", None, id="poison"),
            pytest.param({"modality": "PET"}, None, id="invalid_schema"),
            pytest.param(None, 1, id="valid"),
        ],
    )
    def test_every_delivery_is_settled_exactly_once(
        self, handler, make_message, dicom_payload, payload, attempt
    ):
        body = dicom_payload if payload is None else payload
        message = make_message(body, delivery_attempt=attempt)

        handler.handle(message)

        assert message.settled_once, "message must be acked or nacked exactly once"

    def test_on_result_hook_failure_does_not_break_the_flow(
        self, idempotency, quarantine, settings, make_message, dicom_payload
    ):
        """Metrics are never allowed to take down message processing."""

        def exploding_hook(_result):
            raise RuntimeError("metrics backend down")

        handler = PubSubMessageHandler(
            processor=lambda e: e,
            idempotency=idempotency,
            quarantine=quarantine,
            settings=settings,
            on_result=exploding_hook,
        )
        message = make_message(dicom_payload)

        assert handler.handle(message).outcome is Outcome.PROCESSED
        assert message.ack_calls == 1
