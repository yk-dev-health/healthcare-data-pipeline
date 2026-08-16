"""Duplicate suppression under at-least-once delivery.

These tests encode the guarantees the pipeline actually depends on:

* exactly one of N concurrent claimants wins;
* a completed event stays suppressed until its TTL;
* an unfinished lease expires so work is never permanently stranded;
* a worker that lost its lease cannot clobber the new owner's claim;
* a dead dedup backend stops processing rather than silently allowing
  duplicate clinical records.

The last one is the reason this file exists. Duplicate DICOM records are not a
performance problem — they double-count in every downstream aggregate and emit
a second audit entry claiming a second access to a patient's data.
"""

from __future__ import annotations

import pytest

from common.errors import IdempotencyBackendError
from common.idempotency import Claim, ClaimState, IdempotencyStore, derive_event_id


class TestClaimProtocol:
    def test_first_claim_is_acquired(self, idempotency: IdempotencyStore):
        claim = idempotency.claim("evt-1")

        assert claim.state is ClaimState.ACQUIRED
        assert claim.acquired
        assert claim.token, "an acquired claim must carry a fencing token"

    def test_second_claim_while_in_flight_is_not_acquired(self, idempotency: IdempotencyStore):
        first = idempotency.claim("evt-1")
        second = idempotency.claim("evt-1")

        assert first.state is ClaimState.ACQUIRED
        assert second.state is ClaimState.IN_FLIGHT
        assert second.token is None

    def test_claim_after_commit_is_duplicate(self, idempotency: IdempotencyStore):
        claim = idempotency.claim("evt-1")
        assert idempotency.commit(claim) is True

        assert idempotency.claim("evt-1").state is ClaimState.DUPLICATE

    def test_claim_after_release_is_acquirable_again(self, idempotency: IdempotencyStore):
        """A transient failure must not strand the event until the lease expires."""
        first = idempotency.claim("evt-1")
        assert idempotency.release(first) is True

        retry = idempotency.claim("evt-1")
        assert retry.state is ClaimState.ACQUIRED

    def test_exactly_one_of_many_concurrent_claimants_wins(self, idempotency: IdempotencyStore):
        """The property the old GET-then-SETEX check could not provide.

        Interleaving claims for one event id models N subscribers receiving the
        same redelivered message. The atomic ``SET NX`` must admit exactly one.
        """
        claims = [idempotency.claim("evt-hot") for _ in range(10)]

        acquired = [c for c in claims if c.state is ClaimState.ACQUIRED]
        assert len(acquired) == 1
        assert all(c.state is ClaimState.IN_FLIGHT for c in claims if c not in acquired)

    def test_is_processed_reflects_commit(self, idempotency: IdempotencyStore):
        assert idempotency.is_processed("evt-1") is False

        idempotency.commit(idempotency.claim("evt-1"))

        assert idempotency.is_processed("evt-1") is True


class TestLeaseExpiry:
    def test_expired_lease_can_be_reclaimed(self, idempotency, clock, settings):
        """A worker that dies mid-processing must not block the event forever."""
        stranded = idempotency.claim("evt-1")
        assert stranded.acquired

        clock.advance(settings.idempotency.lease_ttl_seconds + 1)

        assert idempotency.claim("evt-1").state is ClaimState.ACQUIRED

    def test_committed_marker_survives_the_lease_ttl(self, idempotency, clock, settings):
        idempotency.commit(idempotency.claim("evt-1"))

        clock.advance(settings.idempotency.lease_ttl_seconds + 1)

        assert idempotency.claim("evt-1").state is ClaimState.DUPLICATE

    def test_committed_marker_expires_after_completed_ttl(self, idempotency, clock, settings):
        idempotency.commit(idempotency.claim("evt-1"))

        clock.advance(settings.idempotency.completed_ttl_seconds + 1)

        # Beyond the retention window the event is forgotten. This is why
        # completed_ttl must exceed the maximum redelivery window; the config
        # validator enforces the relationship.
        assert idempotency.claim("evt-1").state is ClaimState.ACQUIRED


class TestFencingToken:
    def test_stale_owner_cannot_release_the_new_owner_lease(
        self, idempotency, clock, settings
    ):
        """The failure the fencing token exists to prevent.

        Worker A stalls past its lease. Worker B takes over. A wakes up and
        tries to clean up — without a token check it would delete B's live
        claim and let a third worker start the same work concurrently.
        """
        stale = idempotency.claim("evt-1")
        clock.advance(settings.idempotency.lease_ttl_seconds + 1)
        new_owner = idempotency.claim("evt-1")
        assert new_owner.acquired

        assert idempotency.release(stale) is False
        assert idempotency.claim("evt-1").state is ClaimState.IN_FLIGHT

    def test_stale_owner_cannot_commit_over_the_new_owner(
        self, idempotency, clock, settings
    ):
        stale = idempotency.claim("evt-1")
        clock.advance(settings.idempotency.lease_ttl_seconds + 1)
        idempotency.claim("evt-1")

        assert idempotency.commit(stale) is False
        assert idempotency.is_processed("evt-1") is False


class TestBackendFailure:
    def test_fail_closed_refuses_to_process(self, idempotency, fake_redis):
        """Redis down + fail_mode=closed -> transient error, so the caller nacks."""
        fake_redis.set_failing(True)

        with pytest.raises(IdempotencyBackendError) as excinfo:
            idempotency.claim("evt-1")

        assert excinfo.value.retryable is True

    def test_fail_open_processes_but_flags_degradation(
        self, redis_client, fake_redis, settings
    ):
        """The opposite trade: keep serving, but never pretend dedup happened."""
        open_settings = type(settings.idempotency)(
            namespace="test",
            lease_ttl_seconds=30,
            completed_ttl_seconds=300,
            fail_mode="open",
        )
        store = IdempotencyStore(redis_client, open_settings)
        fake_redis.set_failing(True)

        claim = store.claim("evt-1")

        assert claim.state is ClaimState.ACQUIRED
        assert claim.degraded is True, "degraded mode must be visible to the caller"

    def test_degraded_claim_does_not_write_markers(self, redis_client, fake_redis, settings):
        open_settings = type(settings.idempotency)(fail_mode="open")
        store = IdempotencyStore(redis_client, open_settings)
        fake_redis.set_failing(True)
        claim = store.claim("evt-1")

        assert store.commit(claim) is False
        assert store.release(claim) is False

    def test_commit_failure_does_not_raise(self, idempotency, fake_redis):
        """Work already succeeded: losing the marker must not fail the message.

        Failing here would nack an event that was fully processed, guaranteeing
        the duplicate the store exists to prevent.
        """
        claim = idempotency.claim("evt-1")
        fake_redis.set_failing(True)

        assert idempotency.commit(claim) is False

    def test_is_processed_is_conservative_when_backend_is_down(
        self, idempotency, fake_redis
    ):
        idempotency.commit(idempotency.claim("evt-1"))
        fake_redis.set_failing(True)

        # False, not an exception: callers treat "unknown" as "not confirmed
        # processed" and fall through to the claim protocol, which fails closed.
        assert idempotency.is_processed("evt-1") is False


class TestKeyNamespacing:
    def test_keys_are_namespaced_and_versioned(self, idempotency: IdempotencyStore):
        """Bare event ids as top-level keys share a keyspace with everything else."""
        key = idempotency.key_for("evt-1")

        assert key == "idemp:v1:test:evt-1"

    def test_namespaces_do_not_collide(self, redis_client, settings):
        a = IdempotencyStore(redis_client, type(settings.idempotency)(namespace="dicom"))
        b = IdempotencyStore(redis_client, type(settings.idempotency)(namespace="labs"))

        a.commit(a.claim("evt-1"))

        assert b.claim("evt-1").state is ClaimState.ACQUIRED


class TestDeriveEventId:
    def test_explicit_event_id_wins(self):
        assert derive_event_id({"event_id": "evt-42", "modality": "CT"}) == "evt-42"

    def test_identical_bodies_derive_the_same_id(self):
        raw = b'{"modality":"CT"}'

        assert derive_event_id({"modality": "CT"}, raw) == derive_event_id({"modality": "CT"}, raw)

    def test_different_bodies_derive_different_ids(self):
        assert derive_event_id({}, b'{"a":1}') != derive_event_id({}, b'{"a":2}')

    def test_id_is_never_random(self):
        """The original code fell back to uuid4().

        That made every redelivery of an id-less message look like a brand new
        event, so dedup silently did nothing for exactly the messages most
        likely to be replayed. A content hash is stable across redeliveries.
        """
        payload = {"modality": "CT", "study_uid": "1.2.3"}

        first = derive_event_id(payload)
        second = derive_event_id(dict(reversed(list(payload.items()))))

        assert first == second, "key order must not change the derived identity"

    def test_whitespace_only_event_id_falls_back_to_hash(self):
        derived = derive_event_id({"event_id": "   "}, b"body")

        assert derived.startswith("sha256:")
