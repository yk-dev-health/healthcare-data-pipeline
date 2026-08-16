"""Redis-backed idempotency for at-least-once delivery.

Pub/Sub guarantees *at-least-once* delivery. Duplicates are not an edge case;
they are the documented behaviour, and they happen whenever an ack is lost, a
subscriber restarts mid-work, or the ack deadline expires under load. In a
clinical pipeline a duplicate is not merely wasted CPU: it double-writes an
imaging record, double-counts in every downstream aggregate, and emits a second
audit trail entry claiming a second access to the same patient's data.

What was wrong with the original check
--------------------------------------
::

    if already_processed(event_id):    # GET
        message.ack(); return
    process(event)
    mark_processed(event_id)           # SETEX, *after* the work

Three defects:

1. **Check-then-act race.** Two subscribers can both ``GET`` nil for the same
   ``event_id`` and both proceed. The marker is only written after processing,
   so the window is the entire duration of the work — the widest it could be.
2. **Random fallback ID.** A message without an ``event_id`` was assigned
   ``uuid4()``. Every redelivery of that message therefore got a *different*
   key and dedup silently did nothing for exactly the messages most likely to
   be replayed.
3. **Silent degradation.** With Redis down, ``already_processed`` fell back to
   scanning an in-process list, which is empty in a fresh worker. Dedup
   disappeared without any signal.

What this module does instead
-----------------------------
A three-state claim protocol whose decision point is a single atomic
``SET key value NX EX ttl``:

    NEW → ``in-progress:<token>`` (leased) → ``done`` (remembered)

* ``ACQUIRED``  – this worker owns the event; process it.
* ``IN_FLIGHT`` – another worker holds an unexpired lease; nack and let Pub/Sub
  redeliver rather than racing it.
* ``DUPLICATE`` – already completed; ack and drop.

The lease carries a **fencing token**, so a worker that stalled past its lease
expiry cannot later delete or complete a claim that a different worker has
since taken over.

Failure mode is configurable and audited (:class:`common.config.IdempotencySettings`).
The default, ``closed``, refuses to process when the dedup store is unavailable
— consistency over availability, which is the right trade for clinical records
because the queue will hold the work until Redis returns.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from common.config import IdempotencySettings, get_settings
from common.errors import IdempotencyBackendError, RedisUnavailableError
from common.redis_support import ResilientRedisClient, get_redis_client

__all__ = ["ClaimState", "Claim", "IdempotencyStore", "derive_event_id"]

logger = logging.getLogger(__name__)

_STATE_DONE = "done"
_STATE_IN_PROGRESS = "in-progress"


class ClaimState(str, Enum):
    """Outcome of attempting to claim an event for processing."""

    ACQUIRED = "acquired"
    DUPLICATE = "duplicate"
    IN_FLIGHT = "in_flight"


@dataclass(frozen=True)
class Claim:
    """Result of :meth:`IdempotencyStore.claim`.

    Attributes:
        key: Fully namespaced Redis key.
        event_id: The logical identity being deduplicated.
        state: See :class:`ClaimState`.
        token: Fencing token proving ownership of the lease. Only set when
            ``state`` is :attr:`ClaimState.ACQUIRED`.
        degraded: True when the claim was granted without a working backend
            because the fail mode is ``open``. Duplicate processing is possible
            and the caller should surface this in the audit trail.
    """

    key: str
    event_id: str
    state: ClaimState
    token: str | None = None
    degraded: bool = False

    @property
    def acquired(self) -> bool:
        return self.state is ClaimState.ACQUIRED


def derive_event_id(payload: Mapping[str, Any] | None, raw: bytes | None = None) -> str:
    """Return a *stable* identity for a message.

    Order of preference:

    1. An explicit ``event_id`` from the payload — set by the producer and
       carried through redeliveries.
    2. A content hash of the raw body.
    3. A content hash of the canonicalised payload.

    Never a random UUID. A random fallback makes every redelivery look like a
    new event, which is worse than having no dedup at all because it looks like
    dedup is working.
    """
    if payload:
        explicit = payload.get("event_id")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip()

    if raw is not None:
        return "sha256:" + hashlib.sha256(raw).hexdigest()

    canonical = json.dumps(payload or {}, sort_keys=True, default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


class IdempotencyStore:
    """Atomic claim/commit/release over Redis."""

    def __init__(
        self,
        client: ResilientRedisClient | None = None,
        settings: IdempotencySettings | None = None,
    ) -> None:
        self._client = client or get_redis_client()
        self._settings = settings or get_settings().idempotency

    # -- keys ---------------------------------------------------------------

    def key_for(self, event_id: str) -> str:
        """Namespaced, versioned key.

        The original code used the bare ``event_id`` as a top-level Redis key,
        which shares a keyspace with every other user of that database and
        makes a ``FLUSHDB`` or a key-pattern migration unreviewable. The ``v1``
        segment lets the claim format change without colliding with in-flight
        keys written by the previous release.
        """
        return f"idemp:v1:{self._settings.namespace}:{event_id}"

    # -- protocol -----------------------------------------------------------

    def claim(self, event_id: str) -> Claim:
        """Attempt to take exclusive ownership of ``event_id``.

        Raises:
            IdempotencyBackendError: Redis is unavailable and the configured
                fail mode is ``closed``. Transient — the caller should nack.
        """
        key = self.key_for(event_id)
        token = uuid.uuid4().hex
        lease_value = f"{_STATE_IN_PROGRESS}:{token}"

        try:
            # The single atomic decision point. Exactly one caller can observe
            # a true return for a given key while it is unset.
            if self._client.set(key, lease_value, nx=True, ex=self._settings.lease_ttl_seconds):
                return Claim(key=key, event_id=event_id, state=ClaimState.ACQUIRED, token=token)

            current = self._client.get(key)
            if current is None:
                # The key expired between our SET NX and our GET. One retry is
                # enough: if it races again another worker is actively holding
                # it, which IN_FLIGHT already describes correctly.
                if self._client.set(
                    key, lease_value, nx=True, ex=self._settings.lease_ttl_seconds
                ):
                    return Claim(
                        key=key, event_id=event_id, state=ClaimState.ACQUIRED, token=token
                    )
                return Claim(key=key, event_id=event_id, state=ClaimState.IN_FLIGHT)

            state = self._decode(current)
            if state == _STATE_DONE:
                return Claim(key=key, event_id=event_id, state=ClaimState.DUPLICATE)
            return Claim(key=key, event_id=event_id, state=ClaimState.IN_FLIGHT)

        except RedisUnavailableError as exc:
            return self._handle_backend_failure(key, event_id, token, exc)

    def commit(self, claim: Claim) -> bool:
        """Record ``event_id`` as permanently processed.

        Returns ``False`` (rather than raising) when the marker could not be
        written: the clinical work has already succeeded at this point, so
        failing the message would be strictly worse than accepting the small
        risk of one reprocess on redelivery. The failure is logged at WARNING
        because it is a real, if bounded, duplicate-risk event.
        """
        if claim.degraded:
            return False
        try:
            if not self._owns(claim):
                logger.warning(
                    "idempotency_commit_skipped reason=lease_lost event_id=%s", claim.event_id
                )
                return False
            self._client.setex(claim.key, self._settings.completed_ttl_seconds, _STATE_DONE)
            return True
        except RedisUnavailableError:
            logger.warning(
                "idempotency_commit_failed event_id=%s reason=backend_unavailable; "
                "a redelivery of this event may be reprocessed",
                claim.event_id,
            )
            return False

    def release(self, claim: Claim) -> bool:
        """Drop an unfinished lease so a redelivery can retry immediately.

        Called when processing failed transiently. Without this the message
        would come back and hit its own stale ``IN_FLIGHT`` lease, wasting
        every redelivery until the lease TTL expired.
        """
        if claim.degraded:
            return False
        try:
            if not self._owns(claim):
                # Our lease expired and someone else owns the key now. Deleting
                # it would strip a live worker of its claim — this is exactly
                # what the fencing token exists to prevent.
                return False
            self._client.delete(claim.key)
            return True
        except RedisUnavailableError:
            logger.warning("idempotency_release_failed event_id=%s", claim.event_id)
            return False

    def is_processed(self, event_id: str) -> bool:
        """Read-only check. Returns ``False`` when the backend is unavailable."""
        try:
            value = self._client.get(self.key_for(event_id))
        except RedisUnavailableError:
            return False
        return value is not None and self._decode(value) == _STATE_DONE

    # -- internals ----------------------------------------------------------

    def _owns(self, claim: Claim) -> bool:
        """Verify the stored lease still carries our fencing token."""
        if claim.token is None:
            return False
        current = self._client.get(claim.key)
        if current is None:
            return False
        return self._decode(current) == f"{_STATE_IN_PROGRESS}:{claim.token}"

    @staticmethod
    def _decode(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    def _handle_backend_failure(
        self, key: str, event_id: str, token: str, exc: RedisUnavailableError
    ) -> Claim:
        if self._settings.fail_mode == "closed":
            # Stop consuming. The event stays on the subscription and will be
            # redelivered once Redis recovers: no duplicate, no loss.
            raise IdempotencyBackendError(
                "idempotency backend unavailable; refusing to process without "
                "duplicate protection (fail_mode=closed)",
                context={"event_id": event_id, "cause": exc.code},
            ) from exc

        logger.error(
            "idempotency_degraded event_id=%s fail_mode=open; processing without "
            "duplicate protection, duplicate clinical records are possible",
            event_id,
        )
        return Claim(
            key=key,
            event_id=event_id,
            state=ClaimState.ACQUIRED,
            token=token,
            degraded=True,
        )
