"""Shared fixtures and test doubles.

Design choices worth stating, because they are what make this suite usable in
CI:

* **No live Redis, no live Pub/Sub, no GCP credentials.** Every dependency is
  injected, so the whole suite runs offline and deterministically. The delivery
  semantics being tested here — duplicate suppression, lease contention, retry
  budgets — are precisely the ones that are impossible to trigger reliably
  against a real broker.
* **A hand-written Redis double instead of ``fakeredis``.** It is ~80 lines, it
  keeps the suite dependency-free, and critically it can *fail on demand*:
  :meth:`FakeRedis.set_failing` simulates an outage mid-test, which is how the
  fail-closed idempotency path gets covered.
* **A controllable clock.** Lease expiry and circuit-breaker cooldowns are
  time-dependent. Injecting the clock tests the real behaviour in
  microseconds instead of approximating it with ``sleep``.
"""

from __future__ import annotations

import os
from typing import Any, Iterator

import pytest

# Pin the environment before anything imports application modules, so the
# settings singleton is built from known values rather than the developer's
# shell or a stray .env.
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("PUBSUB_ENABLED", "false")
os.environ.setdefault("PATIENT_HASH_SALT", "unit-test-salt")
os.environ.setdefault("REDIS_CONNECT_TIMEOUT", "0.05")
os.environ.setdefault("REDIS_SOCKET_TIMEOUT", "0.05")
# The ingestion tests deliberately exercise the "Celery broker is unreachable"
# fallback path rather than stubbing it out, so the connect timeout is turned
# right down. With Celery's default it was ~107 s per test.
os.environ.setdefault("CELERY_BROKER_CONNECT_TIMEOUT", "0.1")

import common.redis_support as redis_support  # noqa: E402
from common.config import (  # noqa: E402
    IdempotencySettings,
    PubSubSettings,
    QuarantineSettings,
    RedisSettings,
    Settings,
    get_settings,
    reset_settings_cache,
)
from common.idempotency import IdempotencyStore  # noqa: E402
from common.quarantine import QuarantineSink  # noqa: E402
from common.redis_support import ResilientRedisClient  # noqa: E402


# --------------------------------------------------------------------------
# Clock
# --------------------------------------------------------------------------


class FakeClock:
    """Monotonic clock under test control."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    """The single clock the whole suite shares.

    One clock, not one per fixture: the process-wide Redis double and the
    per-test client must agree on what "now" is, or TTL expiry tests silently
    measure nothing. It is rewound between tests by `_isolate_global_state`.
    """
    return _SESSION_CLOCK


# --------------------------------------------------------------------------
# Redis double
# --------------------------------------------------------------------------


class FakeRedisError(Exception):
    """Stand-in for ``redis.ConnectionError``."""


class FakeRedis:
    """In-memory Redis implementing only the commands this pipeline uses.

    TTLs are evaluated against the injected clock, so expiry can be tested
    without waiting. ``set_failing(True)`` makes every command raise, which is
    how a mid-test Redis outage is simulated.
    """

    def __init__(self, clock: FakeClock | None = None) -> None:
        self._clock = clock or FakeClock()
        self._data: dict[str, tuple[bytes, float | None]] = {}
        self._lists: dict[str, list[bytes]] = {}
        self.failing = False
        self.calls: list[str] = []

    # -- fault injection ---------------------------------------------------

    def set_failing(self, failing: bool = True) -> None:
        self.failing = failing

    def _guard(self, op: str) -> None:
        self.calls.append(op)
        if self.failing:
            raise FakeRedisError(f"simulated redis outage during {op}")

    # -- expiry ------------------------------------------------------------

    def _live(self, key: str) -> tuple[bytes, float | None] | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        _, expires_at = entry
        if expires_at is not None and self._clock() >= expires_at:
            del self._data[key]
            return None
        return entry

    @staticmethod
    def _encode(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")

    # -- commands ----------------------------------------------------------

    def ping(self) -> bool:
        self._guard("ping")
        return True

    def get(self, key: str) -> bytes | None:
        self._guard("get")
        entry = self._live(key)
        return None if entry is None else entry[0]

    def set(self, key: str, value: Any, nx: bool = False, ex: int | None = None) -> bool | None:
        self._guard("set")
        if nx and self._live(key) is not None:
            return None  # redis-py returns None when NX is not satisfied
        expires_at = None if ex is None else self._clock() + ex
        self._data[key] = (self._encode(value), expires_at)
        return True

    def setex(self, key: str, seconds: int, value: Any) -> bool:
        self._guard("setex")
        if seconds <= 0:
            raise FakeRedisError("invalid expire time")
        self._data[key] = (self._encode(value), self._clock() + seconds)
        return True

    def delete(self, *keys: str) -> int:
        self._guard("delete")
        return sum(1 for key in keys if self._data.pop(key, None) is not None)

    def ttl(self, key: str) -> int:
        self._guard("ttl")
        entry = self._live(key)
        if entry is None:
            return -2
        if entry[1] is None:
            return -1
        return int(entry[1] - self._clock())

    def lpush(self, key: str, *values: Any) -> int:
        self._guard("lpush")
        bucket = self._lists.setdefault(key, [])
        for value in values:
            bucket.insert(0, self._encode(value))
        return len(bucket)

    def llen(self, key: str) -> int:
        self._guard("llen")
        return len(self._lists.get(key, []))

    def close(self) -> None:
        pass


# --------------------------------------------------------------------------
# Process-wide offline Redis
# --------------------------------------------------------------------------
#
# `worker.worker` and `worker.data_minimization` resolve `get_redis_client()`
# at import time, so the substitution has to happen here at conftest import,
# before any test module pulls those in. Without it the suite dials a real
# localhost:6379 on every test that touches the audit or TTL store, which on a
# machine with no Redis costs a connect timeout per call.

_SESSION_CLOCK = FakeClock()
_SESSION_REDIS = FakeRedis(_SESSION_CLOCK)

# Teach the wrapper that the double's exception means "transport failure", so
# `set_failing()` exercises the same code path a real outage would.
redis_support._TRANSPORT_ERRORS = redis_support._TRANSPORT_ERRORS + (FakeRedisError,)
redis_support._client = ResilientRedisClient(
    get_settings().redis,
    client_factory=lambda: _SESSION_REDIS,
    clock=_SESSION_CLOCK,
)


@pytest.fixture
def fake_redis() -> FakeRedis:
    """The process-wide double.

    Shared with the module-level clients in `worker.*` so that a test can make
    Redis "go down" for code it does not construct itself.
    """
    return _SESSION_REDIS


@pytest.fixture
def redis_client(fake_redis: FakeRedis, clock: FakeClock, settings: Settings):
    """A :class:`ResilientRedisClient` over the shared in-memory double."""
    return ResilientRedisClient(
        settings.redis, client_factory=lambda: fake_redis, clock=clock
    )


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Deterministic settings with short TTLs and an isolated quarantine file."""
    return Settings(
        environment="test",
        patient_hash_salt="unit-test-salt",
        redis=RedisSettings(connect_timeout=0.05, socket_timeout=0.05, reconnect_cooldown=5.0),
        idempotency=IdempotencySettings(
            namespace="test",
            lease_ttl_seconds=30,
            completed_ttl_seconds=300,
            fail_mode="closed",
        ),
        pubsub=PubSubSettings(
            project_id="test-project",
            topic_id="test-topic",
            subscription_id="test-sub",
            enabled=False,
            max_delivery_attempts=3,
        ),
        quarantine=QuarantineSettings(path=str(tmp_path / "quarantine.jsonl")),
    )


@pytest.fixture
def idempotency(redis_client, settings: Settings) -> IdempotencyStore:
    return IdempotencyStore(redis_client, settings.idempotency)


@pytest.fixture
def quarantine(settings: Settings) -> QuarantineSink:
    return QuarantineSink(settings.quarantine)


# --------------------------------------------------------------------------
# Pub/Sub message double
# --------------------------------------------------------------------------


class FakeMessage:
    """Implements the slice of ``pubsub_v1...Message`` the handler touches.

    Records ack/nack calls so tests can assert the message was settled exactly
    once — an unsettled message is a real production failure mode (it silently
    holds a flow-control slot until the ack deadline expires) and is invisible
    unless a test looks for it.
    """

    def __init__(
        self,
        data: bytes | str,
        *,
        attributes: dict[str, str] | None = None,
        delivery_attempt: int | None = None,
    ) -> None:
        self.data = data.encode("utf-8") if isinstance(data, str) else data
        self.attributes = attributes or {}
        self.delivery_attempt = delivery_attempt
        self.ack_calls = 0
        self.nack_calls = 0

    def ack(self) -> None:
        self.ack_calls += 1

    def nack(self) -> None:
        self.nack_calls += 1

    @property
    def settled_once(self) -> bool:
        return (self.ack_calls + self.nack_calls) == 1


@pytest.fixture
def make_message():
    def _factory(payload: Any, **kwargs: Any) -> FakeMessage:
        import json

        body = payload if isinstance(payload, (bytes, str)) else json.dumps(payload)
        return FakeMessage(body, **kwargs)

    return _factory


# --------------------------------------------------------------------------
# Sample payloads
# --------------------------------------------------------------------------


@pytest.fixture
def dicom_payload() -> dict[str, Any]:
    """A valid, consented DICOM ingestion payload."""
    return {
        "patient_name": "Jane Doe",
        "patient_birth_date": "1985-05-17",
        "study_uid": "1.2.826.0.1.3680043.8.498.123456",
        "modality": "CT",
        "kVp": 120.0,
        "mA": 250.0,
        "consent_logged": True,
        "source": "PACS",
        "purpose": "diagnostic_support",
    }


@pytest.fixture(autouse=True)
def _isolate_global_state(tmp_path, monkeypatch) -> Iterator[None]:
    """Keep tests from writing to the repo's real data/ and logs/ directories.

    Without this the suite appends to the same audit trail and processed-events
    files that a demo run produces, so tests both pollute committed artefacts
    and become order-dependent on each other's output.
    """
    import worker.logger as logger_module
    import worker.worker as worker_module

    monkeypatch.setattr(logger_module, "AUDIT_LOG_DIR", str(tmp_path / "logs"), raising=False)
    os.makedirs(tmp_path / "logs", exist_ok=True)
    monkeypatch.setattr(worker_module, "OUTPUT_PATH", str(tmp_path / "processed.jsonl"))
    monkeypatch.setattr(worker_module, "AUDIT_PATH", str(tmp_path / "audit.jsonl"))

    worker_module.DICOM_INDEX.clear()
    worker_module.CONSENT_LOG.clear()
    worker_module.MEMORY_QUEUE.clear()

    # Rewind the shared clock and flush the shared Redis double so TTL-based
    # tests start from a known point and cannot see each other's keys.
    _SESSION_CLOCK.now = 1_000.0
    _SESSION_REDIS._data.clear()
    _SESSION_REDIS._lists.clear()
    _SESSION_REDIS.calls.clear()
    _SESSION_REDIS.set_failing(False)
    redis_support._client._blocked_until = 0.0
    redis_support._client._consecutive_failures = 0

    yield

    reset_settings_cache()
