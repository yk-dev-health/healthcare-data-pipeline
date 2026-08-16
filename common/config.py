"""Centralised, validated runtime configuration.

Configuration is read from the environment **once**, at the point a component
asks for it, and is then frozen. Two properties matter here:

1. *No import-time I/O.* An earlier version of this pipeline constructed a
   Redis client and a Pub/Sub client at module import. On a host where Redis
   was not listening, importing ``worker.worker`` blocked for ~50 seconds on
   TCP connect retries — which made the test suite look like it had hung and
   would have made a container fail its startup probe for reasons nobody could
   see. Every client in this codebase is now lazily constructed and every
   socket has an explicit timeout.

2. *Fail fast on unsafe production settings.* :meth:`Settings.validate_for_runtime`
   refuses to start with a default pseudonymisation salt outside development,
   because a known salt makes every ``PS_...`` pseudonym trivially reversible
   by dictionary attack over the patient-ID space.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Literal

__all__ = [
    "Settings",
    "RedisSettings",
    "IdempotencySettings",
    "PubSubSettings",
    "QuarantineSettings",
    "get_settings",
    "reset_settings_cache",
    "env_int",
    "env_bool",
    "env_str",
    "env_float",
    "DEFAULT_SALT",
]

DEFAULT_SALT = "default-salt-change-in-production"

FailMode = Literal["closed", "open"]


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    # Tolerate `.env` files that carry trailing inline comments, e.g.
    # `SENSITIVE_DATA_TTL=3600  # 1 hour`, which python-dotenv keeps verbatim.
    raw = raw.split("#", 1)[0].strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    raw = raw.split("#", 1)[0].strip()
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"environment variable {name} must be a number, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.split("#", 1)[0].strip().lower() in {"1", "true", "yes", "on"}


# Public aliases: other modules read a few settings directly rather than
# threading a Settings object through, and should get the same
# comment-tolerant, type-checked parsing.
env_str = _env_str
env_int = _env_int
env_bool = _env_bool
env_float = _env_float


@dataclass(frozen=True)
class RedisSettings:
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    url: str | None = None
    #: Hard cap on TCP connect. Without this, a dead Redis costs ~50s per call
    #: on Windows and ~2 min on some Linux defaults.
    connect_timeout: float = 0.5
    #: Hard cap on a single command round-trip.
    socket_timeout: float = 1.0
    #: After a failure the client stops dialling for this long, so a Redis
    #: outage degrades throughput by a constant instead of by a timeout per
    #: message. This is the circuit-breaker "open" duration.
    reconnect_cooldown: float = 5.0


@dataclass(frozen=True)
class IdempotencySettings:
    namespace: str = "dicom"
    #: How long a worker may hold an unfinished claim before another worker is
    #: allowed to take over. Must exceed the p99 processing time.
    lease_ttl_seconds: int = 300
    #: How long a completed event_id is remembered. Must exceed the maximum
    #: Pub/Sub redelivery window, otherwise a late redelivery reprocesses.
    completed_ttl_seconds: int = 86_400
    #: ``closed`` (default): refuse to process when the dedup store is down,
    #: so a Redis outage can never produce duplicate clinical records.
    #: ``open``: keep processing and accept duplicate risk. Availability over
    #: consistency — a deliberate, audited choice, not an accident.
    fail_mode: FailMode = "closed"


@dataclass(frozen=True)
class PubSubSettings:
    project_id: str = "healthcare-pipeline-yk-01"
    topic_id: str = "healthcare-events"
    subscription_id: str = "healthcare-sub"
    enabled: bool = False
    publish_timeout: float = 30.0
    #: Delivery attempts before a repeatedly-failing message is quarantined.
    #: Mirrors the subscription's dead-letter ``maxDeliveryAttempts``.
    max_delivery_attempts: int = 5
    #: Streaming pull flow control, so one subscriber cannot lease more work
    #: than it can finish inside the ack deadline.
    max_outstanding_messages: int = 100
    max_outstanding_bytes: int = 50 * 1024 * 1024
    #: Pub/Sub rejects anything above 10 MiB; reject locally first so the
    #: failure is a clean permanent error rather than a gRPC surprise.
    max_message_bytes: int = 9 * 1024 * 1024


@dataclass(frozen=True)
class QuarantineSettings:
    path: str = "data/quarantine.jsonl"
    #: Off by design. A quarantine file is an operational artefact with a
    #: different (usually longer, less controlled) lifecycle than the
    #: processing store, so persisting raw clinical payloads there would
    #: quietly create a second uncontrolled copy of PHI. A SHA-256 digest is
    #: enough to correlate with the producer's own logs.
    store_payload: bool = False


@dataclass(frozen=True)
class Settings:
    environment: str = "development"
    patient_hash_salt: str = DEFAULT_SALT
    redis: RedisSettings = field(default_factory=RedisSettings)
    idempotency: IdempotencySettings = field(default_factory=IdempotencySettings)
    pubsub: PubSubSettings = field(default_factory=PubSubSettings)
    quarantine: QuarantineSettings = field(default_factory=QuarantineSettings)

    @property
    def is_production(self) -> bool:
        return self.environment.lower() in {"production", "prod", "live"}

    @classmethod
    def from_env(cls) -> "Settings":
        fail_mode = _env_str("IDEMPOTENCY_FAIL_MODE", "closed").lower()
        if fail_mode not in {"closed", "open"}:
            raise ValueError(
                f"IDEMPOTENCY_FAIL_MODE must be 'closed' or 'open', got {fail_mode!r}"
            )

        # Publishing is opt-in: enable it only when the process has something
        # to publish *to*. This keeps `pytest` and a bare `uvicorn` from
        # reaching for GCP credentials that are not there.
        pubsub_enabled = _env_bool(
            "PUBSUB_ENABLED",
            bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("PUBSUB_EMULATOR_HOST")),
        )

        return cls(
            environment=_env_str("APP_ENV", "development"),
            patient_hash_salt=_env_str("PATIENT_HASH_SALT", DEFAULT_SALT),
            redis=RedisSettings(
                host=_env_str("REDIS_HOST", "localhost"),
                port=_env_int("REDIS_PORT", 6379),
                db=_env_int("REDIS_DB", 0),
                url=os.getenv("REDIS_URL") or None,
                connect_timeout=_env_float("REDIS_CONNECT_TIMEOUT", 0.5),
                socket_timeout=_env_float("REDIS_SOCKET_TIMEOUT", 1.0),
                reconnect_cooldown=_env_float("REDIS_RECONNECT_COOLDOWN", 5.0),
            ),
            idempotency=IdempotencySettings(
                namespace=_env_str("IDEMPOTENCY_NAMESPACE", "dicom"),
                lease_ttl_seconds=_env_int("IDEMPOTENCY_LEASE_TTL", 300),
                completed_ttl_seconds=_env_int("IDEMPOTENCY_COMPLETED_TTL", 86_400),
                fail_mode=fail_mode,  # type: ignore[arg-type]
            ),
            pubsub=PubSubSettings(
                project_id=_env_str("PROJECT_ID", "healthcare-pipeline-yk-01"),
                topic_id=_env_str("TOPIC_ID", "healthcare-events"),
                subscription_id=_env_str("SUBSCRIPTION_ID", "healthcare-sub"),
                enabled=pubsub_enabled,
                publish_timeout=_env_float("PUBSUB_PUBLISH_TIMEOUT", 30.0),
                max_delivery_attempts=_env_int("PUBSUB_MAX_DELIVERY_ATTEMPTS", 5),
                max_outstanding_messages=_env_int("PUBSUB_MAX_OUTSTANDING_MESSAGES", 100),
                max_outstanding_bytes=_env_int(
                    "PUBSUB_MAX_OUTSTANDING_BYTES", 50 * 1024 * 1024
                ),
            ),
            quarantine=QuarantineSettings(
                path=_env_str("QUARANTINE_PATH", "data/quarantine.jsonl"),
                store_payload=_env_bool("QUARANTINE_STORE_PAYLOAD", False),
            ),
        )

    def validate_for_runtime(self) -> list[str]:
        """Return blocking misconfigurations. Empty list means safe to start.

        Callers decide the severity: the API raises on startup, tests assert on
        the contents. Keeping this as a pure function (rather than raising from
        ``from_env``) means configuration can always be *inspected* even when
        it is not safe to *run*.
        """
        problems: list[str] = []

        if self.is_production:
            if self.patient_hash_salt == DEFAULT_SALT:
                problems.append(
                    "PATIENT_HASH_SALT is the built-in default; pseudonyms would be "
                    "reversible by dictionary attack. Set a secret salt (ideally from "
                    "Cloud KMS / Secret Manager)."
                )
            if self.quarantine.store_payload:
                problems.append(
                    "QUARANTINE_STORE_PAYLOAD=true would persist raw clinical payloads "
                    "to the quarantine file, creating an uncontrolled second copy of PHI."
                )
            if self.idempotency.fail_mode == "open":
                problems.append(
                    "IDEMPOTENCY_FAIL_MODE=open permits duplicate processing of clinical "
                    "events during a Redis outage; require an explicit risk acceptance."
                )

        if self.idempotency.lease_ttl_seconds <= 0:
            problems.append("IDEMPOTENCY_LEASE_TTL must be positive.")
        if self.idempotency.completed_ttl_seconds <= self.idempotency.lease_ttl_seconds:
            problems.append(
                "IDEMPOTENCY_COMPLETED_TTL must exceed IDEMPOTENCY_LEASE_TTL, otherwise a "
                "completed event can be forgotten while a redelivery is still in flight."
            )
        if self.pubsub.max_delivery_attempts < 1:
            problems.append("PUBSUB_MAX_DELIVERY_ATTEMPTS must be at least 1.")

        return problems


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton."""
    return Settings.from_env()


def reset_settings_cache() -> None:
    """Drop the cached settings. Used by tests that patch the environment."""
    get_settings.cache_clear()
