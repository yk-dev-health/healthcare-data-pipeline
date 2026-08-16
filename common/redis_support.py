"""A Redis client that degrades predictably instead of hanging.

The previous implementation did this at module import time::

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    r.ping()

Three problems, all of which show up only when Redis is *down* — i.e. exactly
when you need the system to behave:

1. **No socket timeout.** With nothing listening on ``localhost:6379`` this
   call blocked for ~50 s on Windows (OS-level SYN retries). Two modules did
   it, so importing the worker took over a minute. In Kubernetes that reads as
   a failed startup probe with no useful signal.
2. **Connect once, at import.** If Redis was unavailable during startup the
   client was set to ``None`` permanently. Redis coming back never healed the
   process; only a restart did.
3. **No backpressure on failure.** Once Redis is down, every message pays the
   full connect timeout again. Throughput collapses to 1/timeout per worker
   rather than failing fast and letting the queue redeliver.

:class:`ResilientRedisClient` fixes all three: lazy connect, explicit timeouts,
and a cooldown window (a simple circuit breaker) so a sustained outage costs a
constant per-message price rather than a timeout each.

Every failure surfaces as :class:`common.errors.RedisUnavailableError`, a
*transient* error, so callers never have to know about ``redis.RedisError``
subclasses to make the right ack/nack decision.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, TypeVar

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from common.config import RedisSettings, get_settings
from common.errors import RedisUnavailableError

__all__ = ["ResilientRedisClient", "get_redis_client", "reset_redis_client"]

logger = logging.getLogger(__name__)

T = TypeVar("T")

#: Errors that mean "the backend is unreachable or misbehaving", as opposed to
#: a programming error such as passing a bad argument type.
_TRANSPORT_ERRORS = (
    redis.ConnectionError,
    redis.TimeoutError,
    redis.BusyLoadingError,
    redis.ResponseError,
)


class ResilientRedisClient:
    """Lazily-connected Redis wrapper with a fail-fast cooldown.

    Args:
        settings: Host/port/timeout configuration.
        client_factory: Injection point for tests. Called with no arguments and
            must return an object implementing the subset of the redis-py API
            used here.
        clock: Monotonic time source, injected so cooldown behaviour can be
            tested without sleeping.
    """

    def __init__(
        self,
        settings: RedisSettings | None = None,
        *,
        client_factory: Callable[[], Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._settings = settings or get_settings().redis
        self._client_factory = client_factory or self._default_factory
        self._clock = clock
        self._client: Any | None = None
        self._blocked_until: float = 0.0
        self._consecutive_failures = 0

    # -- connection management ---------------------------------------------

    def _default_factory(self) -> Any:
        s = self._settings
        common_kwargs: dict[str, Any] = {
            # The two settings whose absence caused the original 50 s stall.
            "socket_connect_timeout": s.connect_timeout,
            "socket_timeout": s.socket_timeout,
            # We implement our own retry policy (cooldown here, redelivery in
            # Pub/Sub). redis-py 5+ retries connection errors three times with
            # exponential backoff by default, which would silently multiply
            # the worst-case latency of every command by ~8x on top of the
            # socket timeouts above.
            "retry": Retry(NoBackoff(), retries=0),
            "decode_responses": False,
        }
        if s.url:
            return redis.Redis.from_url(s.url, **common_kwargs)
        return redis.Redis(host=s.host, port=s.port, db=s.db, **common_kwargs)

    @property
    def circuit_open(self) -> bool:
        """True while the client is refusing to dial after a recent failure."""
        return self._clock() < self._blocked_until

    def _trip(self, exc: BaseException) -> None:
        self._consecutive_failures += 1
        self._blocked_until = self._clock() + self._settings.reconnect_cooldown
        self._client = None
        logger.warning(
            "redis_unavailable failures=%d cooldown=%.1fs error=%s",
            self._consecutive_failures,
            self._settings.reconnect_cooldown,
            type(exc).__name__,
        )

    def _reset(self) -> None:
        if self._consecutive_failures:
            logger.info("redis_recovered after_failures=%d", self._consecutive_failures)
        self._consecutive_failures = 0
        self._blocked_until = 0.0

    def _connect(self) -> Any:
        if self._client is not None:
            return self._client
        if self.circuit_open:
            raise RedisUnavailableError(
                "redis circuit open; not dialling until cooldown elapses",
                context={"cooldown_remaining_s": round(self._blocked_until - self._clock(), 3)},
            )
        try:
            self._client = self._client_factory()
        except Exception as exc:  # noqa: BLE001 - factory may raise anything
            self._trip(exc)
            raise RedisUnavailableError(
                "failed to construct redis client", context={"error": type(exc).__name__}
            ) from exc
        return self._client

    # -- command execution --------------------------------------------------

    def call(self, operation: str, fn: Callable[[Any], T]) -> T:
        """Run ``fn`` against a live client, translating transport failures.

        Args:
            operation: Short label used in logs and error context. Must not
                contain patient data.
            fn: Receives the underlying redis-py client.

        Raises:
            RedisUnavailableError: The backend is unreachable, timed out, or
                the circuit is open. Always transient; callers should retry or
                nack rather than discard work.
        """
        client = self._connect()
        try:
            result = fn(client)
        except _TRANSPORT_ERRORS as exc:
            self._trip(exc)
            raise RedisUnavailableError(
                f"redis operation {operation!r} failed",
                context={"operation": operation, "error": type(exc).__name__},
            ) from exc
        self._reset()
        return result

    def healthy(self) -> bool:
        """Cheap readiness probe. Never raises."""
        try:
            return bool(self.call("ping", lambda c: c.ping()))
        except RedisUnavailableError:
            return False

    def close(self) -> None:
        client, self._client = self._client, None
        if client is not None:
            try:
                client.close()
            except Exception:  # pragma: no cover - best-effort teardown
                logger.debug("redis_close_failed", exc_info=True)

    # -- thin passthroughs --------------------------------------------------
    # Only the commands this pipeline actually uses, so the blast radius of a
    # Redis outage stays visible in the type signatures.

    def get(self, key: str) -> bytes | None:
        return self.call("get", lambda c: c.get(key))

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool:
        return bool(self.call("set", lambda c: c.set(key, value, nx=nx, ex=ex)))

    def setex(self, key: str, seconds: int, value: str) -> bool:
        return bool(self.call("setex", lambda c: c.setex(key, seconds, value)))

    def delete(self, *keys: str) -> int:
        return int(self.call("delete", lambda c: c.delete(*keys)))

    def ttl(self, key: str) -> int:
        return int(self.call("ttl", lambda c: c.ttl(key)))

    def lpush(self, key: str, *values: str) -> int:
        return int(self.call("lpush", lambda c: c.lpush(key, *values)))

    def llen(self, key: str) -> int:
        return int(self.call("llen", lambda c: c.llen(key)))


_client: ResilientRedisClient | None = None


def get_redis_client() -> ResilientRedisClient:
    """Process-wide client. Construction performs no I/O."""
    global _client
    if _client is None:
        _client = ResilientRedisClient()
    return _client


def reset_redis_client() -> None:
    """Drop the shared client. Used by tests and by config reloads."""
    global _client
    if _client is not None:
        _client.close()
    _client = None
