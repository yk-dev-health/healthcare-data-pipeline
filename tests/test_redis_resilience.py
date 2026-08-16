"""Redis client behaviour when Redis is not there.

The original client called ``.ping()` at import with no socket timeout. On a
host with nothing listening on 6379 that blocked for ~50 seconds, twice (two
modules did it), so importing the worker took over a minute and the test suite
appeared to hang. In a container it presents as a failed startup probe with no
diagnostic.

These tests cover the three properties that fix it: no I/O at construction,
fail fast once a failure is known, and recover without a restart.
"""

from __future__ import annotations

import pytest

from common.errors import RedisUnavailableError
from common.redis_support import ResilientRedisClient
from tests.conftest import FakeRedis, FakeRedisError


@pytest.fixture
def failing_factory():
    """A factory that raises, standing in for an unreachable Redis."""
    calls = {"count": 0}

    def factory():
        calls["count"] += 1
        raise FakeRedisError("connection refused")

    factory.calls = calls  # type: ignore[attr-defined]
    return factory


class TestLazyConnection:
    def test_construction_performs_no_io(self, failing_factory, settings, clock):
        """Constructing the client must never touch the network.

        This is the regression guard for the 50-second import stall.
        """
        ResilientRedisClient(settings.redis, client_factory=failing_factory, clock=clock)

        assert failing_factory.calls["count"] == 0

    def test_first_command_connects(self, fake_redis, settings, clock):
        created = {"count": 0}

        def factory():
            created["count"] += 1
            return fake_redis

        client = ResilientRedisClient(settings.redis, client_factory=factory, clock=clock)
        client.get("k")

        assert created["count"] == 1

    def test_connection_is_reused(self, fake_redis, settings, clock):
        created = {"count": 0}

        def factory():
            created["count"] += 1
            return fake_redis

        client = ResilientRedisClient(settings.redis, client_factory=factory, clock=clock)
        client.get("a")
        client.get("b")

        assert created["count"] == 1


class TestFailureTranslation:
    def test_transport_failure_becomes_a_transient_error(
        self, redis_client, fake_redis
    ):
        """Callers decide ack/nack from the error family, not from redis-py types."""
        fake_redis.set_failing(True)

        with pytest.raises(RedisUnavailableError) as excinfo:
            redis_client.get("k")

        assert excinfo.value.retryable is True

    def test_error_context_names_the_operation_not_the_data(
        self, redis_client, fake_redis
    ):
        fake_redis.set_failing(True)

        with pytest.raises(RedisUnavailableError) as excinfo:
            redis_client.setex("sensitive:patient:evt-1", 60, '{"name":"Jane Doe"}')

        assert excinfo.value.context["operation"] == "setex"
        assert "Jane Doe" not in str(excinfo.value.to_dict())

    def test_healthy_returns_false_instead_of_raising(self, redis_client, fake_redis):
        fake_redis.set_failing(True)

        assert redis_client.healthy() is False


class TestCircuitBreaker:
    def test_circuit_opens_after_a_failure(self, redis_client, fake_redis):
        fake_redis.set_failing(True)
        with pytest.raises(RedisUnavailableError):
            redis_client.get("k")

        assert redis_client.circuit_open is True

    def test_open_circuit_fails_fast_without_dialling(
        self, failing_factory, settings, clock
    ):
        """A sustained outage must cost a constant per call, not a timeout each.

        Without this, throughput during a Redis outage collapses to
        1/connect_timeout per worker while every message waits on TCP.
        """
        client = ResilientRedisClient(
            settings.redis, client_factory=failing_factory, clock=clock
        )

        with pytest.raises(RedisUnavailableError):
            client.get("k")
        attempts_after_first = failing_factory.calls["count"]

        for _ in range(20):
            with pytest.raises(RedisUnavailableError):
                client.get("k")

        assert failing_factory.calls["count"] == attempts_after_first == 1

    def test_circuit_closes_after_the_cooldown(self, failing_factory, settings, clock):
        client = ResilientRedisClient(
            settings.redis, client_factory=failing_factory, clock=clock
        )
        with pytest.raises(RedisUnavailableError):
            client.get("k")

        clock.advance(settings.redis.reconnect_cooldown + 0.1)

        assert client.circuit_open is False
        with pytest.raises(RedisUnavailableError):
            client.get("k")
        assert failing_factory.calls["count"] == 2, "it should dial again after cooldown"

    def test_client_recovers_without_a_restart(self, settings, clock):
        """The original code set the client to None forever on a startup failure.

        Redis coming back never healed the process; only a redeploy did.
        """
        state = {"up": False}
        backing = FakeRedis(clock)

        def factory():
            if not state["up"]:
                raise FakeRedisError("connection refused")
            return backing

        client = ResilientRedisClient(settings.redis, client_factory=factory, clock=clock)
        with pytest.raises(RedisUnavailableError):
            client.get("k")

        state["up"] = True
        clock.advance(settings.redis.reconnect_cooldown + 0.1)

        client.set("k", "v")
        assert client.get("k") == b"v"


class TestSocketTimeoutsAreConfigured:
    def test_default_factory_sets_both_timeouts(self, settings):
        """The two kwargs whose absence caused the 50 s stall."""
        import redis

        client = ResilientRedisClient(settings.redis)
        underlying = client._default_factory()

        assert isinstance(underlying, redis.Redis)
        kwargs = underlying.get_connection_kwargs()
        assert kwargs["socket_connect_timeout"] == settings.redis.connect_timeout
        assert kwargs["socket_timeout"] == settings.redis.socket_timeout

    def test_default_factory_disables_redis_py_internal_retries(self, settings):
        """We own the retry policy; redis-py retrying underneath multiplies latency."""
        client = ResilientRedisClient(settings.redis)

        retry = client._default_factory().get_connection_kwargs()["retry"]

        assert retry.get_retries() == 0
