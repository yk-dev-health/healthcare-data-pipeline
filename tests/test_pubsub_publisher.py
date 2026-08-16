"""Publisher behaviour: lazy connect, bounded waits, typed failures.

Covers the producer half of the boundary. The consumer half lives in
``test_message_handler.py``.
"""

from __future__ import annotations

import json

import pytest

from api.pubsub_client import PubSubPublisher
from common.config import PubSubSettings, Settings
from common.errors import PayloadTooLargeError, PublishError


class FakeFuture:
    def __init__(self, message_id: str = "msg-1", error: Exception | None = None):
        self._message_id = message_id
        self._error = error
        self.timeout_seen: float | None = None

    def result(self, timeout: float | None = None):
        self.timeout_seen = timeout
        if self._error is not None:
            raise self._error
        return self._message_id


class FakePublisherClient:
    def __init__(self, error: Exception | None = None):
        self._error = error
        self.published: list[tuple[str, bytes, dict]] = []
        self.stopped = False

    def topic_path(self, project: str, topic: str) -> str:
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data, retry=None, **attributes):
        self.published.append((topic, data, attributes))
        return FakeFuture(error=self._error)

    def stop(self):
        self.stopped = True


@pytest.fixture
def enabled_settings(settings: Settings) -> Settings:
    return Settings(
        environment=settings.environment,
        patient_hash_salt=settings.patient_hash_salt,
        redis=settings.redis,
        idempotency=settings.idempotency,
        pubsub=PubSubSettings(
            project_id="test-project",
            topic_id="test-topic",
            subscription_id="test-sub",
            enabled=True,
            publish_timeout=7.5,
            max_message_bytes=1024,
        ),
        quarantine=settings.quarantine,
    )


class TestDisabledMode:
    def test_disabled_publisher_returns_a_marked_placeholder(self, settings, dicom_payload):
        """Local/test runs must never silently look like a real publish."""
        publisher = PubSubPublisher(settings)

        message_id = publisher.publish(dicom_payload)

        assert message_id.startswith("local-")

    def test_disabled_publisher_never_constructs_a_client(self, settings, dicom_payload):
        def exploding_factory():
            raise AssertionError("must not construct a client when disabled")

        PubSubPublisher(settings, client_factory=exploding_factory).publish(dicom_payload)

    def test_placeholder_is_stable_for_identical_events(self, settings, dicom_payload):
        publisher = PubSubPublisher(settings)

        assert publisher.publish(dicom_payload) == publisher.publish(dicom_payload)


class TestLazyConnection:
    def test_no_client_is_built_at_construction(self, enabled_settings):
        """Building a PublisherClient resolves credentials and can block for seconds."""
        created = {"count": 0}

        def factory():
            created["count"] += 1
            return FakePublisherClient()

        PubSubPublisher(enabled_settings, client_factory=factory)

        assert created["count"] == 0

    def test_client_is_built_once_and_reused(self, enabled_settings, dicom_payload):
        created = {"count": 0}
        fake = FakePublisherClient()

        def factory():
            created["count"] += 1
            return fake

        publisher = PubSubPublisher(enabled_settings, client_factory=factory)
        publisher.publish(dicom_payload)
        publisher.publish(dicom_payload)

        assert created["count"] == 1
        assert len(fake.published) == 2

    def test_client_construction_failure_is_transient(self, enabled_settings, dicom_payload):
        def factory():
            raise RuntimeError("no credentials")

        publisher = PubSubPublisher(enabled_settings, client_factory=factory)

        with pytest.raises(PublishError) as excinfo:
            publisher.publish(dicom_payload)

        assert excinfo.value.retryable is True


class TestPublishSemantics:
    def test_publish_returns_the_broker_message_id(self, enabled_settings, dicom_payload):
        publisher = PubSubPublisher(
            enabled_settings, client_factory=lambda: FakePublisherClient()
        )

        assert publisher.publish(dicom_payload) == "msg-1"

    def test_publish_is_bounded_by_a_timeout(self, enabled_settings, dicom_payload):
        """An unbounded future.result() turns a Pub/Sub incident into thread exhaustion."""
        fake = FakePublisherClient()
        captured: list[FakeFuture] = []

        original_publish = fake.publish

        def recording_publish(*args, **kwargs):
            future = original_publish(*args, **kwargs)
            captured.append(future)
            return future

        fake.publish = recording_publish
        publisher = PubSubPublisher(enabled_settings, client_factory=lambda: fake)

        publisher.publish(dicom_payload)

        assert captured[0].timeout_seen == enabled_settings.pubsub.publish_timeout

    def test_broker_failure_is_transient(self, enabled_settings, dicom_payload):
        publisher = PubSubPublisher(
            enabled_settings,
            client_factory=lambda: FakePublisherClient(error=RuntimeError("unavailable")),
        )

        with pytest.raises(PublishError) as excinfo:
            publisher.publish(dicom_payload)

        assert excinfo.value.retryable is True

    def test_failure_context_carries_no_patient_data(self, enabled_settings, dicom_payload):
        publisher = PubSubPublisher(
            enabled_settings,
            client_factory=lambda: FakePublisherClient(error=RuntimeError("unavailable")),
        )

        with pytest.raises(PublishError) as excinfo:
            publisher.publish(dicom_payload)

        assert "Jane Doe" not in json.dumps(excinfo.value.to_dict())


class TestSizeLimit:
    def test_oversized_event_is_permanent_not_transient(self, enabled_settings, dicom_payload):
        """Retrying an oversized message can never succeed; fail it cleanly and locally."""
        dicom_payload["source"] = "x" * 2048
        publisher = PubSubPublisher(
            enabled_settings, client_factory=lambda: FakePublisherClient()
        )

        with pytest.raises(PayloadTooLargeError) as excinfo:
            publisher.publish(dicom_payload)

        assert excinfo.value.retryable is False
        assert excinfo.value.context["limit_bytes"] == 1024

    def test_size_check_happens_before_the_client_is_built(
        self, enabled_settings, dicom_payload
    ):
        dicom_payload["source"] = "x" * 2048

        def exploding_factory():
            raise AssertionError("size must be checked before connecting")

        publisher = PubSubPublisher(enabled_settings, client_factory=exploding_factory)

        with pytest.raises(PayloadTooLargeError):
            publisher.publish(dicom_payload)


class TestMessageAttributes:
    def test_schema_metadata_travels_as_attributes(self, enabled_settings, dicom_payload):
        """Lets subscribers route and filter without parsing the body."""
        fake = FakePublisherClient()
        publisher = PubSubPublisher(enabled_settings, client_factory=lambda: fake)

        publisher.publish({**dicom_payload, "event_id": "evt-1"})

        _, _, attributes = fake.published[0]
        assert attributes["schema"] == "dicom_ingestion"
        assert attributes["event_id"] == "evt-1"
        assert attributes["content_type"] == "application/json"
        assert attributes["schema_version"]

    def test_attributes_never_carry_clinical_values(self, enabled_settings, dicom_payload):
        """Attributes are indexed, filterable and logged by tooling that does not
        treat them as sensitive — PHI put here escapes the body's controls."""
        fake = FakePublisherClient()
        publisher = PubSubPublisher(enabled_settings, client_factory=lambda: fake)

        publisher.publish(dicom_payload)

        _, _, attributes = fake.published[0]
        rendered = json.dumps(attributes)
        assert "Jane Doe" not in rendered
        assert "1985-05-17" not in rendered
        assert dicom_payload["study_uid"] not in rendered

    def test_body_is_compact_json(self, enabled_settings, dicom_payload):
        fake = FakePublisherClient()
        publisher = PubSubPublisher(enabled_settings, client_factory=lambda: fake)

        publisher.publish(dicom_payload)

        _, data, _ = fake.published[0]
        assert json.loads(data)["study_uid"] == dicom_payload["study_uid"]
        assert b", " not in data, "separators should be compact"
