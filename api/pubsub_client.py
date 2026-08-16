"""Pub/Sub publisher with explicit failure semantics.

The original module built a ``PublisherClient`` at import and returned
``future.result()`` with no timeout. Both are load-bearing problems:

* Client construction resolves credentials and can block for seconds, so a
  missing service-account key turned into an unexplained slow import rather
  than a clear startup error.
* ``future.result()`` with no timeout means a Pub/Sub incident does not
  surface as a fast 503 — it surfaces as request threads piling up until the
  API stops accepting connections.

This version connects lazily, bounds every publish with a timeout, applies the
transport's own retry policy, and translates every failure into
:class:`~common.errors.PublishError` (transient) or
:class:`~common.errors.PayloadTooLargeError` (permanent) so callers can map
them onto HTTP status codes without importing gRPC exception types.

Messages carry attributes (``event_id``, ``schema``, ``schema_version``) so a
subscriber can route and deduplicate without parsing the body — and so a
schema change is a visible, filterable property of the stream.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from typing import Any, Callable, Mapping

from dotenv import load_dotenv

from common.config import Settings, get_settings
from common.errors import PayloadTooLargeError, PublishError
from common.schemas import SCHEMA_VERSION, resolve_schema_name

try:
    from google.api_core import exceptions as gcp_exceptions
    from google.api_core import retry as gcp_retry
    from google.cloud import pubsub_v1
except Exception:  # pragma: no cover - library optional for local/dev runs
    pubsub_v1 = None
    gcp_retry = None
    gcp_exceptions = None

load_dotenv()

logger = logging.getLogger(__name__)

__all__ = ["PubSubPublisher", "publish_event", "get_publisher", "reset_publisher"]

# Kept for backwards compatibility with existing imports and docs.
PROJECT_ID = get_settings().pubsub.project_id
TOPIC_ID = get_settings().pubsub.topic_id


def _default_retry():
    """Retry only on errors where a retry can plausibly succeed.

    Notably absent: ``InvalidArgument`` and ``PermissionDenied``. Retrying a
    malformed message or a missing IAM binding just multiplies the failure.
    """
    if gcp_retry is None or gcp_exceptions is None:  # pragma: no cover
        return None
    return gcp_retry.Retry(
        predicate=gcp_retry.if_exception_type(
            gcp_exceptions.ServiceUnavailable,
            gcp_exceptions.DeadlineExceeded,
            gcp_exceptions.InternalServerError,
            gcp_exceptions.Aborted,
            gcp_exceptions.ResourceExhausted,
        ),
        initial=0.25,
        maximum=8.0,
        multiplier=2.0,
        timeout=30.0,
    )


class PubSubPublisher:
    """Thread-safe, lazily-connected publisher.

    Args:
        settings: Runtime configuration.
        client_factory: Injection point for tests; returns a publisher client.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client_factory = client_factory
        self._client: Any | None = None
        self._topic_path: str | None = None
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        """False in local/test runs with no credentials and no emulator.

        Disabled mode is explicit rather than a silent no-op: ``publish``
        returns a ``local-`` prefixed id so nothing downstream mistakes a
        skipped publish for a real one.
        """
        return self._settings.pubsub.enabled and pubsub_v1 is not None

    def _ensure_client(self) -> tuple[Any, str]:
        if self._client is not None and self._topic_path is not None:
            return self._client, self._topic_path

        with self._lock:
            if self._client is None:
                try:
                    factory = self._client_factory or pubsub_v1.PublisherClient
                    self._client = factory()
                    self._topic_path = self._client.topic_path(
                        self._settings.pubsub.project_id, self._settings.pubsub.topic_id
                    )
                except Exception as exc:  # noqa: BLE001
                    raise PublishError(
                        "failed to initialise Pub/Sub publisher",
                        context={"error": type(exc).__name__},
                    ) from exc
        return self._client, self._topic_path  # type: ignore[return-value]

    def publish(
        self,
        event: Mapping[str, Any],
        *,
        attributes: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        """Publish one event and block until the broker confirms it.

        Blocking is intentional at this boundary: the API returns
        ``202 Accepted`` to a PACS only once the event is durably on the topic.
        Returning early would mean acknowledging a clinical event that may
        never have been stored.

        Returns:
            The Pub/Sub message id, or a ``local-<digest>`` placeholder when
            publishing is disabled.

        Raises:
            PayloadTooLargeError: Permanent. Body exceeds the size limit.
            PublishError: Transient. Map to HTTP 503; the caller should retry.
        """
        body = json.dumps(event, default=str, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(body).hexdigest()

        limit = self._settings.pubsub.max_message_bytes
        if len(body) > limit:
            # Check locally so an oversized study surfaces as a clean 413
            # instead of a gRPC InvalidArgument three layers down.
            raise PayloadTooLargeError(
                "event exceeds the maximum Pub/Sub message size",
                context={"bytes": len(body), "limit_bytes": limit},
            )

        message_attributes = self._build_attributes(event, attributes)

        if not self.enabled:
            logger.info(
                "publish_skipped reason=pubsub_disabled event_id=%s digest=%s",
                message_attributes.get("event_id"),
                digest[:12],
            )
            return f"local-{digest[:16]}"

        client, topic_path = self._ensure_client()
        publish_timeout = timeout if timeout is not None else self._settings.pubsub.publish_timeout

        try:
            future = client.publish(
                topic_path, data=body, retry=_default_retry(), **message_attributes
            )
            return str(future.result(timeout=publish_timeout))
        except PayloadTooLargeError:
            raise
        except Exception as exc:  # noqa: BLE001 - gRPC/concurrent futures/etc.
            logger.error(
                "publish_failed event_id=%s digest=%s error=%s",
                message_attributes.get("event_id"),
                digest[:12],
                type(exc).__name__,
            )
            raise PublishError(
                "failed to publish event to Pub/Sub",
                context={"error": type(exc).__name__, "topic": self._settings.pubsub.topic_id},
            ) from exc

    def _build_attributes(
        self, event: Mapping[str, Any], extra: Mapping[str, str] | None
    ) -> dict[str, str]:
        """Attributes must be metadata only — never clinical values.

        Pub/Sub attributes are indexed, appear in subscription filters, and are
        logged by tooling that does not treat them as sensitive, so anything
        patient-identifying put here escapes the controls applied to the body.
        """
        attributes: dict[str, str] = {
            "schema": resolve_schema_name(None, event),
            "schema_version": SCHEMA_VERSION,
            "content_type": "application/json",
        }
        event_id = event.get("event_id")
        if isinstance(event_id, str) and event_id:
            attributes["event_id"] = event_id
        if extra:
            attributes.update({k: str(v) for k, v in extra.items()})
        return attributes

    def close(self) -> None:
        with self._lock:
            client, self._client = self._client, None
            self._topic_path = None
        if client is not None:
            try:
                client.stop()
            except Exception:  # pragma: no cover - best-effort teardown
                logger.debug("publisher_close_failed", exc_info=True)


_publisher: PubSubPublisher | None = None
_publisher_lock = threading.Lock()


def get_publisher() -> PubSubPublisher:
    """Process-wide publisher. Construction performs no I/O."""
    global _publisher
    if _publisher is None:
        with _publisher_lock:
            if _publisher is None:
                _publisher = PubSubPublisher()
    return _publisher


def reset_publisher() -> None:
    """Drop the shared publisher. Used by tests and on shutdown."""
    global _publisher
    if _publisher is not None:
        _publisher.close()
    _publisher = None


def publish_event(
    event: Mapping[str, Any],
    *,
    attributes: Mapping[str, str] | None = None,
    timeout: float | None = None,
) -> str:
    """Publish an event to the configured topic. See :meth:`PubSubPublisher.publish`."""
    return get_publisher().publish(event, attributes=attributes, timeout=timeout)
