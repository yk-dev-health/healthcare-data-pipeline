"""Quarantine sink for messages that can never be processed.

A poison message — malformed JSON, a schema violation, a purpose-limitation
breach — must leave the subscription, or it blocks a delivery slot forever. But
it must not simply vanish either: someone has to be able to answer "which PACS
sent 400 invalid studies last night, and what was wrong with them?"

The tension is that the natural way to make that answerable is to store the
rejected payload, and the rejected payload is clinical data. A quarantine file
has a different lifecycle from the processing store — longer-lived, less
access-controlled, frequently copied into a ticket — so writing PHI there
quietly creates a second uncontrolled copy of patient data. That undoes the
Art. 5(1)(c) minimisation work the rest of the pipeline does.

So the default record contains diagnosis without content:

* a SHA-256 digest of the raw body, enough to correlate with the producer's own
  logs and to group identical failures;
* the byte length;
* PHI-free field-level errors (field path, error type, constraint);
* delivery metadata (attempt count, subscription, timestamp).

Storing the body is possible but opt-in via ``QUARANTINE_STORE_PAYLOAD``, and
:meth:`common.config.Settings.validate_for_runtime` blocks that flag in
production.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from common.config import QuarantineSettings, get_settings

__all__ = ["QuarantineRecord", "QuarantineSink"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QuarantineRecord:
    """One permanently-failed message, rendered PHI-free."""

    quarantined_at: str
    reason: str
    error_code: str
    error_message: str
    event_id: str | None
    schema: str | None
    payload_sha256: str | None
    payload_bytes: int | None
    delivery_attempt: int | None
    subscription: str | None
    attributes: dict[str, str] = field(default_factory=dict)
    field_errors: list[dict[str, Any]] = field(default_factory=list)
    payload: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


class QuarantineSink:
    """Append-only JSONL writer for quarantined messages.

    JSONL because it is append-only by construction (no read-modify-write, so
    concurrent workers cannot corrupt earlier records) and because it loads
    directly into BigQuery or any SIEM without a transform step.
    """

    #: Attribute keys safe to copy verbatim. An allowlist rather than a
    #: denylist: a producer that invents `attributes["patient_name"]` must not
    #: be able to smuggle PHI into the quarantine file by accident.
    SAFE_ATTRIBUTE_KEYS = frozenset(
        {"schema", "schema_version", "content_type", "event_id", "source", "producer", "purpose"}
    )

    def __init__(self, settings: QuarantineSettings | None = None, *, path: str | None = None):
        cfg = settings or get_settings().quarantine
        self._path = path or cfg.path
        self._store_payload = cfg.store_payload
        self._lock = threading.Lock()

        if self._store_payload:
            logger.warning(
                "quarantine_store_payload_enabled path=%s; raw clinical payloads will be "
                "written to disk. This must not be enabled in production.",
                self._path,
            )

    @property
    def path(self) -> str:
        return self._path

    def record(
        self,
        *,
        reason: str,
        error_code: str,
        error_message: str,
        raw: bytes | None = None,
        event_id: str | None = None,
        schema: str | None = None,
        attributes: Mapping[str, str] | None = None,
        delivery_attempt: int | None = None,
        subscription: str | None = None,
        field_errors: list[dict[str, Any]] | None = None,
    ) -> QuarantineRecord:
        """Write one quarantine record. Never raises.

        A failure to write the quarantine file must not turn a permanent error
        into an unacked message, because that would reintroduce the poison-pill
        loop this sink exists to break. Write failures are logged and the
        record is still returned to the caller for its own logging.
        """
        record = QuarantineRecord(
            quarantined_at=datetime.now(timezone.utc).isoformat(),
            reason=reason,
            error_code=error_code,
            error_message=error_message,
            event_id=event_id,
            schema=schema,
            payload_sha256=hashlib.sha256(raw).hexdigest() if raw is not None else None,
            payload_bytes=len(raw) if raw is not None else None,
            delivery_attempt=delivery_attempt,
            subscription=subscription,
            attributes=self._safe_attributes(attributes),
            field_errors=list(field_errors or []),
            payload=self._maybe_payload(raw),
        )

        try:
            self._append(record)
        except OSError as exc:
            logger.error(
                "quarantine_write_failed path=%s error=%s reason=%s",
                self._path,
                type(exc).__name__,
                reason,
            )

        logger.warning(
            "message_quarantined reason=%s code=%s event_id=%s digest=%s attempt=%s",
            reason,
            error_code,
            event_id,
            (record.payload_sha256 or "")[:12],
            delivery_attempt,
        )
        return record

    # -- internals ----------------------------------------------------------

    def _safe_attributes(self, attributes: Mapping[str, str] | None) -> dict[str, str]:
        if not attributes:
            return {}
        return {
            k: str(v)[:256] for k, v in attributes.items() if k in self.SAFE_ATTRIBUTE_KEYS
        }

    def _maybe_payload(self, raw: bytes | None) -> str | None:
        if not self._store_payload or raw is None:
            return None
        return raw.decode("utf-8", errors="replace")

    def _append(self, record: QuarantineRecord) -> None:
        directory = os.path.dirname(os.path.abspath(self._path))
        os.makedirs(directory, exist_ok=True)
        line = json.dumps(record.to_dict(), default=str, ensure_ascii=False) + "\n"
        # A single write() of a line shorter than PIPE_BUF is atomic enough for
        # concurrent appenders on the same host; the lock covers threads within
        # this process.
        with self._lock, open(self._path, "a", encoding="utf-8") as handle:
            handle.write(line)
