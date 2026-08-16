"""Drive the Pub/Sub receive path locally, without a broker.

The delivery semantics in ``worker/message_handler.py`` are the part of this
system that is hardest to see from the outside: duplicates, lease contention
and retry budgets are invisible in a normal request/response trace. This script
feeds a handful of messages straight into the handler and prints what it
decided, so the ack/nack matrix can be observed rather than only read about.

Run it twice — once without Redis and once with — to see the fail-closed
idempotency policy change the outcome of the *same* messages:

    python scripts/inspect_delivery.py
    docker run -d --rm --name hdp-redis -p 6379:6379 redis:7-alpine
    python scripts/inspect_delivery.py

Without Redis every valid message is nacked (``idempotency_backend_unavailable``)
because processing clinical data without duplicate protection is refused by
default. With Redis up, the second delivery of the same event is recognised as
a duplicate and dropped.

On Windows, run with ``PYTHONIOENCODING=utf-8`` so the console can render the
audit log output.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common.config import get_settings  # noqa: E402
from common.errors import DownstreamUnavailableError  # noqa: E402
from common.redis_support import get_redis_client  # noqa: E402
from worker.worker import build_message_handler  # noqa: E402


class StubMessage:
    """Minimal stand-in for ``pubsub_v1.subscriber.message.Message``.

    The handler depends only on ``data``, ``attributes``, ``delivery_attempt``,
    ``ack()`` and ``nack()`` — which is what makes it testable without a broker.
    """

    def __init__(self, body, *, attributes=None, delivery_attempt=None):
        self.data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        self.attributes = attributes or {}
        self.delivery_attempt = delivery_attempt
        self.acked = False
        self.nacked = False

    def ack(self) -> None:
        self.acked = True

    def nack(self) -> None:
        self.nacked = True


VALID_EVENT = {
    "event_id": "evt-probe-1",
    "study_uid": "1.2.826.0.1.3680043.8.498.999",
    "modality": "CT",
    "consent_logged": True,
    "purpose": "diagnostic_support",
    "source": "PACS",
}


def scenarios() -> list[tuple[str, StubMessage]]:
    return [
        ("valid event", StubMessage(VALID_EVENT)),
        ("SAME event redelivered", StubMessage(VALID_EVENT)),
        ("malformed JSON", StubMessage(b"{not json at all")),
        (
            "unsupported modality",
            StubMessage({**VALID_EVENT, "event_id": "evt-probe-2", "modality": "PET"}),
        ),
        (
            "unapproved purpose",
            StubMessage({**VALID_EVENT, "event_id": "evt-probe-3", "purpose": "marketing"}),
        ),
    ]


def retry_scenarios() -> list[tuple[str, StubMessage, int | None]]:
    """Transient-failure rows, which need a processor that fails on purpose.

    These are the two most interesting outcomes and the hardest to trigger
    against a live broker: the same downstream failure is *retried* early in
    the delivery budget and *quarantined* once the budget is spent, so a poison
    message cannot circulate forever.
    """
    return [
        ("downstream down (attempt 1)", StubMessage({**VALID_EVENT, "event_id": "evt-r1"}), 1),
        ("downstream down (attempt 5)", StubMessage({**VALID_EVENT, "event_id": "evt-r2"}), 5),
    ]


def main(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.ERROR,
        format="%(levelname)-8s %(message)s",
    )

    redis_up = get_redis_client().healthy()
    print(f"\nRedis: {'UP' if redis_up else 'DOWN (fail-closed policy applies)'}")
    print("-" * 72)
    print(f"{'scenario':<28}{'outcome':<26}{'settle':<7}reason")
    print("-" * 72)

    def show(label, result):
        settle = "ACK" if result.acked else "NACK"
        print(f"{label:<28}{result.outcome.value:<26}{settle:<7}{result.error_code or ''}")

    handler = build_message_handler()
    for label, message in scenarios():
        show(label, handler.handle(message))

    def always_failing(_event):
        raise DownstreamUnavailableError("simulated BigQuery outage")

    failing_handler = build_message_handler(processor=always_failing)
    for label, message, attempt in retry_scenarios():
        message.delivery_attempt = attempt
        show(label, failing_handler.handle(message))

    print("-" * 72)
    budget = get_settings().pubsub.max_delivery_attempts
    print(f"Retry budget: PUBSUB_MAX_DELIVERY_ATTEMPTS={budget}")
    print("Quarantined messages -> data/quarantine.jsonl")
    print("Audit trail          -> logs/audit_trail.jsonl")
    if redis_up:
        print("Idempotency keys     -> redis-cli --scan --pattern 'idemp:*'")
    print()


if __name__ == "__main__":
    main(verbose="-v" in sys.argv)
