"""Celery application for asynchronous DICOM processing.

The configuration below is mostly about **not blocking the request thread**.

Measured on this codebase, a single ``apply_async`` against an unreachable
Redis took **107 seconds** before raising — inside the HTTP handler for
``POST /dicom/events``. Two separate causes, and the second is the interesting
one:

1. The broker connection retried with backoff and no socket timeout.
2. Far worse: configuring a *result backend* makes ``apply_async`` call
   ``on_task_call``, which synchronously opens a Redis pub/sub subscription so
   that a future ``AsyncResult`` could be awaited. This pipeline never reads a
   Celery result — durability comes from Pub/Sub and the idempotency store —
   so that round-trip bought nothing and cost the request two minutes.

The result backend is therefore removed rather than merely tuned. It also
removes a second copy of clinical metadata (task arguments and return values
are serialised into the backend) that nothing ever consumed, which is a data
minimisation win as well as a latency one.

What remains is bounded by :data:`BROKER_CONNECT_TIMEOUT`, after which
:func:`api.main.receive_dicom_event` takes its documented fallback path.
"""

from __future__ import annotations

from celery import Celery
from dotenv import load_dotenv

from common.config import env_bool, env_float, env_str

load_dotenv()

REDIS_URL = env_str("REDIS_URL", "redis://localhost:6379/0")

#: Seconds to wait for a broker connection before giving up. Must stay well
#: below the upstream HTTP timeout of anything calling the ingestion endpoint.
BROKER_CONNECT_TIMEOUT = env_float("CELERY_BROKER_CONNECT_TIMEOUT", 2.0)

celery_app = Celery("healthcare_data_pipeline", broker=REDIS_URL)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_default_queue="dicom_queue",
    task_always_eager=env_bool("CELERY_TASK_ALWAYS_EAGER", False),
    # -- no result backend --------------------------------------------------
    # Nothing in this pipeline awaits a Celery result. Keeping a backend made
    # every apply_async open a synchronous pub/sub subscription (the 107 s
    # stall described above) and persisted task payloads — clinical metadata —
    # into Redis with no consumer and no retention policy.
    result_backend=None,
    task_ignore_result=True,
    # -- fail fast on an unreachable broker ---------------------------------
    broker_connection_timeout=BROKER_CONNECT_TIMEOUT,
    broker_connection_retry=False,
    broker_connection_retry_on_startup=False,
    broker_connection_max_retries=0,
    # Do not retry the publish either; the caller has an explicit fallback and
    # a retry here would just re-pay the connection timeout.
    task_publish_retry=False,
    broker_transport_options={
        "socket_connect_timeout": BROKER_CONNECT_TIMEOUT,
        "socket_timeout": BROKER_CONNECT_TIMEOUT,
        "max_retries": 0,
    },
    # -- worker-side reliability --------------------------------------------
    # Ack after the task returns, not on receipt, so a worker killed mid-task
    # releases the message back to the queue instead of losing the study.
    task_acks_late=True,
    # ...and requeue it when the worker dies outright, which late-ack alone
    # does not cover.
    task_reject_on_worker_lost=True,
    # One task in flight per worker process. DICOM tasks are long relative to
    # the ack deadline, and prefetching them just makes them time out in a
    # queue that another idle worker could have served.
    worker_prefetch_multiplier=1,
)


def _load_task_processor():
    # Imported lazily to avoid a circular import: worker.worker imports the
    # API-facing helpers, which import this module.
    try:
        from worker.worker import route_event

        return route_event
    except ImportError:  # pragma: no cover - packaging error
        return None


@celery_app.task(
    name="worker.process_dicom_task",
    #: Transient failures are retried by Celery with backoff. Permanent ones
    #: (schema, consent, purpose) are not in this list and surface immediately.
    autoretry_for=(ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=3,
)
def process_dicom_task(event: dict):
    processor = _load_task_processor()
    if processor is None:
        raise RuntimeError("DICOM processor unavailable")
    return processor(event)
