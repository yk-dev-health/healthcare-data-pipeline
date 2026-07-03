import os

from celery import Celery
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "healthcare_data_pipeline",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_default_queue="dicom_queue",
    task_always_eager=os.getenv("CELERY_TASK_ALWAYS_EAGER", "False").lower() in ("1", "true", "yes"),
)


def _load_task_processor():
    try:
        from worker.worker import process_dicom_event
        return process_dicom_event
    except ImportError:
        return None


@celery_app.task(name="worker.process_dicom_task")
def process_dicom_task(event: dict):
    processor = _load_task_processor()
    if processor is None:
        raise RuntimeError("DICOM processor unavailable")
    return processor(event)
