import json
import logging
import os
import uuid
from datetime import date, datetime, timezone
from typing import Any

import redis
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

PROJECT_ID = os.getenv("PROJECT_ID", "healthcare-pipeline-yk-01")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "healthcare-sub")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# ----------------------------
# Redis (dedup store)
# ----------------------------
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def already_processed(event_id: str) -> bool:
    return r.get(event_id) is not None


def mark_processed(event_id: str):
    # TTL付きで保存（例: 1日）
    r.setex(event_id, 86400, "1")


def parse_date(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    if isinstance(value, date):
        return value
    return None


def process_event(event: dict) -> dict:
    issues = []
    study_date = parse_date(event.get("study_date"))
    if study_date is None:
        issues.append("study_date_invalid")
    elif study_date > date.today():
        issues.append("study_date_in_future")

    slice_thickness = event.get("slice_thickness")
    if slice_thickness is None:
        issues.append("slice_thickness_missing")
    elif not isinstance(slice_thickness, (int, float)):
        issues.append("slice_thickness_type_error")
    elif slice_thickness <= 0:
        issues.append("slice_thickness_nonpositive")
    elif slice_thickness > 50:
        issues.append("slice_thickness_unrealistic")

    modality = event.get("modality")
    if modality not in {"CT", "MRI", "US"}:
        issues.append("modality_invalid")

    patient_id = event.get("patient_id")
    if not patient_id or not isinstance(patient_id, str):
        issues.append("patient_id_missing")
    elif not patient_id.startswith("P"):
        issues.append("patient_id_format")

    quality_score = max(0, 100 - len(issues) * 20)
    return {
        "quality_score": quality_score,
        "issues": issues,
        "processed_at": datetime.now(timezone.utc).isoformat()
    }


def callback(message):
    try:
        event = json.loads(message.data.decode("utf-8"))

        # ----------------------------
        # event_id check / generation
        # ----------------------------
        event_id = event.get("event_id")

        if not event_id:
            event_id = str(uuid.uuid4())
            event["event_id"] = event_id

        # ----------------------------
        # idempotency check
        # ----------------------------
        if already_processed(event_id):
            logging.warning(f"duplicate_event_skipped event_id={event_id}")
            message.ack()
            return

        # ----------------------------
        # processing
        # ----------------------------
        quality_report = process_event(event)

        logging.info(
            f"processing_event "
            f"event_id={event_id} "
            f"patient_id={event.get('patient_id')} "
            f"modality={event.get('modality')} "
            f"quality_score={quality_report['quality_score']}"
        )

        if quality_report["issues"]:
            logging.warning(
                f"quality_issues event_id={event_id} issues={quality_report['issues']}"
            )

        # ここでDB保存 / AI処理など
        # process(event)

        # ----------------------------
        # mark processed BEFORE ack safety
        # ----------------------------
        mark_processed(event_id)

        # ----------------------------
        # ack (only after success)
        # ----------------------------
        message.ack()

    except Exception as e:
        logging.exception(f"failed_to_process error={e}")
        message.nack()


def run_worker():
    logging.info("worker_started")

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback
    )

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()


if __name__ == "__main__":
    run_worker()