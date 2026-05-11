from google.cloud import pubsub_v1
import json
import logging
import uuid
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

PROJECT_ID = "healthcare-pipeline-yk-01"
SUBSCRIPTION_ID = "healthcare-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

# ----------------------------
# Redis (dedup store)
# ----------------------------
r = redis.Redis(host="localhost", port=6379, db=0)


def already_processed(event_id: str) -> bool:
    return r.get(event_id) is not None


def mark_processed(event_id: str):
    # TTL付きで保存（例: 1日）
    r.setex(event_id, 86400, "1")


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
        logging.info(
            f"processing_event "
            f"event_id={event_id} "
            f"patient_id={event['patient_id']} "
            f"modality={event['modality']}"
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