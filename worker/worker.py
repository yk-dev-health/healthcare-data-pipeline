from google.cloud import pubsub_v1
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

PROJECT_ID = "healthcare-pipeline-yk-01"
SUBSCRIPTION_ID = "healthcare-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_ID
)


def callback(message):
    try:
        event = json.loads(message.data.decode("utf-8"))

        logging.info(
            f"processing_event "
            f"patient_id={event['patient_id']} "
            f"modality={event['modality']}"
        )

        message.ack()

    except Exception as e:
        logging.error(f"error={e}")
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