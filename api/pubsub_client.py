from google.cloud import pubsub_v1
import json

# Initialise publisher client
publisher = pubsub_v1.PublisherClient()

# Define your GCP project and topic
PROJECT_ID = "healthcare-pipeline-yk-01"
TOPIC_ID = "healthcare-events"

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def publish_event(event: dict):
    """
    Publish an event to Pub/Sub topic.
    """

    # Convert dict to JSON string, then bytes
    data = json.dumps(event).encode("utf-8")

    # Publish message
    future = publisher.publish(topic_path, data=data)

    return future.result()