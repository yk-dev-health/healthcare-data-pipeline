import time
import logging
from api.main import event_queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

def run_worker():
    logging.info("worker_started")

    while True:
        # Check if there are events in the queue
        if event_queue:
            # Retrieve the first event (FIFO)
            event = event_queue.pop(0)

            # Log processing event
            logging.info(f"processing_event patient_id={event['patient_id']}")

        # Wait before checking the queue again
        time.sleep(2)

if __name__ == "__main__":
    run_worker()