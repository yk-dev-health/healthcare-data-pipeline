import time
from api.main import event_queue

def run_worker():
    # Start the worker process
    print("Worker started...")

    while True:
        # Check if there are events in the queue
        if event_queue:
            # Retrieve the first event (FIFO)
            event = event_queue.pop(0)

            # Simulate processing
            print("Processing event:", event)

        # Wait before checking the queue again
        time.sleep(2)

if __name__ == "__main__":
    run_worker()