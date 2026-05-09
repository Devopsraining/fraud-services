import time
import json
import random
from google.cloud import pubsub_v1

PROJECT_ID = "devops-492107"
TOPIC_ID = "fraud-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

print("Generator started...")

while True:
    txn = {
        "user_id": random.randint(1, 100),
        "amount": random.randint(100, 10000)
    }

    future = publisher.publish(
        topic_path,
        json.dumps(txn).encode("utf-8")
    )

    print(f"Sent: {txn}, message_id={future.result()}")

    time.sleep(1)