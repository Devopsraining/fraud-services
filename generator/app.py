import time
import json
import random
from google.cloud import pubsub_v1

PROJECT_ID = "devops-492107"
TOPIC_ID = "fraud-sub"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

print("Generator started...")

while True:
    txn = {
        "user_id": random.randint(1, 100),
        "amount": random.randint(100, 10000)
    }

    publisher.publish(topic_path, json.dumps(txn).encode("utf-8"))

    print("Sent:", txn)

    time.sleep(1)