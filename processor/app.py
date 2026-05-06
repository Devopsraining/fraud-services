import requests
import json
import time
from google.cloud import pubsub_v1

PROJECT_ID = "devops-492107"
SUBSCRIPTION_ID = "fraud-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

def callback(message):
    try:
        data = json.loads(message.data.decode("utf-8"))

        res = requests.post("http://ml-service:8000/predict", json=data)

        print("Transaction:", data)
        print("Prediction:", res.json())

        message.ack()

    except Exception as e:
        print("Error:", e)

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print("Processor started...")

while True:
    time.sleep(10)