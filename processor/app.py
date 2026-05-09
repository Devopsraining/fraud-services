import json
import signal
import sys
import time

import requests
from google.cloud import pubsub_v1

# ==========================================
# Configuration
# ==========================================

PROJECT_ID = "devops-492107"
SUBSCRIPTION_ID = "fraud-sub"

ML_SERVICE_URL = "http://ml-service:8000/predict"

# ==========================================
# Pub/Sub Subscriber Setup
# ==========================================

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_ID
)

print("===================================")
print(" Fraud Processor Started")
print("===================================")
print(f"Subscription: {subscription_path}")
print(f"ML Service: {ML_SERVICE_URL}")
print("Waiting for messages...\n")

# ==========================================
# Message Processing Callback
# ==========================================

def callback(message):
    try:
        # Decode message
        data = json.loads(message.data.decode("utf-8"))

        print("===================================")
        print(f"Received transaction: {data}")

        # Send to ML service
        response = requests.post(
            ML_SERVICE_URL,
            json=data,
            timeout=5
        )

        # Handle ML response
        if response.status_code == 200:

            prediction = response.json()

            print(f"Prediction: {prediction}")

            # Example fraud logic
            if prediction.get("fraud") is True:
                print("🚨 FRAUD DETECTED 🚨")

            # Acknowledge message
            message.ack()

            print("Message acknowledged")

        else:
            print(
                f"ML service error: "
                f"HTTP {response.status_code}"
            )

    except requests.exceptions.Timeout:
        print("ML service request timed out")

    except requests.exceptions.ConnectionError:
        print("Unable to connect to ML service")

    except json.JSONDecodeError:
        print("Invalid JSON received")

    except Exception as e:
        print(f"Processing error: {e}")

# ==========================================
# Start Subscriber
# ==========================================

streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback
)

print("Subscriber connection established\n")

# ==========================================
# Graceful Shutdown
# ==========================================

def shutdown(sig, frame):
    print("\nStopping processor...")

    streaming_pull_future.cancel()

    subscriber.close()

    print("Processor stopped")

    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

# ==========================================
# Keep Container Alive
# ==========================================

try:
    while True:
        time.sleep(30)

except KeyboardInterrupt:
    shutdown(None, None)