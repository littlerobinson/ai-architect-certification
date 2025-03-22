import json
import time
import requests
from kafka import KafkaProducer

API_URL = "https://charlestng-real-time-fraud-detection.hf.space/current-transactions"
TOPIC_NAME = "real-time-payments"
KAFKA_BROKER = "localhost:9092"

# Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

while True:
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            # Log the data being sent
            print(f"Sending message: {data}")
            producer.send(TOPIC_NAME, data)
            producer.flush()  # Ensure the message is sent
            print("Message sent successfully")
        else:
            print(f"Erreur API: {response.status_code}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except json.JSONDecodeError as json_err:
        print(f"JSON decode error: {json_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    time.sleep(60)  # Get data every minute
