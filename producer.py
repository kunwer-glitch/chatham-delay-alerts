import json
import time
import requests
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = "YOUR_KAFKA_BROKER"
KAFKA_TOPIC = "train_updates"

API_URL = "YOUR_TRAIN_API_ENDPOINT"

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def fetch_train_data():
    try:
        r = requests.get(API_URL, timeout=10)
        return r.json()
    except Exception as e:
        print(f"[ERROR] API fetch failed: {e}")
        return None


def calculate_delay(aimed, expected):
    try:
        h1, m1 = map(int, aimed.split(":"))
        h2, m2 = map(int, expected.split(":"))
        return (h2 * 60 + m2) - (h1 * 60 + m1)
    except:
        return 0


def main():
    print("Producer running...")

    while True:
        data = fetch_train_data()
        if not data:
            time.sleep(10)
            continue

        for train in data.get("services", []):
            origin = train.get("origin_crs")
            destination = train.get("destination_crs")
            aimed = train.get("aimed_departure_time")
            expected = train.get("expected_departure_time")

            delay = calculate_delay(aimed, expected)

            payload = {
                "origin_crs": origin,
                "destination_crs": destination,
                "delay_minutes": delay
            }

            producer.produce(KAFKA_TOPIC, json.dumps(payload).encode("utf-8"))
            producer.flush()

            print(f"[INFO] Sent: {origin} → {destination}, Delay {delay}")

        time.sleep(20)


if __name__ == "__main__":
    main()
