import json
import time
import threading
import requests
from confluent_kafka import Producer, Consumer

# ---------------------------------------------------
# 🔧 CONFIG — FILL THESE IN WITH YOUR REAL VALUES
# ---------------------------------------------------

TELEGRAM_BOT_TOKEN = "PUT_YOUR_REAL_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID = "PUT_YOUR_REAL_CHAT_ID_HERE"

# If you do NOT have Kafka yet, temporarily use localhost
# This prevents crashes and allows Telegram test to fire
KAFKA_BOOTSTRAP = "localhost:9092"

KAFKA_TOPIC = "train_updates"
KAFKA_GROUP = "train_alert_group"

# Your real train API endpoint goes here
API_URL = "PUT_YOUR_REAL_API_URL_HERE"

# ---------------------------------------------------
# TELEGRAM
# ---------------------------------------------------

def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text})
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}")

# ---------------------------------------------------
# PRODUCER
# ---------------------------------------------------

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

def calculate_delay(aimed, expected):
    try:
        h1, m1 = map(int, aimed.split(":"))
        h2, m2 = map(int, expected.split(":"))
        return (h2 * 60 + m2) - (h1 * 60 + m1)
    except:
        return 0

def fetch_train_data():
    try:
        r = requests.get(API_URL, timeout=10)
        return r.json()
    except:
        return None

def run_producer():
    print("[PRODUCER] Started")
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

            print(f"[PRODUCER] Sent: {origin} → {destination}, Delay {delay}")

        time.sleep(20)

# ---------------------------------------------------
# CONSUMER
# ---------------------------------------------------

def run_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "latest"
    })

    consumer.subscribe([KAFKA_TOPIC])

    print("[CONSUMER] Started")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        try:
            data = json.loads(msg.value())
        except:
            continue

        origin = data.get("origin_crs")
        destination = data.get("destination_crs")
        delay = data.get("delay_minutes", 0)

        print(f"[CONSUMER] {origin} → {destination}, Delay: {delay}")

        if delay >= 1:
            send_telegram(
                f"🚆 Delay Alert\n{origin} → {destination}\nDelay: {delay} minutes"
            )

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

if __name__ == "__main__":
    # 🔥 Telegram test fires BEFORE Kafka starts
    send_telegram("🚀 Train Alert System Started (Telegram Test OK)")
    print("[SYSTEM] Telegram test sent")

    # Start producer in background
    threading.Thread(target=run_producer, daemon=True).start()

    # Run consumer in foreground
    run_consumer()
