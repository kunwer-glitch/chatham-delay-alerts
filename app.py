import json
import time
import threading
import requests
from confluent_kafka import Producer, Consumer

# -----------------------------
# CONFIG
# -----------------------------
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

KAFKA_BOOTSTRAP = "YOUR_KAFKA_BROKER"
KAFKA_TOPIC = "train_updates"
KAFKA_GROUP = "train_alert_group"

API_URL = "YOUR_TRAIN_API_ENDPOINT"

# -----------------------------
# TELEGRAM
# -----------------------------
def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text})
    except Exception as e:
        print(f"[ERROR] Telegram send failed: {e}")

# -----------------------------
# PRODUCER
# -----------------------------
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

# -----------------------------
# CONSUMER
# -----------------------------
def run_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "latest"
    })

    consumer.subscribe([KAFKA_TOPIC])

    print("[CONSUMER] Started")

    # 🔥 SEND TEST MESSAGE ON STARTUP
    send_telegram("🚀 Train Alert System Started Successfully (Test Message)")

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

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    # Start producer in background
    threading.Thread(target=run_producer, daemon=True).start()

    # Run consumer in foreground
    run_consumer()
