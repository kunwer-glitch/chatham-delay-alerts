import json
from confluent_kafka import Consumer
import requests
import time

TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID"

KAFKA_BOOTSTRAP = "YOUR_KAFKA_BROKER"
KAFKA_TOPIC = "train_updates"
KAFKA_GROUP = "train_alert_group"

# CRS codes
STP = "STP"   # St Pancras
VIC = "VIC"   # Victoria
CST = "CST"   # Cannon Street
CTM = "CTM"   # Chatham

MONITORED_ORIGINS = {STP, VIC, CST, CTM}
MONITORED_DESTINATIONS = {STP, VIC, CST, CTM}

DELAY_THRESHOLD_MINUTES = 1  # test mode


def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
    requests.post(url, json=payload, timeout=10)


def is_monitored_route(origin, destination):
    return (
        origin in MONITORED_ORIGINS and
        destination in MONITORED_DESTINATIONS and
        origin != destination
    )


def should_alert(delay):
    return delay >= DELAY_THRESHOLD_MINUTES


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "latest"
    })

    consumer.subscribe([KAFKA_TOPIC])

    print("Monitoring STP/VIC/CST ↔ CTM delay alerts...")

    send_telegram_message("Consumer started successfully.")

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

        print(f"[DEBUG] {origin} → {destination}, Delay: {delay}")

        if not is_monitored_route(origin, destination):
            continue

        if should_alert(delay):
            alert = (
                f"🚆 Delay Alert\n"
                f"{origin} → {destination}\n"
                f"Delay: {delay} minutes"
            )
            send_telegram_message(alert)
            print("[INFO] Alert sent to Telegram")

        time.sleep(0.1)


if __name__ == "__main__":
    main()
