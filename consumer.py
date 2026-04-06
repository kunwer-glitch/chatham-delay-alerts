import json
from datetime import datetime
from confluent_kafka import Consumer
import requests

# Kafka config
conf = {
    'bootstrap.servers': 'pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'OIVMCKPNCWGI272N',
    'sasl.password': 'cfltBOCyjjHIvfg8dphmdi/tKcdsYlnwkjMuPU9kCIq94YQ70B15jUEIDscJUdNA',
    'group.id': 'SC-91b9d82d-fdbb-4766-962f-248ba0bfdf92',
    'auto.offset.reset': 'latest',
}

TOPIC = 'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON'

# Route definitions
LONDON_STATIONS = {"VIC", "BFR", "CST", "CHX", "LBG", "STP"}
CHATHAM = "CHM"
DELAY_THRESHOLD = 15  # minutes

# Telegram config
BOT_TOKEN = "8317324668:AAF3g5n2iw937ZAW38hF-ZCqHQ7xuAVurMM"
CHAT_ID = "8129283137"

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, data=data, timeout=5)
    except Exception as e:
        print(f"Telegram send failed: {e}")

def parse_time(t):
    if not t:
        return None
    return datetime.fromisoformat(t.replace("Z", "+00:00"))

def is_london_chatham_route(origin, destination):
    return (
        (origin in LONDON_STATIONS and destination == CHATHAM) or
        (origin == CHATHAM and destination in LONDON_STATIONS)
    )

consumer = Consumer(conf)
consumer.subscribe([TOPIC])

print("Listening for London ↔ Chatham delay alerts...")

# --- TEST TELEGRAM ALERT ---
send_telegram("🚨 *Test alert* 🚨\nThis is a simulated delay notification.")
print("Test alert sent.")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue

    data = json.loads(msg.value().decode('utf-8'))

    # Only process Train Status messages
    if data.get("eventType") != "TS":
        continue

    origin = data.get("origin")
    destination = data.get("destination")

    if not origin or not destination:
        continue

    # Filter for London ↔ Chatham
    if not is_london_chatham_route(origin, destination):
        continue

    print(f"Matched train on route {origin} → {destination}")

    scheduled = parse_time(data.get("scheduled"))
    actual = parse_time(data.get("actual"))

    if not scheduled or not actual:
        continue

    delay = (actual - scheduled).total_seconds() / 60

    if delay >= DELAY_THRESHOLD:
        alert_text = (
            f"🚨 *Delay alert* 🚨\n"
            f"Train {data.get('trainId')} {origin} → {destination}\n"
            f"Delay: {delay:.1f} minutes"
        )

        print("\n" + alert_text)
        print("-" * 60)

        send_telegram(alert_text)
