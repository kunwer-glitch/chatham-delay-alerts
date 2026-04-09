import requests

BOT = "8317324668:AAF3g5n2iw937ZAW38hF-ZCqHQ7xuAVurMM"
CHAT = "433738448"

url = f"https://api.telegram.org/bot{BOT}/sendMessage"
r = requests.post(url, json={"chat_id": CHAT, "text": "Test message from your bot"})

print(r.text)
