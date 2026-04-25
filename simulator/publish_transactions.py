import json
import random
import uuid
import time
from datetime import datetime, timezone

MERCHANTS = ["grocery", "electronics", "travel", "atm", "online_gaming"]
COUNTRIES = ["IN", "US", "GB", "NG", "RU", "CN", "BR"]

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC{random.randint(1000, 9999)}",
        "amount": round(random.uniform(1, 10000), 2),
        "currency": "USD",
        "merchant_category": random.choice(MERCHANTS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "country_code": random.choice(COUNTRIES),
        "is_online": random.choice([True, False]),
        "ip_address": f"192.168.{random.randint(0,255)}.{random.randint(0,255)}"
    }

def publish(n=100, delay=0.06):
    for _ in range(n):
        msg = generate_transaction()
        print(f"Generated transaction: {msg['transaction_id']} | 
              Amount: {msg['amount']} | Country: {msg['country_code']}")
        time.sleep(delay)

if __name__ == "__main__":
    publish(n=1000)
