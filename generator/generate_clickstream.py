import json
import os
import random
import uuid
from datetime import datetime, timedelta

RAW_PATH = "data/raw"

EVENT_FLOW = [
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout",
    "purchase"
]

DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["KR", "US", "JP", "SG"]

PRODUCTS = list(range(1001, 1101))


def random_timestamp():
    now = datetime.now()
    delta = timedelta(minutes=random.randint(0, 1440))
    return (now - delta).isoformat()


def generate_session(user_id):
    session_id = str(uuid.uuid4())

    events = []

    max_step = random.choices(
        [2, 3, 4, 5],
        weights=[25, 30, 25, 20]
    )[0]

    device = random.choice(DEVICES)
    country = random.choice(COUNTRIES)
    product_id = random.choice(PRODUCTS)
    price = random.randint(10000, 300000)

    for step in range(max_step):
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": EVENT_FLOW[step],
            "product_id": product_id,
            "device": device,
            "country": country,
            "price": price if EVENT_FLOW[step] == "purchase" else None,
            "timestamp": random_timestamp()
        }

        events.append(event)

    return events


def generate_clickstream(num_users=1000):
    all_events = []

    for user_id in range(1, num_users + 1):
        session_count = random.randint(1, 5)

        for _ in range(session_count):
            all_events.extend(generate_session(user_id))

    return all_events


def save_to_json(events):
    os.makedirs(RAW_PATH, exist_ok=True)

    filename = f"{RAW_PATH}/clickstream_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(filename, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    print(f"Saved {len(events)} events to {filename}")


if __name__ == "__main__":
    events = generate_clickstream(1000)
    save_to_json(events)