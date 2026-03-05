import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# ---- Setup ----
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',          # where Kafka is running
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # convert dict → JSON bytes
)

TOPIC = 'ecommerce_events'

# ---- Seed Data ----
# Realistic products to make events feel authentic
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones", "price": 89.99, "category": "Electronics"},
    {"id": "P002", "name": "Running Shoes",        "price": 64.99, "category": "Sports"},
    {"id": "P003", "name": "Coffee Maker",         "price": 49.99, "category": "Kitchen"},
    {"id": "P004", "name": "Python Book",          "price": 39.99, "category": "Books"},
    {"id": "P005", "name": "Yoga Mat",             "price": 29.99, "category": "Sports"},
    {"id": "P006", "name": "Bluetooth Speaker",    "price": 59.99, "category": "Electronics"},
    {"id": "P007", "name": "Desk Lamp",            "price": 34.99, "category": "Home"},
    {"id": "P008", "name": "Water Bottle",         "price": 19.99, "category": "Sports"},
]

# Simulate ~50 active users on the site at any time
USER_IDS = [fake.uuid4() for _ in range(50)]


# ---- Event Builders ----
# Each function returns a dictionary representing one event

def page_view_event(user_id, product):
    return {
        "event_type": "page_view",
        "event_id": fake.uuid4(),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "session_id": fake.uuid4(),
    }

def add_to_cart_event(user_id, product):
    return {
        "event_type": "add_to_cart",
        "event_id": fake.uuid4(),
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": random.randint(1, 3),
        "price": product["price"],
    }

def order_placed_event(user_id, product):
    quantity = random.randint(1, 3)
    return {
        "event_type": "order_placed",
        "event_id": fake.uuid4(),
        "user_id": user_id,
        "order_id": fake.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
        "payment_method": random.choice(["credit_card", "paypal", "debit_card"]),
        "status": random.choice(["success", "success", "success", "failed"]),  # 75% success rate
    }

def simulate_user_journey():
    """
    Simulates a realistic user journey:
    - Always views a page
    - 60% chance they add to cart
    - 40% chance they place an order
    This mirrors real e-commerce conversion funnels
    """
    user_id = random.choice(USER_IDS)
    product = random.choice(PRODUCTS)

    # Step 1: User always views a product page
    event = page_view_event(user_id, product)
    producer.send(TOPIC, value=event)
    print(f"[PAGE VIEW]   user={user_id[:8]}...  product={product['name']}")

    time.sleep(random.uniform(0.5, 2.0))  # simulate time spent on page

    # Step 2: 60% of users add to cart
    if random.random() < 0.6:
        event = add_to_cart_event(user_id, product)
        producer.send(TOPIC, value=event)
        print(f"[ADD TO CART] user={user_id[:8]}...  product={product['name']}")

        time.sleep(random.uniform(0.5, 1.5))

        # Step 3: 40% of those who add to cart place an order
        if random.random() < 0.4:
            event = order_placed_event(user_id, product)
            producer.send(TOPIC, value=event)
            print(f"[ORDER]       user={user_id[:8]}...  product={product['name']}  total=${event['total_amount']}")


# ---- Main Loop ----
if __name__ == "__main__":
    print("🚀 Starting e-commerce event simulator...")
    print(f"   Sending events to Kafka topic: '{TOPIC}'")
    print(f"   Simulating {len(USER_IDS)} active users across {len(PRODUCTS)} products\n")

    event_count = 0
    try:
        while True:
            simulate_user_journey()
            event_count += 1
            time.sleep(random.uniform(0.2, 1.0))  # pace between user journeys

    except KeyboardInterrupt:
        print(f"\n✅ Simulator stopped. Total journeys simulated: {event_count}")
        producer.close()