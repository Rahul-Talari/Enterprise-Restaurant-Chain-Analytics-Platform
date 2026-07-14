'''
Source Code          : https://learn.microsoft.com/en-us/azure/databricks/ldp/event-hubs
Kafka Cluster        = Event Hubs Namespace
Kafka Topic          = Event Hub
Kafka Producer       = Producer application (policy with send permissions)
Kafka Consumer       = Consumer application (policy with listen permissions)
Kafka ACL            = SAS Policy           (send/listen/manage)
Kafka Broker Address = Namespace Endpoint
'''

import os
import json
import random
import time
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME")

if not EVENTHUB_CONNECTION_STRING or not EVENTHUB_NAME:
    raise RuntimeError(
        "Missing required environment variables: EVENTHUB_CONNECTION_STRING "
        "and/or EVENTHUB_NAME. Check your .env file."
    )

# Current script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Move one folder up -> 00_synthetic_data, then into 3_datasets
base_dir = os.path.dirname(script_dir)
dataset_dir = os.path.join(base_dir, "3_datasets")

df_restaurants = pd.read_csv(os.path.join(dataset_dir, "restaurants.csv"))
df_customers = pd.read_csv(os.path.join(dataset_dir, "customers.csv"))
df_menu_items = pd.read_csv(os.path.join(dataset_dir, "menu_items.csv"))

RESTAURANTS = df_restaurants['restaurant_id'].tolist()
CUSTOMERS = df_customers['customer_id'].tolist()

# Build restaurant_id -> list of menu item dicts without the groupby/apply
# FutureWarning (grouping columns retained in operation)
MENU_BY_RESTAURANT = {
    rest_id: group.to_dict('records')
    for rest_id, group in df_menu_items.groupby('restaurant_id')
}

ORDER_TYPES = ["dine_in", "takeaway", "delivery"]
PAYMENT_METHODS = ["cash", "card", "wallet"]

# Must match CK_Order_Status in the SQL schema exactly.
ORDER_STATUSES = ["received", "preparing", "ready", "delivered", "completed"]


def generate_order():
    order_date = datetime.now(timezone.utc)
    restaurant_id = random.choice(RESTAURANTS)
    customer_id = random.choice(CUSTOMERS)

    menu_items = MENU_BY_RESTAURANT[restaurant_id]
    num_items = random.randint(1, min(5, len(menu_items)))
    selected_items = random.sample(menu_items, num_items)

    items = []
    total_amount = 0.0

    for item in selected_items:
        quantity = random.randint(1, 3)
        subtotal = item["price"] * quantity
        total_amount += subtotal

        items.append({
            "item_id": item["item_id"],
            "name": item["name"],
            "category": item["category"],
            "quantity": quantity,
            "unit_price": item["price"],
            "subtotal": round(subtotal, 2)
        })

    order_id = f"ORD-{order_date.strftime('%Y%m%d')}-{random.randint(100000, 999999)}"
    timestamp_iso = order_date.isoformat().replace("+00:00", "Z")

    return {
        "order_id": order_id,
        "order_timestamp": timestamp_iso,
        "restaurant_id": restaurant_id,
        "customer_id": customer_id,
        "order_type": random.choice(ORDER_TYPES),
        "items": items,  # nested array in the streaming payload; Silver layer
                          # should normalize this vs. the JSON-string form
                          # used in historical_orders.csv
        "total_amount": round(total_amount, 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
        "last_updated": timestamp_iso
    }


def stream_to_eventhub(interval_seconds=3, max_orders=None):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME
    )

    print(f"\nStreaming to Event Hub: {EVENTHUB_NAME}")
    order_count = 0

    try:
        while True:
            order = generate_order()
            event_data_batch = producer.create_batch()
            event_data_batch.add(EventData(json.dumps(order)))
            producer.send_batch(event_data_batch)

            order_count += 1
            print(f"\n[{order_count}] {order['order_id']} | {order['restaurant_id']} | AED {order['total_amount']}")
            print(json.dumps(order, indent=4))

            if max_orders and order_count >= max_orders:
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nStopped")
    finally:
        producer.close()


if __name__ == "__main__":
    stream_to_eventhub(interval_seconds=0.1, max_orders=100)