from faker import Faker
import psycopg2
from datetime import datetime, timedelta
import random
import time
import uuid

# --------------------------------------------------
# Initialize Faker
# --------------------------------------------------
fake = Faker()

# Shipment lifecycle statuses (state transitions)
SHIPMENT_STATUSES = [
    "CREATED",
    "PICKED_UP",
    "IN_TRANSIT",
    "OUT_FOR_DELIVERY",
    "DELIVERED"
]

# Sample shipping companies (Olist-like scenario)
SHIPPING_COMPANIES = [
    "DHL",
    "FedEx",
    "UPS",
    "Aramex",
    "Local Express"
]

# --------------------------------------------------
# Database helpers
# --------------------------------------------------

def create_table(conn):
    """
    Create the shipment_events table if it does not exist.
    PostgreSQL acts only as the latest-state store.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shipment_events (
            shipment_id UUID PRIMARY KEY,
            order_id UUID,
            customer_id UUID,
            customer_city TEXT,
            customer_country TEXT,
            company_name TEXT,
            order_amount NUMERIC(10,2),
            shipment_status TEXT,
            estimated_delivery_ts TIMESTAMP,
            actual_delivery_ts TIMESTAMP,
            event_ts TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()


def create_shipment(conn):
    """
    Insert a new shipment with initial status = CREATED.
    Each UUID is converted to string to avoid psycopg2 errors.
    """
    cur = conn.cursor()
    now = datetime.utcnow()
    shipment_id = uuid.uuid4()

    cur.execute("""
        INSERT INTO shipment_events (
            shipment_id,
            order_id,
            customer_id,
            customer_city,
            customer_country,
            company_name,
            order_amount,
            shipment_status,
            estimated_delivery_ts,
            actual_delivery_ts,
            event_ts
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        str(shipment_id),           # Convert UUID to string
        str(uuid.uuid4()),          # order_id
        str(uuid.uuid4()),          # customer_id
        fake.city(),
        fake.country(),
        random.choice(SHIPPING_COMPANIES),
        round(random.uniform(50, 1500), 2),
        "CREATED",
        now + timedelta(days=random.randint(2, 7)),
        None,                       # not delivered yet
        now
    ))

    conn.commit()
    cur.close()

    # Print shipment creation details
    print(f"[CREATED] Shipment {shipment_id} inserted at {now}")
    return shipment_id


def update_shipment_status(conn, shipment_id, status):
    """
    Update shipment status.
    Each UPDATE generates a CDC event captured by Debezium.
    """
    cur = conn.cursor()
    now = datetime.utcnow()
    actual_delivery_time = now if status == "DELIVERED" else None

    cur.execute("""
        UPDATE shipment_events
        SET shipment_status = %s,
            actual_delivery_ts = COALESCE(actual_delivery_ts, %s),
            event_ts = %s
        WHERE shipment_id = %s
    """, (
        status,
        actual_delivery_time,
        now,
        str(shipment_id)   # Convert UUID to string
    ))

    conn.commit()
    cur.close()

    # Print shipment update details
    print(f"[{status}] Shipment {shipment_id} updated at {now}")


# --------------------------------------------------
# Main execution loop (stream simulation)
# --------------------------------------------------

if __name__ == "__main__":

    # Connect to PostgreSQL (CDC source)
    conn = psycopg2.connect(
        host="localhost",
        database="olist_delivery_db",
        user="postgres",
        password="postgres",
        port=5432
    )

    # Ensure table exists
    create_table(conn)

    # Continuously generate shipments and status updates
    while True:
        # Create a new shipment
        shipment_id = create_shipment(conn)

        # Progress shipment through its lifecycle with random delays
        for status in SHIPMENT_STATUSES[1:]:
            time.sleep(random.randint(3, 6))  # simulate real-time delay
            update_shipment_status(conn, shipment_id, status)
