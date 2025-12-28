CREATE KEYSPACE IF NOT EXISTS olist_delivery
WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS olist_delivery.shipment_agg (
    merchant_name VARCHAR PRIMARY KEY,
    total_amount DECIMAL,
    total_orders INT
);







CREATE TABLE IF NOT EXISTS shipment_events (
    shipment_id UUID PRIMARY KEY,
    order_id UUID,
    customer_id UUID,
    customer_city TEXT,
    customer_country TEXT,
    company_name TEXT,
    order_amount DECIMAL,
    shipment_status TEXT,
    estimated_delivery_ts TIMESTAMP,
    actual_delivery_ts TIMESTAMP,
    event_ts TIMESTAMP,
    updated_at TIMESTAMP
);
