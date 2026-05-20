-- Seed script for the orders_by_customer_report DAG.
-- Run this against the Postgres database referenced by aiven-psql-01.

CREATE TABLE IF NOT EXISTS customers (
    customer_id integer PRIMARY KEY,
    customer_name text NOT NULL,
    customer_tier text NOT NULL CHECK (customer_tier IN ('standard', 'gold', 'platinum')),
    customer_region text NOT NULL,
    created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id integer PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES customers (customer_id),
    order_ts timestamp NOT NULL,
    order_status text NOT NULL CHECK (
        order_status IN ('paid', 'shipped', 'delivered', 'cancelled')
    ),
    order_total numeric(10, 2) NOT NULL CHECK (order_total >= 0)
);

CREATE TABLE IF NOT EXISTS customer_order_7_day_report (
    report_date date NOT NULL,
    customer_id integer NOT NULL,
    customer_name text NOT NULL,
    customer_tier text NOT NULL,
    order_count integer NOT NULL,
    total_revenue numeric(12, 2) NOT NULL,
    average_order_value numeric(12, 2) NOT NULL,
    most_recent_order_ts timestamp NOT NULL,
    PRIMARY KEY (report_date, customer_id)
);

TRUNCATE TABLE customer_order_7_day_report, orders, customers;

INSERT INTO customers (
    customer_id,
    customer_name,
    customer_tier,
    customer_region,
    created_at
) VALUES
    (1, 'Acme Field Labs', 'platinum', 'northeast', CURRENT_TIMESTAMP - INTERVAL '400 days'),
    (2, 'Northstar Retail', 'gold', 'midwest', CURRENT_TIMESTAMP - INTERVAL '280 days'),
    (3, 'Canyon Outfitters', 'standard', 'west', CURRENT_TIMESTAMP - INTERVAL '190 days'),
    (4, 'Harbor Coffee Co', 'gold', 'southeast', CURRENT_TIMESTAMP - INTERVAL '120 days'),
    (5, 'Pine Ridge Foods', 'standard', 'northwest', CURRENT_TIMESTAMP - INTERVAL '60 days');

INSERT INTO orders (
    order_id,
    customer_id,
    order_ts,
    order_status,
    order_total
) VALUES
    (1001, 1, CURRENT_DATE - INTERVAL '1 day' + TIME '09:05', 'paid', 420.75),
    (1002, 1, CURRENT_DATE - INTERVAL '2 days' + TIME '14:20', 'shipped', 1180.00),
    (1003, 1, CURRENT_DATE - INTERVAL '5 days' + TIME '11:45', 'delivered', 875.50),
    (1004, 2, CURRENT_DATE - INTERVAL '1 day' + TIME '16:10', 'paid', 250.00),
    (1005, 2, CURRENT_DATE - INTERVAL '3 days' + TIME '10:30', 'delivered', 310.25),
    (1006, 3, CURRENT_DATE - INTERVAL '6 days' + TIME '12:00', 'shipped', 90.00),
    (1007, 3, CURRENT_DATE - INTERVAL '8 days' + TIME '12:00', 'delivered', 140.00),
    (1008, 4, CURRENT_DATE - INTERVAL '2 days' + TIME '08:35', 'cancelled', 999.00),
    (1009, 4, CURRENT_DATE - INTERVAL '4 days' + TIME '15:15', 'paid', 640.40),
    (1010, 5, CURRENT_DATE - INTERVAL '7 days' + TIME '09:50', 'delivered', 75.20),
    (1011, 5, CURRENT_DATE - INTERVAL '10 days' + TIME '17:40', 'paid', 220.00);
