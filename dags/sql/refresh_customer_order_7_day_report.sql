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

DELETE FROM customer_order_7_day_report
WHERE report_date = CURRENT_DATE;

INSERT INTO customer_order_7_day_report (
    report_date,
    customer_id,
    customer_name,
    customer_tier,
    order_count,
    total_revenue,
    average_order_value,
    most_recent_order_ts
)
SELECT
    CURRENT_DATE AS report_date,
    c.customer_id,
    c.customer_name,
    c.customer_tier,
    COUNT(o.order_id)::integer AS order_count,
    SUM(o.order_total)::numeric(12, 2) AS total_revenue,
    AVG(o.order_total)::numeric(12, 2) AS average_order_value,
    MAX(o.order_ts) AS most_recent_order_ts
FROM customers c
JOIN orders o
    ON o.customer_id = c.customer_id
WHERE o.order_ts >= CURRENT_DATE - INTERVAL '7 days'
    AND o.order_ts < CURRENT_DATE + INTERVAL '1 day'
    AND o.order_status IN ('paid', 'shipped', 'delivered')
GROUP BY c.customer_id, c.customer_name, c.customer_tier
ORDER BY total_revenue DESC;
