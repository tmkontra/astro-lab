SELECT
    customer_name,
    customer_tier,
    order_count,
    total_revenue,
    average_order_value,
    most_recent_order_ts
FROM customer_order_7_day_report
WHERE report_date = %(report_date)s::date
ORDER BY total_revenue DESC
LIMIT 10;
