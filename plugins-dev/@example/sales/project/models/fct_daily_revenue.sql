SELECT
    o.order_date,
    COUNT(DISTINCT o.order_id) AS num_orders,
    SUM(o.quantity) AS total_units,
    ROUND(SUM(o.quantity * p.unit_price), 2) AS daily_revenue
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.id
GROUP BY o.order_date
