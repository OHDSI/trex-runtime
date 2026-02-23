SELECT
    p.product_name,
    p.category,
    p.unit_price,
    SUM(o.quantity) AS total_units_sold,
    ROUND(SUM(o.quantity * p.unit_price), 2) AS total_revenue
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.id
GROUP BY p.product_name, p.category, p.unit_price
