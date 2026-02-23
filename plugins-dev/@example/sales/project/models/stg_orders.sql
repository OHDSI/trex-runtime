SELECT
    id AS order_id,
    product_id,
    quantity,
    CAST(order_date AS DATE) AS order_date,
    customer_region
FROM orders
