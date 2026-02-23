SELECT
    id,
    name AS product_name,
    category,
    CAST(price AS DECIMAL(10,2)) AS unit_price
FROM products
