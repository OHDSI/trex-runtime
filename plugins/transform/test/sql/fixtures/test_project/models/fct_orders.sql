SELECT
    o.id,
    o.order_number,
    o.amount,
    c.country_name
FROM stg_orders o
LEFT JOIN stg_countries c ON o.country_code = c.code
