SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.price,
    o.order_date
FROM orders o
LEFT JOIN etl_processed_orders p
    ON o.order_id = p.order_id
WHERE p.order_id IS NULL
ORDER BY o.order_id;