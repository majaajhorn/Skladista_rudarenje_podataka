USE superstore_dw;

SHOW TABLES;

# upit za povezivanje tablica 'order' i 'customer'
SELECT 
	o.order_id,
    o.order_date,
    o.ship_date,
    c.name AS customer_name,
    c.segment,
    c.state
FROM
	`order` o
JOIN
	customer c ON o.customer_fk = c.id
ORDER BY
	o.order_date DESC
LIMIT 30;

# product koji je napravio najveći profit
SELECT 
    p.product_id,
    p.name AS product_name,
    SUM(od.profit) AS total_profit
FROM
    `order` o
JOIN
    order_details od ON od.order_fk = o.id
JOIN 
    product p ON od.product_fk = p.id
GROUP BY
    p.id, p.product_id, p.name
ORDER BY
    total_profit DESC
LIMIT 1;
