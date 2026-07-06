SELECT
    order_date,
    region,
    category,
    orders_count,
    gmv,
    avg_order_value
FROM analytics.daily_sales
ORDER BY order_date, gmv DESC;

SELECT
    segment,
    count() AS customers,
    sum(total_spent) AS revenue
FROM analytics.customer_segments
GROUP BY segment
ORDER BY revenue DESC;

SELECT
    category,
    sum(revenue) AS revenue,
    sum(items_sold) AS items_sold
FROM analytics.product_performance
GROUP BY category
ORDER BY revenue DESC;
