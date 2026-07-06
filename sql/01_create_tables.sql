CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.daily_sales
(
    order_date Date,
    region LowCardinality(String),
    category LowCardinality(String),
    orders_count UInt64,
    active_customers UInt64,
    items_sold UInt64,
    gmv Decimal(18, 2),
    avg_order_value Decimal(18, 2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, region, category);

CREATE TABLE IF NOT EXISTS analytics.customer_segments
(
    customer_id UInt64,
    customer_name String,
    region LowCardinality(String),
    orders_count UInt64,
    total_spent Decimal(18, 2),
    avg_order_value Decimal(18, 2),
    segment LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (segment, region, customer_id);

CREATE TABLE IF NOT EXISTS analytics.product_performance
(
    product_id UInt64,
    product_name String,
    category LowCardinality(String),
    orders_count UInt64,
    items_sold UInt64,
    revenue Decimal(18, 2),
    buyers UInt64
)
ENGINE = MergeTree
ORDER BY (category, revenue, product_id);
