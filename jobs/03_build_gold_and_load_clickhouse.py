from __future__ import annotations

import clickhouse_connect
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    LAKE_DIR,
    create_spark,
    get_logger,
    log_dataframe_count,
    write_parquet,
)
from data_quality import assert_no_nulls, assert_not_empty, assert_positive


logger = get_logger("build_gold_and_load_clickhouse")


def build_daily_sales(silver_orders: DataFrame) -> DataFrame:
    return silver_orders.groupBy("order_date", "region", "category").agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.countDistinct("customer_id").alias("active_customers"),
        F.sum("quantity").alias("items_sold"),
        F.sum("total_amount").alias("gmv"),
        F.round(F.sum("total_amount") / F.countDistinct("order_id"), 2).alias("avg_order_value"),
    )


def build_customer_segments(silver_orders: DataFrame) -> DataFrame:
    customer_metrics = silver_orders.groupBy("customer_id", "customer_name", "region").agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.sum("total_amount").alias("total_spent"),
        F.round(F.sum("total_amount") / F.countDistinct("order_id"), 2).alias("avg_order_value"),
    )

    return customer_metrics.withColumn(
        "segment",
        F.when(F.col("total_spent") >= 50000, "high_value")
        .when(F.col("orders_count") >= 3, "regular")
        .otherwise("new_or_low_activity"),
    )


def build_product_performance(silver_orders: DataFrame) -> DataFrame:
    return silver_orders.groupBy("product_id", "product_name", "category").agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.sum("quantity").alias("items_sold"),
        F.sum("total_amount").alias("revenue"),
        F.countDistinct("customer_id").alias("buyers"),
    )


def truncate_and_insert(table: str, df: DataFrame) -> None:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DATABASE,
    )
    logger.info("Loading table %s.%s", CLICKHOUSE_DATABASE, table)
    client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table}")
    client.insert_df(f"{CLICKHOUSE_DATABASE}.{table}", df.toPandas())


def main() -> None:
    logger.info("Starting gold build and ClickHouse load")
    spark = create_spark("etl-03-build-gold-load-clickhouse")

    silver_orders = spark.read.parquet(str(LAKE_DIR / "silver" / "orders_enriched"))
    assert_not_empty(silver_orders, "silver_orders")

    daily_sales = build_daily_sales(silver_orders)
    customer_segments = build_customer_segments(silver_orders)
    product_performance = build_product_performance(silver_orders)

    assert_not_empty(daily_sales, "daily_sales")
    assert_no_nulls(daily_sales, "daily_sales", ["order_date", "region", "category"])
    assert_positive(daily_sales, "daily_sales", "orders_count")
    assert_positive(daily_sales, "daily_sales", "gmv")

    assert_not_empty(customer_segments, "customer_segments")
    assert_no_nulls(customer_segments, "customer_segments", ["customer_id", "segment"])

    assert_not_empty(product_performance, "product_performance")
    assert_no_nulls(product_performance, "product_performance", ["product_id", "category"])
    assert_positive(product_performance, "product_performance", "revenue")

    log_dataframe_count(logger, "gold.daily_sales", daily_sales)
    log_dataframe_count(logger, "gold.customer_segments", customer_segments)
    log_dataframe_count(logger, "gold.product_performance", product_performance)

    write_parquet(daily_sales, LAKE_DIR / "gold" / "daily_sales", ["order_date"])
    write_parquet(customer_segments, LAKE_DIR / "gold" / "customer_segments")
    write_parquet(product_performance, LAKE_DIR / "gold" / "product_performance")

    truncate_and_insert("daily_sales", daily_sales)
    truncate_and_insert("customer_segments", customer_segments)
    truncate_and_insert("product_performance", product_performance)

    spark.stop()
    logger.info("Gold build and ClickHouse load finished")


if __name__ == "__main__":
    main()
