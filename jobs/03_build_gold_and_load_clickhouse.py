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
    write_parquet,
)


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
    client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table}")
    client.insert_df(f"{CLICKHOUSE_DATABASE}.{table}", df.toPandas())


def main() -> None:
    spark = create_spark("etl-03-build-gold-load-clickhouse")

    silver_orders = spark.read.parquet(str(LAKE_DIR / "silver" / "orders_enriched"))

    daily_sales = build_daily_sales(silver_orders)
    customer_segments = build_customer_segments(silver_orders)
    product_performance = build_product_performance(silver_orders)

    write_parquet(daily_sales, LAKE_DIR / "gold" / "daily_sales", ["order_date"])
    write_parquet(customer_segments, LAKE_DIR / "gold" / "customer_segments")
    write_parquet(product_performance, LAKE_DIR / "gold" / "product_performance")

    truncate_and_insert("daily_sales", daily_sales)
    truncate_and_insert("customer_segments", customer_segments)
    truncate_and_insert("product_performance", product_performance)

    spark.stop()


if __name__ == "__main__":
    main()
