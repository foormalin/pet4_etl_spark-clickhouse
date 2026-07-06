from __future__ import annotations

from pathlib import Path

import clickhouse_connect
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
LAKE_DIR = ROOT / "data" / "lake"

CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DATABASE = "analytics"


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("spark-clickhouse-lakehouse-etl")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(str(RAW_DIR / f"{name}.csv"))


def write_parquet(df: DataFrame, path: Path, partition_by: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(path))


def build_silver_orders(orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    paid_orders = (
        orders.filter(F.col("status") == "paid")
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("total_amount", F.col("total_amount").cast(DecimalType(18, 2)))
    )

    clean_customers = customers.select(
        F.col("customer_id").cast(IntegerType()).alias("customer_id"),
        "customer_name",
        "region",
        F.to_date("registration_date").alias("registration_date"),
    )

    clean_products = products.select(
        F.col("product_id").cast(IntegerType()).alias("product_id"),
        "product_name",
        "category",
        F.col("base_price").cast(DecimalType(18, 2)).alias("base_price"),
    )

    return (
        paid_orders.join(clean_customers, "customer_id", "left")
        .join(clean_products, "product_id", "left")
        .filter(F.col("customer_name").isNotNull() & F.col("product_name").isNotNull())
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
    spark = create_spark()

    customers = read_csv(spark, "customers")
    products = read_csv(spark, "products")
    orders = read_csv(spark, "orders")
    events = read_csv(spark, "events").withColumn("event_time", F.to_timestamp("event_time"))

    write_parquet(customers, LAKE_DIR / "bronze" / "customers")
    write_parquet(products, LAKE_DIR / "bronze" / "products")
    write_parquet(orders, LAKE_DIR / "bronze" / "orders")
    write_parquet(events, LAKE_DIR / "bronze" / "events")

    silver_orders = build_silver_orders(orders, customers, products)
    write_parquet(silver_orders, LAKE_DIR / "silver" / "orders_enriched", ["order_date"])

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
