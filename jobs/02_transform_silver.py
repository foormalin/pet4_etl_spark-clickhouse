from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

from common import LAKE_DIR, create_spark, write_parquet


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


def main() -> None:
    spark = create_spark("etl-02-transform-silver")

    bronze_dir = LAKE_DIR / "bronze"
    customers = spark.read.parquet(str(bronze_dir / "customers"))
    products = spark.read.parquet(str(bronze_dir / "products"))
    orders = spark.read.parquet(str(bronze_dir / "orders"))

    silver_orders = build_silver_orders(orders, customers, products)
    write_parquet(silver_orders, LAKE_DIR / "silver" / "orders_enriched", ["order_date"])

    spark.stop()


if __name__ == "__main__":
    main()
