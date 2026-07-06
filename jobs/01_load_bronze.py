from __future__ import annotations

from pyspark.sql import functions as F

from common import LAKE_DIR, RAW_DIR, create_spark, write_parquet


def main() -> None:
    spark = create_spark("etl-01-load-bronze")

    customers = spark.read.parquet(str(RAW_DIR / "customers"))
    products = spark.read.parquet(str(RAW_DIR / "products"))
    orders = spark.read.parquet(str(RAW_DIR / "orders"))
    events = spark.read.parquet(str(RAW_DIR / "events")).withColumn(
        "event_time",
        F.to_timestamp("event_time"),
    )

    write_parquet(customers, LAKE_DIR / "bronze" / "customers")
    write_parquet(products, LAKE_DIR / "bronze" / "products")
    write_parquet(orders, LAKE_DIR / "bronze" / "orders")
    write_parquet(events, LAKE_DIR / "bronze" / "events")

    spark.stop()


if __name__ == "__main__":
    main()
