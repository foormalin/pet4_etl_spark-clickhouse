from __future__ import annotations

from pyspark.sql import functions as F

from common import LAKE_DIR, RAW_DIR, create_spark, get_logger, log_dataframe_count, write_parquet
from data_quality import assert_no_nulls, assert_not_empty, assert_positive


logger = get_logger("load_bronze")


def main() -> None:
    logger.info("Starting bronze load")
    spark = create_spark("etl-01-load-bronze")

    customers = spark.read.parquet(str(RAW_DIR / "customers"))
    products = spark.read.parquet(str(RAW_DIR / "products"))
    orders = spark.read.parquet(str(RAW_DIR / "orders"))
    events = spark.read.parquet(str(RAW_DIR / "events")).withColumn(
        "event_time",
        F.to_timestamp("event_time"),
    )

    for name, df in {
        "customers": customers,
        "products": products,
        "orders": orders,
        "events": events,
    }.items():
        assert_not_empty(df, name)
        log_dataframe_count(logger, f"raw.{name}", df)

    assert_no_nulls(customers, "customers", ["customer_id", "customer_name", "region"])
    assert_no_nulls(products, "products", ["product_id", "product_name", "category"])
    assert_no_nulls(orders, "orders", ["order_id", "customer_id", "product_id", "order_date", "status"])
    assert_no_nulls(events, "events", ["event_id", "customer_id", "event_time", "event_type"])
    assert_positive(products, "products", "base_price")
    assert_positive(orders, "orders", "quantity")
    assert_positive(orders, "orders", "total_amount")

    write_parquet(customers, LAKE_DIR / "bronze" / "customers")
    write_parquet(products, LAKE_DIR / "bronze" / "products")
    write_parquet(orders, LAKE_DIR / "bronze" / "orders")
    write_parquet(events, LAKE_DIR / "bronze" / "events")

    spark.stop()
    logger.info("Bronze load finished")


if __name__ == "__main__":
    main()
