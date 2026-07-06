from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def assert_not_empty(df: DataFrame, dataset_name: str) -> None:
    if df.limit(1).count() == 0:
        raise ValueError(f"{dataset_name} is empty")


def assert_no_nulls(df: DataFrame, dataset_name: str, columns: list[str]) -> None:
    for column in columns:
        invalid_rows = df.filter(F.col(column).isNull()).limit(1).count()
        if invalid_rows:
            raise ValueError(f"{dataset_name}.{column} contains null values")


def assert_positive(df: DataFrame, dataset_name: str, column: str) -> None:
    invalid_rows = df.filter(F.col(column) <= 0).limit(1).count()
    if invalid_rows:
        raise ValueError(f"{dataset_name}.{column} must be positive")


def assert_referential_integrity(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_key: str,
    parent_key: str,
    dataset_name: str,
) -> None:
    missing_rows = (
        child_df.select(F.col(child_key).alias("lookup_key"))
        .join(parent_df.select(F.col(parent_key).alias("lookup_key")), "lookup_key", "left_anti")
        .limit(1)
        .count()
    )
    if missing_rows:
        raise ValueError(f"{dataset_name}.{child_key} contains values missing in parent {parent_key}")
