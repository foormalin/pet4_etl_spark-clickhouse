from init_clickhouse import split_sql_statements


def test_split_sql_statements_ignores_empty_chunks() -> None:
    sql = """
    CREATE DATABASE IF NOT EXISTS analytics;

    CREATE TABLE analytics.example
    (
        id UInt64
    )
    ENGINE = MergeTree
    ORDER BY id;
    """

    statements = split_sql_statements(sql)

    assert statements == [
        "CREATE DATABASE IF NOT EXISTS analytics",
        "CREATE TABLE analytics.example\n    (\n        id UInt64\n    )\n    ENGINE = MergeTree\n    ORDER BY id",
    ]


def test_split_sql_statements_handles_trailing_semicolon() -> None:
    assert split_sql_statements("SELECT 1;\n") == ["SELECT 1"]
