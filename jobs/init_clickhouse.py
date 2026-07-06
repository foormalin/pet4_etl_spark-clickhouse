from __future__ import annotations

import re

import clickhouse_connect

from common import CLICKHOUSE_HOST, CLICKHOUSE_PORT, ROOT, get_logger


SQL_FILE = ROOT / "sql" / "01_create_tables.sql"
logger = get_logger("init_clickhouse")


def split_sql_statements(sql: str) -> list[str]:
    return [statement.strip() for statement in re.split(r";\s*(?:\r?\n|$)", sql) if statement.strip()]


def main() -> None:
    logger.info("Initializing ClickHouse schema from %s", SQL_FILE)
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

    for statement in split_sql_statements(SQL_FILE.read_text(encoding="utf-8")):
        client.command(statement)
    logger.info("ClickHouse schema initialized")


if __name__ == "__main__":
    main()
