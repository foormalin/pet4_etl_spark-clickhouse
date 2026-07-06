# pet4_etl_spark-clickhouse

ETL/ELT-проект для обработки данных интернет-магазина с использованием Apache Spark, PySpark и ClickHouse.

Проект имитирует production-подход к аналитической платформе: исходные данные хранятся в Parquet, обрабатываются в Spark, сохраняются в bronze/silver/gold слоях и загружаются в ClickHouse для быстрых аналитических запросов.

## Что реализовано

- загрузка Parquet-источников в lake-слой;
- преобразование данных средствами PySpark;
- построение bronze/silver/gold слоев;
- расчет витрин продаж, клиентских сегментов и эффективности товаров;
- загрузка агрегированных данных в ClickHouse;
- SQL-скрипты для создания таблиц и проверки аналитических запросов;
- генератор синтетических Parquet-данных для тестового запуска.

## Архитектура

```text
data/raw/customers
data/raw/products
data/raw/orders
data/raw/events
        |
        v
Apache Spark / PySpark
        |
        +--> data/lake/bronze
        +--> data/lake/silver
        +--> data/lake/gold
        |
        v
ClickHouse analytics
        |
        +--> daily_sales
        +--> customer_segments
        +--> product_performance
```

## Стек

- Python 3.11
- Apache Spark 3.5
- PySpark
- ClickHouse
- Docker Compose
- Parquet

## Структура проекта

```text
.
|-- data/
|   `-- raw/
|       `-- README.md
|-- jobs/
|   |-- etl_pipeline.py
|   `-- generate_sample_data.py
|-- sql/
|   |-- 01_create_tables.sql
|   `-- 02_analytics_queries.sql
|-- docker-compose.yml
|-- requirements.txt
`-- README.md
```

После запуска генератора в `data/raw/` появятся Parquet-директории:

```text
data/raw/customers/
data/raw/products/
data/raw/orders/
data/raw/events/
```

## Быстрый старт

Запустить ClickHouse:

```bash
docker compose up -d
```

Установить зависимости:

```bash
pip install -r requirements.txt
```

Сгенерировать исходные Parquet-данные:

```bash
python jobs/generate_sample_data.py
```

Создать таблицы ClickHouse:

```bash
docker exec -i clickhouse clickhouse-client < sql/01_create_tables.sql
```

Запустить ETL:

```bash
python jobs/etl_pipeline.py
```

Проверить витрины:

```bash
docker exec -i clickhouse clickhouse-client < sql/02_analytics_queries.sql
```

## Данные

В проекте используются четыре Parquet-источника:

- `customers` - клиенты, регионы и даты регистрации;
- `products` - товары, категории и базовые цены;
- `orders` - заказы, статусы, количество и сумма;
- `events` - пользовательские события: просмотры, добавления в корзину, покупки.

## Слои Lakehouse

`bronze` хранит сырые данные в Parquet без бизнес-логики.

`silver` хранит очищенные и обогащенные данные: оплаченные заказы соединяются с клиентами и товарами, приводятся типы и даты.

`gold` хранит агрегированные витрины для аналитических запросов и последующей загрузки в ClickHouse.

## Витрины ClickHouse

`analytics.daily_sales`

- продажи по дате, региону и категории;
- количество заказов;
- активные клиенты;
- проданные товары;
- GMV;
- средний чек.

`analytics.customer_segments`

- количество заказов клиента;
- суммарные траты;
- средний чек;
- сегмент клиента.

`analytics.product_performance`

- выручка по товарам;
- проданные единицы;
- количество покупателей;
- эффективность категорий.

## Оптимизация

- исходные и промежуточные данные хранятся в Parquet;
- gold-витрина `daily_sales` партиционируется по дате;
- тяжелые join и aggregation выполняются в Spark;
- ClickHouse использует `MergeTree`;
- таблицы ClickHouse имеют сортировочные ключи под типовые фильтры по датам, регионам и категориям.
