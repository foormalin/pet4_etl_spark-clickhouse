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
|-- dags/
|   `-- spark_clickhouse_etl_dag.py
|-- jobs/
|   |-- 01_load_bronze.py
|   |-- 02_transform_silver.py
|   |-- 03_build_gold_and_load_clickhouse.py
|   |-- common.py
|   |-- generate_sample_data.py
|   |-- init_clickhouse.py
|   `-- run_pipeline.py
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
python jobs/init_clickhouse.py
```

Запустить все этапы ETL:

```bash
python jobs/run_pipeline.py
```

Или выполнить этапы отдельно:

```bash
python jobs/01_load_bronze.py
python jobs/02_transform_silver.py
python jobs/03_build_gold_and_load_clickhouse.py
```

Проверить витрины:

```bash
docker exec -i clickhouse clickhouse-client < sql/02_analytics_queries.sql
```

## Airflow DAG

В проект добавлен DAG `dags/spark_clickhouse_etl_dag.py`.

Он оркестрирует полный процесс:

```text
generate_raw_parquet
        |
        v
init_clickhouse
        |
        v
load_bronze
        |
        v
transform_silver
        |
        v
build_gold_and_load_clickhouse
```

Для запуска через Airflow установите зависимости:

```bash
pip install -r requirements-airflow.txt
```

Укажите путь к проекту для DAG:

```bash
set PROJECT_ROOT=C:\path\to\pet4_etl_spark-clickhouse
```

На Linux/macOS:

```bash
export PROJECT_ROOT=/path/to/pet4_etl_spark-clickhouse
```

Затем скопируйте или смонтируйте папку `dags/` в Airflow. DAG использует `BashOperator` и запускает те же Python-этапы, что и локальный `jobs/run_pipeline.py`.

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
