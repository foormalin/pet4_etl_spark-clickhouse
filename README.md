# pet4_etl_spark-clickhouse

ETL/ELT-проект для обработки данных интернет-магазина с использованием Apache Spark, PySpark, ClickHouse и Apache Airflow.

Проект имитирует production-like аналитическую платформу: исходные данные хранятся в Parquet, обрабатываются в Spark, проходят bronze/silver/gold слои, проверяются на качество и загружаются в ClickHouse для быстрых аналитических запросов. Airflow DAG оркестрирует весь процесс.

## Что реализовано

- генерация исходных Parquet-данных;
- загрузка raw Parquet в bronze-слой;
- очистка и обогащение данных в silver-слое;
- построение gold-витрин;
- загрузка витрин в ClickHouse;
- Airflow DAG для запуска пайплайна;
- Docker Compose для ClickHouse, PostgreSQL и Airflow;
- проверки качества данных;
- логирование этапов;
- Makefile для частых команд;
- базовые pytest-тесты.

## Архитектура

```text
data/raw/*.parquet
        |
        v
01_load_bronze.py
        |
        v
data/lake/bronze
        |
        v
02_transform_silver.py
        |
        v
data/lake/silver
        |
        v
03_build_gold_and_load_clickhouse.py
        |
        +--> data/lake/gold
        |
        v
ClickHouse analytics
```

Airflow DAG:

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

## Стек

- Python 3.11
- Apache Spark 3.5 / PySpark
- Apache Airflow 2.9
- ClickHouse 24.3
- PostgreSQL 15 для Airflow metadata
- Docker Compose
- Parquet
- pytest

## Структура проекта

```text
.
|-- dags/
|   `-- spark_clickhouse_etl_dag.py
|-- data/
|   `-- raw/
|       `-- README.md
|-- jobs/
|   |-- 01_load_bronze.py
|   |-- 02_transform_silver.py
|   |-- 03_build_gold_and_load_clickhouse.py
|   |-- common.py
|   |-- data_quality.py
|   |-- generate_sample_data.py
|   |-- init_clickhouse.py
|   `-- run_pipeline.py
|-- sql/
|   |-- 01_create_tables.sql
|   `-- 02_analytics_queries.sql
|-- tests/
|-- .env.example
|-- Dockerfile.airflow
|-- docker-compose.yml
|-- Makefile
|-- requirements.txt
|-- requirements-airflow.txt
`-- README.md
```

## Быстрый старт через Docker и Airflow

Скопируйте пример окружения при необходимости:

```bash
cp .env.example .env
```

На Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Соберите Airflow-образ:

```bash
docker compose build
```

Инициализируйте Airflow:

```bash
docker compose run --rm airflow-init
```

Запустите сервисы:

```bash
docker compose up -d clickhouse postgres airflow-webserver airflow-scheduler
```

Откройте Airflow UI:

```text
http://localhost:8080
```

Логин и пароль по умолчанию:

```text
airflow / airflow
```

Запустите DAG:

```text
spark_clickhouse_lakehouse_etl
```

## Локальный запуск без Airflow

Установите зависимости:

```bash
pip install -r requirements.txt
```

Запустите ClickHouse:

```bash
docker compose up -d clickhouse
```

Сгенерируйте raw Parquet:

```bash
python jobs/generate_sample_data.py
```

Запустите весь пайплайн:

```bash
python jobs/run_pipeline.py
```

Или выполните этапы отдельно:

```bash
python jobs/init_clickhouse.py
python jobs/01_load_bronze.py
python jobs/02_transform_silver.py
python jobs/03_build_gold_and_load_clickhouse.py
```

Проверьте витрины:

```bash
docker exec -i clickhouse clickhouse-client < sql/02_analytics_queries.sql
```

## Makefile

```bash
make up          # поднять Docker Compose
make airflow-init
make generate
make init-db
make pipeline
make test
make logs
make down
```

## Данные

Генератор создает четыре Parquet-источника:

- `customers` - клиенты, регионы и даты регистрации;
- `products` - товары, категории и базовые цены;
- `orders` - заказы, статусы, количество и сумма;
- `events` - пользовательские события.

Сгенерированные данные не коммитятся в репозиторий:

```text
data/raw/customers/
data/raw/products/
data/raw/orders/
data/raw/events/
data/lake/
```

## Проверки качества данных

В этапы встроены проверки:

- датасеты не пустые;
- ключевые поля не содержат null;
- цены, суммы и количества положительные;
- `orders.customer_id` существует в `customers`;
- `orders.product_id` существует в `products`;
- gold-витрины не пустые перед загрузкой в ClickHouse.

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

## Тесты

```bash
pytest -q
```

Тесты проверяют вспомогательную SQL-логику и порядок этапов пайплайна.

## Конфигурация

Основные переменные окружения:

```text
CLICKHOUSE_HOST
CLICKHOUSE_PORT
CLICKHOUSE_DATABASE
PROJECT_ROOT
PYTHON_BIN
AIRFLOW_UID
AIRFLOW_ADMIN_USER
AIRFLOW_ADMIN_PASSWORD
AIRFLOW_ADMIN_EMAIL
```

Для локального запуска значения по умолчанию подходят без `.env`. Для Docker/Airflow можно использовать `.env.example` как шаблон.
