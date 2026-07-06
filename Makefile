.PHONY: up airflow-init down logs generate init-db pipeline test clean

up:
	docker compose build
	docker compose run --rm airflow-init
	docker compose up -d clickhouse postgres airflow-webserver airflow-scheduler

airflow-init:
	docker compose run --rm airflow-init

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler airflow-webserver clickhouse

generate:
	python jobs/generate_sample_data.py

init-db:
	python jobs/init_clickhouse.py

pipeline:
	python jobs/run_pipeline.py

test:
	pytest -q

clean:
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(p, ignore_errors=True) for p in [Path('data/lake'), Path('data/raw/customers'), Path('data/raw/products'), Path('data/raw/orders'), Path('data/raw/events')]]"
