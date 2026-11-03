PYTHON ?= python3
COMPOSE ?= docker compose

.PHONY: up down logs test demo verify inspect
up:
	mkdir -p runtime/hw5/reports data
	$(COMPOSE) up -d --build --wait --wait-timeout 300

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --tail=100 scorer simulator airflow-scheduler airflow-webserver bootstrap score-writer prometheus grafana

test:
	$(PYTHON) -m pytest -q

demo:
	$(PYTHON) -m tests.make_training_demo

verify:
	$(PYTHON) -m scripts.verify_hw5

inspect:
	$(COMPOSE) exec -T scorer python -m scripts.inspect_hw3

.PHONY: hw4-up hw4-load hw4-check hw4-docs
hw4-up:
	$(COMPOSE) -f docker-compose.hw4.yml up -d --wait clickhouse
hw4-load:
	$(COMPOSE) -f docker-compose.hw4.yml run --rm --entrypoint python dbt load_source.py /data/train.csv
hw4-check:
	$(COMPOSE) -f docker-compose.hw4.yml run --rm --entrypoint make dbt check
hw4-docs:
	$(COMPOSE) -f docker-compose.hw4.yml run --rm --service-ports dbt docs serve --host 0.0.0.0 --port 8082

.PHONY: hw4-lint
hw4-lint:
	$(COMPOSE) -f docker-compose.hw4.yml run --rm --entrypoint sqlfluff dbt lint models macros tests

.PHONY: verify-hw5 reload-hw5
verify-hw5:
	$(PYTHON) -m scripts.verify_hw5
reload-hw5:
	$(PYTHON) -m scripts.verify_hw5 --reload

.PHONY: verify-hw5-alerts
verify-hw5-alerts:
	$(PYTHON) -m scripts.verify_hw5_alerts
