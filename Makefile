PYTHON ?= python3
COMPOSE ?= docker compose

.PHONY: up down logs test demo verify inspect
up:
	mkdir -p runtime/reports data
	$(COMPOSE) up -d --build --wait --wait-timeout 300

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --tail=100 scorer simulator airflow-scheduler airflow-webserver bootstrap

test:
	$(PYTHON) -m pytest -q

demo:
	$(PYTHON) -m tests.make_training_demo

verify:
	$(PYTHON) -m scripts.verify_hw3

inspect:
	$(COMPOSE) exec -T scorer python -m scripts.inspect_hw3
