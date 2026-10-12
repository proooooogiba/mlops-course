PYTHON ?= python3
COMPOSE ?= docker compose

.PHONY: up load smoke verify benchmark test down reset logs

up:
	$(COMPOSE) build producer
	$(COMPOSE) up -d --wait kafka clickhouse
	$(COMPOSE) run --rm clickhouse-init

load:
	$(COMPOSE) run --rm producer

smoke:
	$(COMPOSE) run --rm producer python -m scripts.publish_csv /data/train.csv /data/test.csv --limit 1000
	$(COMPOSE) run --rm verify

verify:
	$(COMPOSE) run --rm verify

benchmark:
	$(COMPOSE) run --rm benchmark

test:
	$(PYTHON) -m pytest -q

logs:
	$(COMPOSE) logs --tail=100 kafka clickhouse clickhouse-init

down:
	$(COMPOSE) down

# Destructive ONLY for this homework's named Docker volumes and runtime outputs.
# Does not touch source CSVs or the committed results for the full dataset.
reset:
	$(COMPOSE) run --rm --no-deps --build producer python -c "import shutil; from pathlib import Path; p=Path('/results/runtime'); shutil.rmtree(p) if p.exists() else None"
	$(COMPOSE) down -v --remove-orphans
