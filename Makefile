.DEFAULT_GOAL := help

COMPOSE ?= docker compose
PYTHON ?= .venv/bin/python
LIMIT ?= 100
TRAIN_CSV ?= Teta ML 1 2025/train.csv

.PHONY: help check-data setup build up down status logs publish smoke test preprocess results scores

help:
	@printf '%s\n' \
	  'make up                    Собрать и запустить контейнеры' \
	  'make down                  Остановить контейнеры, сохранив данные' \
	  'make build                 Собрать образы' \
	  'make status                Показать состояние контейнеров' \
	  'make logs                  Показать последние логи' \
	  'make publish LIMIT=100     Отправить N транзакций из test.csv (0 — весь файл)' \
	  'make smoke LIMIT=100       Проверить Kafka → модель → PostgreSQL' \
	  'make results               Показать последние 10 результатов в БД' \
	  'make scores                Прочитать 10 сообщений топика scores' \
	  'make setup                 Подготовить Python-окружение для разработки' \
	  'make test                  Запустить unit-тесты' \
	  'make preprocess            Экспортировать статистики из train.csv'

check-data:
	@test -f "Teta ML 1 2025/test.csv" || { echo 'Поместите test.csv в папку Teta ML 1 2025'; exit 1; }

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements-dev.txt

build:
	$(COMPOSE) build

up: check-data
	$(COMPOSE) up -d --build --wait --wait-timeout 240

down:
	$(COMPOSE) down

status:
	$(COMPOSE) ps -a

logs:
	$(COMPOSE) logs --tail=100 scorer scores-writer ui

publish:
	$(COMPOSE) exec -T scorer python -m scripts.publish_csv --limit $(LIMIT)

smoke:
	$(COMPOSE) exec -T scorer python -m scripts.smoke_test --limit $(LIMIT)

test:
	$(PYTHON) -m pytest -q

preprocess:
	$(PYTHON) -m scripts.export_preprocessing "$(TRAIN_CSV)"

results:
	$(COMPOSE) exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "SELECT transaction_id, score, fraud_flag, received_at FROM transaction_scores ORDER BY id DESC LIMIT 10;"'

scores:
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic scores --from-beginning --max-messages 10 --timeout-ms 5000
