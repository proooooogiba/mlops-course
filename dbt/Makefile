DBT ?= dbt
export DBT_PROFILES_DIR := $(CURDIR)
.PHONY: deps seed run test build docs serve lint fix check clean
deps:
	$(DBT) deps
seed:
	$(DBT) seed
run:
	$(DBT) run
test:
	$(DBT) test
build:
	$(DBT) build
docs:
	$(DBT) docs generate
serve:
	$(DBT) docs serve --host 127.0.0.1 --port 8082
lint:
	sqlfluff lint models macros tests
fix:
	sqlfluff fix models macros tests
check: deps build docs lint
clean:
	$(DBT) clean
