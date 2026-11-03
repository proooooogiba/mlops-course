# HW4: dbt + ClickHouse

Аналитика транзакций: `raw → staging → 6 marts`.
Версии: dbt-core **1.9.10**, dbt-clickhouse **1.9.5**.

## Запуск в Docker

Из корня репозитория (нужны Docker Engine и Compose v2):

```bash
make hw4-up
# Один раз: загрузить исходный train.csv из HW2, сохранив его в data/train.csv.
make hw4-load
make hw4-check
make hw4-docs
```

Документация: http://localhost:8082. ClickHouse: http://localhost:18123.
`hw4-check` устанавливает пакеты, выполняет `dbt build` (seed, модели,
включая unit tests и data tests), генерирует Docs и запускает SQLFluff.
Загрузчик работает только с пустым источником в отдельном проекте Compose `mlops-hw4`.

## Существующий источник из HW2

```bash
python3.11 -m venv .venv-dbt
. .venv-dbt/bin/activate
pip install -r dbt/requirements.txt
cd dbt
export DBT_PROFILES_DIR="$PWD"
export DBT_RAW_DATABASE=homework
# При необходимости: DBT_CH_HOST, DBT_CH_PORT, DBT_CH_USER, DBT_CH_PASSWORD,
# DBT_CH_SCHEMA (по умолчанию fraud_analytics), DBT_CH_SECURE=true для TLS.
make check
make serve
```

`homework.transactions` — реальное имя источника HW2 этого репозитория.
В изолированном Docker-запуске используется `transactions_db.transactions`,
как в задании. Параметры подключения задаются переменными окружения из примера выше.

## Модели и правила

| Модель | Зерно | Метрики |
|---|---|---|
| stg_transactions | исходная транзакция | UTC, типы, нормализация, сегмент суммы, прокси клиента |
| mart_daily_state_metrics | дата × штат | количество, оборот, средний чек, точный P95, доля крупных (%) |
| mart_fraud_by_category | категория | количество/число меток/фродов, fraud_rate (%), оборот, сумма фрода |
| mart_fraud_by_state | штат | фрод, суммы, уникальные клиенты и мерчанты |
| mart_customer_risk_profile | прокси клиента | история, средний чек, fraud_rate, сегмент риска |
| mart_hourly_fraud_pattern | ISO-день недели × час UTC | число операций, фрод, суммы |
| mart_merchant_analytics | мерчант | оборот, фрод, флаг подозрительности |

Макрос `amount_bucket`: SMALL <100, MEDIUM от 100 до 1000, LARGE >=1000.
`customer_risk`: HIGH >=5%, MEDIUM >=1%, LOW <1%, UNKNOWN при отсутствии меток;
границы заданы seed `risk_thresholds`. Подозрительный мерчант: не менее 20
размеченных операций и fraud_rate >=5%.

Процент фрода вычисляется только среди размеченных транзакций. Без меток
возвращается NULL. Количество и оборот каждой витрины сверяются со staging.
Источник должен быть стабилен на время build и тестов.

Идентификатор клиента — хеш имени, фамилии и координат: это приближение,
поскольку в источнике нет банковского ID. Уникальность проверяется по ключам витрин.

## Тесты и качество

- `not_null`, `unique`, `accepted_values`, `dbt_utils.unique_combination_of_columns`.
- `dbt_expectations.expect_column_values_to_be_between`: неотрицательная сумма.
- Singular: отрицательные суммы, границы fraud_rate, согласованность счетчиков
  и NULL, сверка количества и оборота.
- Unit: границы сегментов суммы и нормализация staging; расчет категории
  с фродом, легитимной операцией и неизвестной меткой.
- SQLFluff использует ClickHouse dialect и dbt templater. Проверка регистра
  функций отключена, поскольку ClickHouse использует имена `countIf`, `toDate`.
- Установка hook: `pre-commit install` в корне репозитория после установки
  `dbt/requirements.txt`. Hook запускает SQLFluff в Docker; сначала выполните `make hw4-up` и
  `make hw4-check`.

Проверено на 786 431 транзакции: `dbt build` — 75/75, `dbt test` — 67/67,
SQLFluff — без ошибок. [Полный лог](evidence/check.log),
[лог dbt test](evidence/dbt-test.log), [DAG](evidence/dbt-docs-dag.png).
