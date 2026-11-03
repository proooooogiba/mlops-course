# HW5: мониторинг антифрод-сервиса

Поток: `simulator → Kafka transactions → scorer → Kafka scores → score-writer → PostgreSQL`.
Airflow сохраняет переобучение и публикацию модели из HW3. Скоринг подхватывает
новый файл модели без перезапуска. Prometheus собирает метрики раз в секунду,
Grafana автоматически подключает источники, три дашборда и два алерта.

## Запуск

Нужны Docker с Compose v2.20+, 4 CPU и 8 GB RAM для Docker.

```bash
git clone --branch hw5 https://github.com/proooooogiba/mlops-course.git
cd mlops-course
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
echo "GRAFANA_DB_PASSWORD=$(openssl rand -hex 16)" >> .env
# Положите размеченный train.csv из HW2 в data/train.csv.
# Для небольшого демонстрационного датасета вместо него, до первого запуска:
# python3 -m tests.make_monitoring_demo
make up
```

Существующий `data/train.csv` генератор не перезаписывает. Результаты HW5 лежат
в `runtime/hw5` (модели и отчеты); SQLite-витрина обучения, PostgreSQL,
Prometheus и Grafana хранят состояние в Docker volumes.
Все сервисы находятся в одной Compose-сети, внешние порты доступны только с localhost.
Значения паролей ниже предназначены для локального учебного стенда.

| Сервис | Адрес | Вход |
|---|---|---|
| Grafana | [localhost:3000](http://localhost:3000) | `admin` / `admin` |
| Prometheus | [localhost:9090](http://localhost:9090) | — |
| Метрики скоринга | [localhost:8000/metrics](http://localhost:8000/metrics) | — |
| Метрики записи | [localhost:8001/metrics](http://localhost:8001/metrics) | — |
| Airflow | [localhost:8081](http://localhost:8081) | `admin` / `admin` |
| DQ-отчет | [localhost:8090/latest.html](http://localhost:8090/latest.html) | — |

Порты и скорость потока задаются в `.env.example`. PostgreSQL доступен внутри
сети; Grafana использует отдельного пользователя `grafana_reader` с правом SELECT.
Его пароль задается в `.env` через `GRAFANA_DB_PASSWORD` до первого запуска.

## Дашборды

В Grafana откройте папку **Fraud Monitoring**:

1. [Скоринг транзакций](http://localhost:3000/d/fraud-scoring): TPS, p50/p95 времени
   обработки, доля предсказанного фрода, heatmap скоров, возраст транзакции,
   ошибки и активная версия модели. Оранжевая аннотация с версией отмечает
   первые 30 секунд после загрузки модели.
2. [Запись и аналитика результатов](http://localhost:3000/d/fraud-writer): TPS и p95
   записи, счетчик фрода, heatmap записанных скоров, число скоров и доля фрода
   по версии модели из PostgreSQL. Фильтры **Штат** и **Мерчант** управляют
   панелями TPS и плотности скоров. Плотность рассчитана по интервалам 0.05;
   сумма высот столбцов × 0.05 равна 1. Отдельный barplot показывает среднюю
   долю фрода по категориям в последних 1000 операциях всего потока.
   Бизнес-панель показывает сумму подозрительных операций по штатам.
3. [Системные метрики](http://localhost:3000/d/fraud-system): CPU, память, сеть,
   заполнение диска, дисковый I/O и доступность экспортеров. На macOS это
   метрики Linux VM Docker, а не всей macOS.

Фрод на графиках — предсказанный `fraud_flag`, не подтвержденная разметка.
TPS с фильтрами использует `scored_at` из сервиса скоринга, остальные панели
PostgreSQL — время записи; Prometheus-панели — время сбора метрик.

## Метрики

| Метрика | Значение |
|---|---|
| `fraud_transactions_total` | Успешно обработанные и опубликованные транзакции |
| `fraud_processing_seconds` | Histogram времени обработки до подтверждения Kafka |
| `fraud_score` | Histogram предсказанных вероятностей |
| `fraud_flagged_total`, `fraud_flag_ratio` | Число и доля флагов фрода с начала процесса |
| `fraud_scoring_errors_total` | Ошибочные входные сообщения, отправленные в dead_letters |
| `fraud_event_age_seconds` | Histogram возраста транзакции при скоринге |
| `fraud_model_loads_total{version}` | Успешные загрузки модели по версиям |
| `fraud_model_loaded_at_seconds{version}`, `fraud_model_active{version}` | Момент загрузки и текущая версия |
| `fraud_db_scores_total`, `fraud_db_flagged_total` | Новые записанные скоры и флаги фрода |
| `fraud_db_write_seconds`, `fraud_db_score` | Histograms времени записи и записанных скоров |
| `fraud_db_duplicates_total`, `fraud_db_errors_total` | Повторные доставки и ошибки PostgreSQL |
| `node_cpu_*`, `node_memory_*`, `node_network_*`, `node_filesystem_*`, `node_disk_*` | Системные метрики Node Exporter |

Дополнительная метрика `fraud_event_age_seconds` отсутствует в примере семинара:
она показывает задержку между временем события и скорингом, включая ожидание
в очереди. Смотрите панель «Возраст транзакции при скоринге p95».

Writer фиксирует транзакцию PostgreSQL до подтверждения offset Kafka.
Повторная доставка не создает вторую строку благодаря `transaction_id` и
`ON CONFLICT DO NOTHING`; счетчики записи учитывают только новые строки.

## Проверка потока и смены модели

```bash
make verify-hw5
make reload-hw5
```

Первая команда проверяет рост счетчиков и числа строк, запросы всех панелей,
фильтры, provisioning алертов и повторную запись. Вторая дополнительно обучает
новую модель на текущем трехмесячном окне, проходит прежний quality gate,
публикует новую версию и проверяет ее подхват без перезапуска scorer.
Скользящее окно при демонстрации не сдвигается; плановое переобучение Airflow
продолжает работать с новыми месяцами.

Держите дашборд скоринга открытым при `make reload-hw5`: появится аннотация
с новой версией, затем эта версия появится в таблице PostgreSQL. Результаты
проверки записываются в `runtime/hw5/verification.json`.

## Алерты

Правила появляются в **Alerting → Alert rules** автоматически:

| Правило | Условие | Проверка |
|---|---|---|
| Fraud share above 15% | Доля флагов за минуту >15% в течение 30 с | `FRAUD_THRESHOLD=0 docker compose up -d --no-deps scorer`; подождать около 90 с |
| Scoring stream stopped | За последние 30 с нет обработанных транзакций; pending 20 с | `docker compose stop simulator`; после опустошения очереди подождать около минуты |

При отсутствии метрик правила также переходят в Alerting. После проверки:

```bash
docker compose up -d --no-deps scorer simulator
```

Команда вернет порог из `.env` (по умолчанию 0.98) и запустит поток.
Автоматическая проверка срабатывания и восстановления обоих правил:
`make verify-hw5-alerts`. Доставка уведомлений не настроена; состояние
срабатывания видно в Grafana.

## Файлы и предыдущие задания

Конфигурации находятся в `monitoring/`, дашборды написаны для этого проекта.
Исходный пример: [семинар 11](https://github.com/NikitaMalykhin/mlops_realtime_fraud_detector_monitoring).
Предыдущие решения: [HW3](https://github.com/proooooogiba/mlops-course/tree/hw3),
[HW4 dbt](dbt/README.md).

```bash
make test                   # локальный Python 3.11+ с requirements-dev.txt
make logs
docker compose down         # остановить, сохранив данные
```

Проверки: [29 тестов](monitoring/evidence/tests.log),
[24 панели и горячая смена модели](monitoring/evidence/verification.json),
[срабатывание и восстановление алертов](monitoring/evidence/alerts.json).
