# HW3: переобучение антифрода в Apache Airflow

Решение в ветке **hw3** публичного репозитория [mlops-course](https://github.com/proooooogiba/mlops-course/tree/hw3). CPU-скоринг читает транзакции Kafka, возвращает `score`, `fraud_flag` и `model_version` в Kafka `scores` и сохраняет их в таблице результатов. Airflow по расписанию пополняет размеченную витрину, обучает кандидата на скользящем окне и публикует его, если качество не снизилось. Отдельный DAG выпускает HTML Data Quality отчёт.

Предыдущие задания доступны в ветках [hw1](https://github.com/proooooogiba/mlops-course/tree/hw1) и [hw2](https://github.com/proooooogiba/mlops-course/tree/hw2). DDL и скрипты HW2 сохранены; для его стенда есть отдельный `docker-compose.hw2.yml`. Приведённые ниже команды относятся к HW3.

## Быстрый старт

Нужны Docker Engine, Compose v2.20+ и минимум 4 CPU / 8 GB RAM для Docker. Поддерживаются amd64 и arm64. Все внешние порты привязаны к `127.0.0.1`; сервисы общаются в общей Compose-сети. Первый запуск скачивает образы и зависимости, поэтому может занять несколько минут.

```bash
git clone --branch hw3 https://github.com/proooooogiba/mlops-course.git
cd mlops-course
mkdir -p data runtime/reports
cp .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
# Положите размеченный train.csv из Teta ML в data/train.csv.
# test.csv не нужен: в нём нет целевой переменной.
docker compose up -d --build --wait --wait-timeout 300
```

Источник исходного CSV: [Teta ML на Kaggle](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394). Датасет не распространяется в Git. Вместо Kaggle можно воспроизвести весь стенд на небольших синтетических данных, **до первого запуска**:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m tests.make_training_demo
cp -n .env.example .env
echo "AIRFLOW_UID=$(id -u)" >> .env
# Генератор откажется перезаписывать существующий data/train.csv.
docker compose up -d --build --wait --wait-timeout 300
```

В контейнерах все зависимости уже установлены; локальный Python нужен только для тестов и проверочного скрипта. Для реального CSV локальные зависимости не обязательны. Для локальных ML-тестов и генератора используйте Python 3.11 или 3.12.

| Сервис / результат | Адрес |
|---|---|
| Airflow UI | [localhost:8081](http://localhost:8081), `admin` / `admin` |
| Последний DQ-отчёт | [localhost:8090/latest.html](http://localhost:8090/latest.html) |
| Все DQ-отчёты и сравнения моделей | [localhost:8090](http://localhost:8090) |
| Kafka внутри сети | `kafka:9092`, топики `transactions`, `scores`, `dead_letters` |
| Модели на хосте | `runtime/models/model_v_*.cbm`, указатель `runtime/models/latest.json` |
| Витрина, состояние, результаты | SQLite `runtime/warehouse/fraud.db` |

Если порт занят, измените `AIRFLOW_PORT` / `REPORTS_PORT` в `.env`. При необходимости `TRAIN_FILENAME` выбирает CSV внутри `data/`, а `SHARED_HOST_DIR` — каталог состояния вместо `runtime/`; для проверочного скрипта в таком случае задайте `SHARED_DIR` с тем же путём на хосте. При изменении порта Airflow для проверочного скрипта задайте `AIRFLOW_URL=http://localhost:НОВЫЙ_ПОРТ`.

`AIRFLOW_UID` согласует владельца общего каталога с пользователем хоста: это позволяет проверочному скрипту сохранять результаты и обслуживать `runtime/` на Linux без sudo. Контейнеры Airflow и приложения используют этот UID и группу 0.

`bootstrap` один раз загружает **последние три календарных месяца** размеченного CSV, обучает начальную модель и выпускает DQ-отчёт. Для исходного файла это январь–март 2020, 92 849 строк. Ранние месяцы намеренно не загружаются, чтобы пополнение сразу продолжало текущее окно. Нужны три последовательных месяца и оба класса в каждом месяце. Начальный бинарник обучается на этих данных, а не скачивается отдельно.

После успешного bootstrap автоматически запускаются scorer, simulator, scheduler, webserver и сервер отчётов. Завершившиеся с кодом 0 `bootstrap`, `storage-init`, `kafka-init`, `airflow-init` — ожидаемые одноразовые сервисы. PostgreSQL 16 используется для метаданных Airflow с LocalExecutor. Размеченная витрина и результаты размещены в SQLite WAL на локальном общем каталоге: это компактный учебный стенд, рассчитанный на один хост. Транзакции записи сериализуются через `BEGIN IMMEDIATE` и busy timeout.

## Запуск DAG вручную и проверка результата

В Airflow UI откройте DAG и вкладку Graph. Все DAG включены автоматически, имеют `catchup=False`, `max_active_runs=1` и расписание **каждые 5 минут**:

1. Запустите **supply_labeled_batch** кнопкой Trigger DAG и дождитесь success: `append_month → prune_history`.
2. Запустите **retrain_fraud**, дождитесь success: `prepare_window → train_candidate → evaluate_candidate → publish_model`.
3. Запустите **dataset_quality**, дождитесь success: `inspect_dataset → render_report → assert_quality`.

Если retrain опередил supply и нового месяца ещё нет, подготовка возвращает пустой план, последующие задачи завершаются без изменений. Запустите retrain ещё раз после supply. Никаких случайных разбиений или сдвигов окна при пустом запуске нет.

Ручной запуск через CLI тоже доступен:

```bash
docker compose exec -T airflow-scheduler airflow dags trigger supply_labeled_batch
# После success первого DAG:
docker compose exec -T airflow-scheduler airflow dags trigger retrain_fraud
docker compose exec -T airflow-scheduler airflow dags trigger dataset_quality
```

Проверить новый файл и версию в результатах:

```bash
ls -lh runtime/models/
cat runtime/models/latest.json
docker compose exec -T scorer python -m scripts.inspect_hw3
# Скорер пишет Loaded model v_... при загрузке новой версии:
docker compose logs --tail=30 scorer
```

`inspect_hw3` выводит месяцы витрины, указатель обучения, статусы запусков и последние пять скоров. Поле `model_version` у новых транзакций должно совпасть с `version` в `latest.json`. Simulator непрерывно отправляет транзакции каждые 0.5 секунды с новыми ID и текущим временем, без `target`. Для просмотра Kafka:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic scores --from-beginning --max-messages 5
```

Выходной контракт (первые три поля сохранены из HW1):

```json
{"transaction_id":"...","score":0.023,"fraud_flag":0,"model_version":"v_..."}
```

`fraud_flag = int(score > 0.98)`. На входе остаются исходные признаки транзакции: `transaction_time`, `amount`, `population_city`, координаты покупателя и продавца, `cat_id`, `us_state`, `gender`, `merch`, `jobs`; остальные поля CSV допустимы и не участвуют в модели. Если `transaction_id` отсутствует (сообщения HW2), используется `topic:partition:offset`. Невалидные сообщения направляются в `dead_letters`. Kafka-доставка at least once; таблица `scores` подавляет повторы по ID. Offsets подтверждаются только после подтверждения публикации результата Kafka. Имена топиков и формат основных полей не менялись.

Полностью автоматическая проверка **реального scheduler**, ручных запусков всех трёх DAG и появления скоров текущей версии:

```bash
python3 -m scripts.verify_hw3
# Или: make verify
```

Скрипт использует стандартную библиотеку Python, ждёт окончания задач, проверяет модель, HTML и таблицу скоров, сохраняет `runtime/verification.json`. При отклонении кандидата он проверяет решение gate, а текущая модель продолжает работу. На синтетическом датасете тесты отдельно требуют принятия следующей версии и проверяют hot reload на том же объекте скорера. Для просмотра DAG без UI:

```bash
docker compose exec -T airflow-scheduler airflow dags list
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

## Данные, окно и собственный препроцессинг

`MONTHLY_BATCH_SIZE=10000`, `HISTORY_MONTHS=36`, окно обучения фиксировано — **3 месяца**. Поставка берёт детерминированную выборку с возвращением из существующих строк и переносит `transaction_time` в месяц после максимального месяца витрины. Время суток сохраняется, дни 29–31 обрезаются до последнего допустимого дня нового месяца. Для учебной проверки в батче гарантируются оба класса. Это синтетическое продолжение истории, а не новые независимо размеченные наблюдения.

Первые два месяца окна идут в train, последний — в test. При принятии модели окно сдвигается ровно на один месяц: например, январь–март → февраль–апрель. Снимок обоих разбиений сохраняется в `runtime/work/<run_hash>/dataset.pkl`; между задачами передаются только пути и небольшой план через XCom. Pickle читается только из собственных локальных артефактов задач.

Собственный препроцессинг находится в `pipeline/features.py` и одинаков для обучения и скоринга:

- `log1p(amount)` и `log1p(population_city)`, сумма на жителя;
- расстояние покупатель–продавец по haversine;
- циклические `sin/cos` часа и дня недели, признак выходного;
- нормализация регистра и пробелов категорий, заполнение пропусков категорий;
- исключение ID, имён, адреса и `target` из признаков.

Используется CatBoost на CPU: depth 5, 80 итераций, seed 42, 2 потока. Последний месяц не участвует ни в fit, ни в early stopping. Преобразования не обучаются на test и не требуют внешнего preprocessing-файла; категории обрабатывает CatBoost. Версии ML-библиотек одинаково закреплены в образах Airflow и scorer.

## Проверка качества модели и публикация

Для дисбалансного фрода основная метрика — **Average Precision (AP)**; дополнительная — **ROC-AUC**. Текущий champion заново оценивается на том же последнем месяце, что и кандидат. Сравнивать новую метрику со старым числом с другого месяца было бы некорректно. Обе метрики кандидата должны быть **не ниже** champion; равенство допускается. Первый bootstrap проходит gate без champion. Подробности, размеры train/test и решение сохраняются в `runtime/reports/training_v_*.json`.

Отклонённый кандидат остаётся только в рабочем каталоге. Он не попадает в `models`, не меняет `latest.json` и не продвигает `trained_month`. DAG при этом успешен: отклонение — ожидаемое бизнес-решение, а не инфраструктурная ошибка. Следующий запуск повторит тот же месяц. При детерминированном повторении отклонение может сохраняться: для нового эксперимента можно изменить `MODEL_ITERATIONS` в `.env`, пересоздать сервисы через `docker compose up -d` и запустить новый DAG. Проверка метрик остаётся обязательной.

Принятый бинарник записывается под уникальным именем, затем через `os.replace` атомарно переключается `latest.json`, и только потом фиксируется указатель в SQLite. Скорер перед каждым сообщением перечитывает manifest, загружает новый бинарник и меняет пару `(model, version)` только после успешной загрузки. **Перезапуск scorer для смены модели не нужен.** Старые версии сохраняются для сравнения; автоматического удаления моделей и рабочих снимков в этом учебном проекте нет.

## Повторные запуски и ошибки

- Поставка записывает батч и ledger `batches` одной транзакцией. Повтор task с тем же Airflow `run_id` возвращает уже записанный месяц. Новый ручной запуск с новым ID означает новую поставку.
- Подготовка фиксирует целевой месяц в `training_runs` и повторно использует снимок. Ошибка fit/evaluate не изменяет указатель.
- Если процесс упал после переключения manifest, но до commit указателя, retry допубликует **тот же** месяц и версию. Повтор уже опубликованной задачи не откатывает champion.
- Пока обучение не приняло месяц, supply может уйти максимум на два месяца вперёд. Затем включается backpressure: новые месяцы не создаются. Поэтому отклонение модели не приводит к удалению ещё нужной истории.
- Retention удаляет наблюдения старше 36 календарных месяцев относительно последнего батча и дополнительно защищает следующее окно обучения. Оба ledger сохраняются независимо от удаления исходных строк.
- `down` / `up` сохраняют состояние: seed не загружает CSV повторно, старая модель остаётся доступной.

## Data Quality

Отдельный DAG `dataset_quality.py` проверяет полную размеченную витрину:

1. Датасет не пуст.
2. Присутствуют все обязательные признаки, timestamp, ID и target.
3. ID транзакций уникальны.
4. Временные метки корректно разбираются.
5. Target заполнен и принадлежит `{0, 1}`.
6. Сумма конечна и неотрицательна.
7. Население конечно и неотрицательно.
8. Все координаты конечны, широта в `[-90,90]`, долгота в `[-180,180]`.
9. Пять категориальных признаков заполнены и не пусты после удаления пробелов.
10. В каждом месяце представлены оба класса, месяцев минимум три.
11. В истории нет пропущенных календарных месяцев.

Сначала сохраняются JSON и HTML в отдельном соседнем с `models` каталоге `runtime/reports`, затем задача `assert_quality` падает при нарушении правила. Поэтому HTML доступен **даже для неуспешного DQ DAG**. В отчёте есть ID запуска, число строк, PASS/FAIL и детали каждого правила. `latest.html` — последний отчёт; исторические отчёты сохраняются под именем `dq_<run_hash>.html`.

## Проверки и структура

Результаты выполненной контейнерной проверки, решения gate и подтверждение hot reload сохранены в [results/hw3/REPORT.md](results/hw3/REPORT.md).

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
```

Тесты проверяют временное разбиение, отсутствие target в признаках, повтор seed/supply, backpressure, блокировку обеих ухудшившихся метрик, неизменность указателя после отказа, восстановление после сбоя между manifest и commit, отсутствие отката от повторного запуска старой задачи, DQ-отчёт при ошибке и hot reload без пересоздания Scorer. GitHub Actions дополнительно собирает контейнеры, парсит DAG через Airflow, запускает все DAG реальным scheduler и требует публикации новой версии на синтетических данных (`verify_hw3 --require-publish`). Проверяется появление скоров этой версии, неизменность ID контейнера scorer и нулевое число его перезапусков.

```text
app/                   bootstrap, Kafka scorer, simulator, создание топиков
pipeline/features.py   общий собственный препроцессинг
pipeline/retrain.py    подготовка → обучение → сравнение → публикация
pipeline/supply.py     месячный батч и очистка истории
pipeline/quality.py    DQ-проверки, HTML, утверждение результата
pipeline/storage.py    транзакции, схема и атомарные файловые операции
dags/                  только сборка графов, расписания и зависимости
airflow/Dockerfile     Airflow 2.10.5 + те же ML-зависимости
scripts/verify_hw3.py   интеграционная проверка через Airflow REST API
runtime/               данные, модели, отчёты и рабочие снимки; игнорируется Git
```

Диагностика:

```bash
docker compose ps -a
docker compose logs --tail=100 bootstrap airflow-init airflow-scheduler scorer
docker compose exec -T scorer python -m scripts.inspect_hw3
docker compose down                    # Остановка с сохранением данных.
```

Для полностью чистого прогона остановите стенд, удалите только `runtime/` этого клона и выполните `docker compose down -v`, затем снова `up`. Это удаляет обученную историю, модели, результаты и метаданные Airflow; исходный `data/train.csv` сохраняется. При изменении исходного CSV после seed нужен такой чистый прогон, иначе перезапуск продолжит прежнюю витрину.

Полезные источники: [задание и пример семинара](https://github.com/NikitaMalykhin/mlops_realtime_fraud_detector_airflow), [Airflow в Docker](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html), [LocalExecutor и PostgreSQL](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/set-up-database.html), [определение Average Precision](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.average_precision_score.html). Реализация препроцессинга, SQLite-витрины, gate и публикации в этом решении собственная.
