# Проверка HW3 — 21 октября 2026

Локально: macOS Apple Silicon, Colima, Linux arm64, Docker Engine 29.5.2,
4 CPU / 8 GB RAM. Использованы закреплённые зависимости из requirements.txt.

- `python -m pytest -q`: **26 passed**.
- `docker compose config --quiet`: успешно.
- Образы приложения и Airflow собраны; `python -m pip check` в Airflow:
  `No broken requirements found`.
- Airflow DagBag загрузил ровно три DAG без import errors, с цепочками из
  4, 2 и 3 задач соответственно.
- Реальный scheduler выполнил ручные запуски всех трёх DAG на исходном
  `train.csv`: все задачи success. Метрики кандидата на новом месяце:
  AP 0.778859 против AP champion 0.816874; ROC-AUC 0.974195 против 0.972976.
  Публикация корректно заблокирована из-за снижения AP.
  Подробности: [real_verification.json](real_verification.json).
- На свежей синтетической витрине bootstrap успешно обучил исходную модель.
  Затем все три DAG запущены вручную через REST API и исполнены scheduler.
  Второй проверочный прогон потребовал принятия кандидата:
  `python -m scripts.verify_hw3 --require-publish`.
- После принятия новой версии появились её скоры, контейнер scorer сохранил
  ID, RestartCount = 0. В его логах последовательно видны загрузки двух
  версий. Оба ID и версии сохранены в
  [demo_verification.json](demo_verification.json).
- Повторная инициализация существующих Kafka-топиков завершилась успешно
  с сообщением `All Kafka topics already exist`.
- DQ-отчёты содержат 11 правил и результат PASS. Последний HTML доступен
  по HTTP на localhost:8090/latest.html. Снимки:
  [реальные данные](real_data_quality.html), [синтетические данные](demo_data_quality.html).
- Kafka console consumer подтвердил наличие `transaction_id`, `score`,
  `fraud_flag`, `model_version` непосредственно в топике `scores`.

Синтетические данные проверяют оркестрацию и смену версии. Их идеальные
метрики не являются оценкой качества обнаружения реального фрода.
Исходные CSV, размеченные строки и бинарники моделей не включены в Git.
GitHub Actions воспроизводит тесты и контейнерную проверку на синтетике;
этот отчёт описывает локальные запуски, а не результат GitHub Actions.
