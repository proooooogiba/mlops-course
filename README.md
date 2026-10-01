# HW1 — потоковый скоринг транзакций

Сервис читает транзакции из Kafka, применяет предобученную CatBoost-модель на CPU и публикует результат в `scores`. Отдельный consumer сохраняет скоры в PostgreSQL; Streamlit показывает последние 10 фродовых транзакций и распределение скоров последних 100 записей.

```text
test.csv → producer → Kafka transactions → scorer → Kafka scores → consumer → PostgreSQL → UI
```

## Запуск

Нужны Docker с Compose v2, `make`, около 3 ГБ памяти и свободный порт 8501. В корне проекта должна быть папка `Teta ML 1 2025` с [датасетом соревнования](https://www.kaggle.com/competitions/teta-ml-1-2025); для запуска достаточно `test.csv`.

```bash
make up
make status
```

Откройте [UI](http://localhost:8501). `kafka-init` должен завершиться с кодом 0, остальные сервисы — перейти в `healthy`. Порт, порог фрода и параметры PostgreSQL можно изменить через `.env` по образцу `.env.example`.

## Проверка

```bash
make publish                 # Отправить первые 100 строк настоящего test.csv
make smoke                   # Проверить скоринг, запись в БД и повторную доставку
make results                 # Посмотреть последние результаты в PostgreSQL
```

В UI загрузите CSV и нажмите «Отправить на скоринг», затем на вкладке «Результаты» — «Посмотреть результаты». `make publish LIMIT=0` отправляет весь файл; остальные команды доступны через `make help`.

## Данные и модель

- `train.csv`, `test.csv` и `sample_submition.csv` находятся в `Teta ML 1 2025`; датасет исключён из Git и Docker-образа. `test.csv` подключается к контейнерам только для чтения и служит источником сообщений для producer.
- Модель `models/my_catboost.cbm` и статистики `models/preprocessing.json` готовы к inference. Обучения в контейнере нет; повторный экспорт из `train.csv` выполняется командами `make setup` и `make preprocess`, после него — `make up`.
- Результат содержит `transaction_id`, `score`, `fraud_flag`; фрод определяется как `score > 0.98`. PostgreSQL хранит результаты скоринга, повторная доставка одного ID не создаёт дубли, некорректные сообщения отправляются в `dead_letters`.

## Разработка и остановка

```bash
make setup                   # Создать .venv и установить зависимости разработки
make test                    # Запустить unit-тесты
make logs                    # Посмотреть логи сервисов
make down                    # Остановить контейнеры; данные сохранятся в томах
```

`requirements.txt` используется контейнерами. `requirements-dev.txt` добавляет pytest для локальных тестов и CI. Unit-тесты используют небольшие фикстуры, локальный smoke-тест — настоящий `test.csv`.

Исходная модель взята из [проекта семинара](https://github.com/NikitaMalykhin/mts25_mlops_hw1_fraud_detector). Разработка ведётся в ветке `hw1`.
