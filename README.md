# Shad MTS MLOps Project 2

Это учебный проект построения системы обнаружения мошеннических транзакций на основе потоковых данных с использованием Kafka, PostgreSQL, Streamlit UI, Grafana и ML-модели CatBoost.

Проект развивает сервис из домашнего задания 1 по MLOps: вместо загрузки статичного файла в контейнер реализована имитация потока транзакций через Kafka. Пользователь загружает `test.csv` через Streamlit UI, каждая строка файла отправляется в Kafka как отдельное сообщение, далее ML-сервис выполняет preprocessing, применяет CatBoost-модель и отправляет результат скоринга в отдельный Kafka topic.

Датасет можно найти [здесь](https://www.kaggle.com/t/c9670cbd31c645839a4fca29e68c2bc2).

---

## Архитектура

Проект состоит из следующих сервисов:

| Сервис | Описание |
|--------|----------|
| **Kafka** | Шина сообщений для потоковой передачи транзакций и результатов скоринга |
| **Zookeeper** | Управление кластером Kafka |
| **Kafka UI** | Веб-интерфейс для мониторинга Kafka topics/messages |
| **fraud_detector** | Микросервис, выполняющий preprocessing и inference CatBoost-модели |
| **scoring_writer** | Потребитель Kafka, сохраняющий результаты скоринга в PostgreSQL |
| **PostgreSQL** | Хранилище результатов скоринга |
| **interface** | Веб-интерфейс Streamlit для загрузки `test.csv` и просмотра результатов |
| **Prometheus** | Система мониторинга и сбора метрик (фактически в данной работе не используется) |
| **Grafana** | Визуализация метрик и построение dashboard на основе PostgreSQL |
| **Node Exporter** | Сбор системных метрик контейнеров/хоста |

Общий поток данных:

```text
Streamlit UI
    ↓
Kafka topic: transactions
    ↓
fraud_detector
    ↓
Kafka topic: scores
    ↓
scoring_writer
    ↓
PostgreSQL table: scores
    ↓
Streamlit UI + Grafana
```

---

## Структура проекта

```text
.
├── fraud_detector/                 # Микросервис скоринга транзакций
│   ├── app/
│   │   └── app.py
│   ├── models/
│   │   └── my_catboost.cbm
│   ├── src/
│   │   ├── kafka_io.py
│   │   ├── preprocessing.py
│   │   └── scorer.py
│   ├── train_data/
│   │   └── train.csv               # Не хранится в GitHub, нужно положить локально
│   ├── Dockerfile
│   └── requirements.txt
├── interface/                      # Веб-интерфейс Streamlit
│   ├── .streamlit/
│   │   └── config.toml
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── scoring_writer/                 # Сервис записи результатов в PostgreSQL
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── postgres/
│   └── init.sql                    # Создание таблицы scores
├── prometheus/                     # Конфигурация Prometheus
│   └── prometheus.yml
├── grafana/                        # Конфигурация и dashboards Grafana
│   ├── dashboards/
│   │   ├── fraud_detector.json
│   │   ├── node_exporter.json
│   │   ├── scoring.json
│   │   └── fraud_postgres_dashboard.json
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboards.yaml
│       └── datasources/
│           ├── prometheus.yaml
│           └── postgres.yaml
├── data/
│   └── .gitkeep
├── docker-compose.yaml             # Основной файл Docker Compose
├── .env.example                    # Пример файла переменных окружения
├── .gitignore                      # Игнорирование приватных и временных файлов
└── README.md
```

---

## Функциональность

### 1. Загрузка CSV-файла с транзакциями

- Поддерживается формат `.csv`.
- Файл `test.csv` загружается через Streamlit UI.
- Каждая строка отправляется в Kafka topic `transactions` как отдельное JSON-сообщение.
- Для каждой строки добавляется `transaction_id`.

### 2. Обработка и скоринг транзакций

- Сервис `fraud_detector` считывает сообщения из Kafka topic `transactions`.
- Выполняет preprocessing данных в соответствии с логикой из соревнования.
- Применяет предобученную CatBoost-модель.
- Возвращает:
  - `score` — вероятность фрода;
  - `fraud_flag` — бинарный флаг фрода.

### 3. Отправка результата скоринга в Kafka

Сервис `fraud_detector` отправляет результаты в Kafka topic `scores`.

Topic `scores` содержит обязательные поля:

```text
transaction_id
score
fraud_flag
```

Также в сообщение добавлены дополнительные поля:

```text
us_state
merch
cat_id
```

Эти поля нужны для реализации Grafana dashboard на зачёт 5: фильтров по штатам и мерчантам, а также barplot по категориям продукта.

### 4. Хранение результатов

Сервис `scoring_writer` читает Kafka topic `scores` и сохраняет данные в PostgreSQL таблицу `scores`.

### 5. Визуализация результатов в Streamlit

Во вкладке "Посмотреть результаты" Streamlit UI выводит:

- 10 последних транзакций с `fraud_flag == 1`;
- гистограмму распределения `score` по последним 100 транзакциям;
- таблицу последних 100 транзакций.

### 6. Визуализация в Grafana

Grafana содержит dashboard:

```text
Realtime Fraud Detection PostgreSQL Dashboard
```

В нём реализованы:

- фильтр по `us_state`;
- фильтр по `merch`;
- график распределения скоров;
- TPS обработки транзакций;
- barplot средней доли фродовых транзакций по `cat_id` в последних 1000 транзакциях.

---

## Технологии

- **Python 3.11**
- **ML**: CatBoost, pandas, numpy, scikit-learn
- **Kafka**: потоковая передача транзакций и результатов скоринга
- **PostgreSQL**: хранение результатов скоринга
- **Streamlit**: пользовательский интерфейс
- **Docker / Docker Compose**: запуск всех сервисов
- **Prometheus**: сбор метрик
- **Grafana**: визуализация результатов и метрик

---

## Переменные окружения

Пример `.env.example`:

```env
# PostgreSQL
POSTGRES_USER=fraud_user
POSTGRES_PASSWORD=fraud_pass
POSTGRES_DB=fraud_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_SCORING_TOPIC=scores

# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
```

Перед запуском нужно создать локальный `.env` файл.

Для Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Для macOS / Linux:

```bash
cp .env.example .env
```

Файл `.env` не должен попадать в GitHub.

---

## Подготовка данных

Перед запуском нужно скачать данные из соревнования:

```text
https://www.kaggle.com/t/c9670cbd31c645839a4fca29e68c2bc2
```

### train.csv

Файл `train.csv` используется как reference dataset для preprocessing.

Его нужно положить локально в директорию:

```text
fraud_detector/train_data/train.csv
```

Файл `train.csv` не хранится в GitHub.

### test.csv

Файл `test.csv` не нужно класть внутрь проекта.

Он загружается через Streamlit UI после запуска контейнеров.

CSV-файлы игнорируются через `.gitignore`, поэтому они не должны попадать в репозиторий.

---

## Как запустить проект

### 1. Клонируйте репозиторий

```bash
git clone https://github.com/SvgPrizrak/mts_mlops_hw2_realtime_fraud_detector.git
cd mts_mlops_hw2_realtime_fraud_detector
```

### 2. Создайте `.env` файл

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

macOS / Linux:

```bash
cp .env.example .env
```

### 3. Проверьте наличие модели и train.csv

Перед сборкой убедитесь, что существуют файлы:

```text
fraud_detector/models/my_catboost.cbm
fraud_detector/train_data/train.csv
```

### 4. Запустите контейнеры

```bash
docker-compose up --build
```

Если используется Docker Compose V2:

```bash
docker compose up --build
```

Первый запуск может занять несколько минут: Docker скачает образы и соберёт Python-сервисы.

---

## Доступные сервисы

| Сервис | URL / подключение |
|--------|-------------------|
| **Streamlit UI** | http://localhost:8501 |
| **Kafka UI** | http://localhost:8081 |
| **PostgreSQL** | http://localhost:5433 |
| **Prometheus** | http://localhost:9090 |
| **Grafana** | http://localhost:3000 |

Важно:

```text
localhost:5433 — внешний порт PostgreSQL на компьютере
postgres:5432 — внутренний адрес PostgreSQL внутри docker-compose сети
```

PostgreSQL проброшен наружу на порт `5433`, потому что на локальной машине порт `5432` уже используется другим PostgreSQL.

Kafka UI доступен на порту `8081`, потому что локальный порт `8080` уже занят Airflow, запущенным в другом контейнере.

При этом внутри docker-compose сети сервисы обращаются друг к другу по внутренним адресам:

```text
PostgreSQL: postgres:5432
Kafka UI: kafka-ui:8080
Kafka: kafka:9092
```

Поэтому в `grafana/provisioning/datasources/postgres.yaml` должен оставаться адрес:

```yaml
url: postgres:5432
```

Не нужно менять его на `localhost:5433`, потому что Grafana работает внутри Docker-сети.

---

## Как использовать

1. Откройте Streamlit UI:

```text
http://localhost:8501
```

2. Перейдите во вкладку:

```text
Отправка транзакций
```

3. Загрузите файл `test.csv`.

4. Для первого теста рекомендуется отправить не весь файл, а небольшое количество строк:

```text
10
```

или

```text
100
```

5. Нажмите кнопку:

```text
Отправить транзакции в Kafka
```

6. После обработки перейдите во вкладку:

```text
Посмотреть результаты
```

7. Нажмите кнопку:

```text
Посмотреть результаты
```

Интерфейс покажет:

- общее количество записей в PostgreSQL;
- 10 последних записей с `fraud_flag == 1`;
- гистограмму скоров последних 100 транзакций;
- таблицу последних 100 транзакций.

---

## Kafka topics

В проекте используются два основных Kafka topic.

| Topic | Назначение |
|-------|------------|
| `transactions` | Входные транзакции из `test.csv` |
| `scores` | Результаты скоринга модели |

Проверить topics можно через Kafka UI:

```text
http://localhost:8081
```

Или через консоль:

```bash
docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Посмотреть сообщения в topic `transactions`:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic transactions --from-beginning --max-messages 3
```

Посмотреть сообщения в topic `scores`:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic scores --from-beginning --max-messages 3
```

---

## Таблица в PostgreSQL

Таблица `scores` содержит следующие поля:

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | SERIAL | Уникальный ID записи |
| `transaction_id` | TEXT | Идентификатор транзакции |
| `score` | DOUBLE PRECISION | Скор модели |
| `fraud_flag` | INTEGER | 1 — мошенничество, 0 — норма |
| `us_state` | TEXT | Штат, используется для фильтра Grafana |
| `merch` | TEXT | Мерчант, используется для фильтра Grafana |
| `cat_id` | TEXT | Категория продукта, используется для barplot в Grafana |
| `created_at` | TIMESTAMP | Дата и время записи результата |

Поля `transaction_id`, `score` и `fraud_flag` являются обязательными по заданию. Поля `us_state`, `merch` и `cat_id` добавлены дополнительно для выполнения требований зачёта на 5 в Grafana.

Проверить таблицу можно так:

```bash
docker exec -it postgres psql -U fraud_user -d fraud_db
```

Количество записей:

```sql
SELECT COUNT(*) FROM scores;
```

Последние записи:

```sql
SELECT *
FROM scores
ORDER BY created_at DESC
LIMIT 10;
```

Последние фродовые транзакции:

```sql
SELECT *
FROM scores
WHERE fraud_flag = 1
ORDER BY created_at DESC
LIMIT 10;
```

Проверка полей для Grafana:

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(score) AS rows_with_score,
    COUNT(us_state) AS rows_with_state,
    COUNT(merch) AS rows_with_merch,
    COUNT(cat_id) AS rows_with_cat_id
FROM scores;
```

Выйти из PostgreSQL:

```sql
\q
```

---

## Grafana

Откройте Grafana:

```text
http://localhost:3000
```

Логин и пароль:

```text
admin / admin
```

Основной dashboard для зачёта на 5:

```text
Realtime Fraud Detection PostgreSQL Dashboard
```

Dashboard использует PostgreSQL datasource с uid:

```text
postgres-ds
```

Dashboard содержит:

- фильтр `us_state`;
- фильтр `merch`;
- `Score density distribution`;
- `Transactions per second`;
- `Fraud rate by product category, last 1000 transactions`.

---

## Важный момент про Grafana

Dashboard JSON должен лежать именно здесь:

```text
grafana/dashboards/fraud_postgres_dashboard.json
```

В папке `grafana/provisioning/dashboards/` должен лежать только конфигурационный YAML:

```text
grafana/provisioning/dashboards/dashboards.yaml
```

Файл `dashboards.yaml` указывает Grafana, откуда загружать dashboards:

```yaml
options:
  path: /var/lib/grafana/dashboards
```

А в `docker-compose.yaml` папка dashboards монтируется так:

```yaml
- ./grafana/dashboards:/var/lib/grafana/dashboards
```

Если dashboard не появился в Grafana, проверьте:

```bash
docker exec -it grafana ls -la /var/lib/grafana/dashboards
```

В списке должен быть файл:

```text
fraud_postgres_dashboard.json
```

---

## Проверка Grafana datasource

Файл PostgreSQL datasource:

```text
grafana/provisioning/datasources/postgres.yaml
```

Должен содержать подключение:

```yaml
url: postgres:5432
```

Не нужно менять его на `localhost:5433`.

Объяснение:

```text
postgres:5432 — адрес PostgreSQL внутри docker-compose сети
localhost:5433 — внешний порт для подключения с компьютера
```

Если Grafana пустая, проверьте:

1. В PostgreSQL есть данные:

```sql
SELECT COUNT(*) FROM scores;
```

2. В dashboard выбран период:

```text
Last 24 hours
```

3. Фильтры `us_state` и `merch` стоят в значении:

```text
All
```

4. PostgreSQL datasource проходит проверку (ВАЖНЫЙ МОМЕНТ - БЕЗ ЭТОГО ПУНКТА КОНКРЕТНО У МЕНЯ ДАШБОРД СНАЧАЛА НЕ ПОЯВИЛСЯ):

```text
Grafana → Connections → Data sources → PostgreSQL → Save & test
```

---

## Мониторинг и метрики

Проект включает Prometheus и Grafana.

Доступны dashboards из шаблона:

- Fraud Detection Dashboard;
- Scoring Dashboard;
- Node Metrics.

Также добавлен dashboard на основе PostgreSQL:

- Realtime Fraud Detection PostgreSQL Dashboard.

---

## Очистка после тестирования

Остановить контейнеры:

```bash
docker-compose down
```

Остановить контейнеры и удалить volumes:

```bash
docker-compose down -v
```

Полная очистка контейнеров, volumes и образов проекта:

```bash
docker-compose down -v --rmi all --remove-orphans
docker builder prune -f
```

После такой очистки проект будет подниматься полностью заново, включая установку зависимостей внутри Python-контейнеров.

---

## Частые проблемы, с которыми я лично столкнулся при разработке

### 1. Порт PostgreSQL 5432 занят

Если возникает ошибка:

```text
Bind for 0.0.0.0:5432 failed: port is already allocated
```

это значит, что на локальной машине уже запущен другой PostgreSQL. В моём локальном окружении порт `5432` уже занят локальным PostgreSQL, поэтому в этом проекте внешний порт PostgreSQL изменён на `5433`:

```yaml
ports:
  - "5433:5432"
```

Поэтому с компьютера PostgreSQL доступен на:

```text
localhost:5433
```

А внутри Docker-сети сервисы используют:

```text
postgres:5432
```

### 2. Порт Kafka UI 8080 занят

Если возникает ошибка:

```text
Bind for 0.0.0.0:8080 failed: port is already allocated
```

это значит, что порт `8080` уже занят другим сервисом. В моём локальном окружении этот порт занят Airflow, запущенным в другом контейнере, поэтому Kafka UI проброшен на порт `8081`:

```yaml
ports:
  - "8081:8080"
```

Kafka UI доступен по адресу:

```text
http://localhost:8081
```

### 3. Grafana dashboard появился, но графики пустые

Проверьте:

```sql
SELECT COUNT(*) FROM scores;
```

Если данных нет, значит проблема не в Grafana, а раньше в pipeline.

Если данные есть, проверьте:

- dashboard time range: `Last 24 hours`;
- фильтры `us_state` и `merch`: `All`;
- PostgreSQL datasource: `Save & test`;
- путь dashboard JSON: `grafana/dashboards/fraud_postgres_dashboard.json`;
- адрес в datasource: `postgres:5432`, а не `localhost:5433`.

### 4. Kafka topic `scores` пустой

Проверьте логи ML-сервиса:

```bash
docker-compose logs fraud_detector --tail=100
```

Должны быть строки:

```text
Prediction completed
Sent score to topic=scores
```

Если их нет, проверьте topic `transactions`.

### 5. PostgreSQL пустой, но topic `scores` содержит сообщения

Проверьте логи сервиса записи:

```bash
docker-compose logs scoring_writer --tail=100
```

Должны быть строки:

```text
Inserted score for transaction_id=...
```

---

## Проверка полного pipeline

После запуска проекта:

1. Откройте Streamlit:

```text
http://localhost:8501
```

2. Загрузите `test.csv`.

3. Отправьте 10–100 строк.

4. Проверьте topic `transactions` и `scores` в Kafka UI:

```text
http://localhost:8081
```

5. Проверьте записи в PostgreSQL:

```sql
SELECT COUNT(*) FROM scores;
```

6. Откройте вкладку Streamlit:

```text
Посмотреть результаты
```

7. Откройте Grafana dashboard:

```text
Realtime Fraud Detection PostgreSQL Dashboard
```

## Соответствие требованиям задания

### Базовые требования реализации

| Требование | Реализация в проекте |
|-----------|----------------------|
| Сервис скоринга читает сообщения из Kafka | Сервис `fraud_detector` читает сообщения из Kafka topic `transactions` |
| Сервис выгружает score модели и fraud flag в Kafka | Сервис `fraud_detector` отправляет результат в Kafka topic `scores` |
| Препроцессинг данных реализован в отдельном скрипте | Логика preprocessing вынесена в `fraud_detector/src/preprocessing.py` |
| Скоринг обработанного сообщения реализован отдельно | Логика загрузки модели и inference вынесена в `fraud_detector/src/scorer.py` |
| Сервис выполняет только inference | Обучение модели внутри контейнеров не выполняется |
| Inference выполняется на CPU | Используется CatBoost-модель без GPU-зависимостей |
| Проект оформлен как GitHub-репозиторий | Код проекта размещается в публичном GitHub-репозитории |
| Подготовлены requirements | У каждого Python-сервиса есть собственный `requirements.txt` |
| Подготовлен docker-compose.yml | Все сервисы поднимаются через `docker-compose.yml` |

---

### Зачёт на 4

| Требование | Реализация в проекте |
|-----------|----------------------|
| Проект загружен в GitHub | Репозиторий: `https://github.com/SvgPrizrak/mts_mlops_hw2_realtime_fraud_detector` |
| `docker-compose.yml` поднимает стабильно работающие контейнеры | Все сервисы описаны в `docker-compose.yml` |
| Имитация потока поставки данных сделана через UI | Streamlit UI позволяет загрузить `test.csv` и отправить строки в Kafka |
| Сервис читает из Kafka сообщения из файла `test.csv` | Streamlit отправляет строки файла в topic `transactions`, `fraud_detector` читает этот topic |
| Сервис выдаёт score и fraud flag | `fraud_detector` рассчитывает `score` и `fraud_flag` |
| В сервисе есть модель, и она применяется | CatBoost-модель загружается из `fraud_detector/models/my_catboost.cbm` и используется через `predict_proba` |
| PostgreSQL поднимается в той же сети | PostgreSQL описан как отдельный сервис в `docker-compose.yml` |
| Создаётся витрина для хранения идентификаторов транзакций и скоров | Таблица `scores` создаётся через `postgres/init.sql` |
| Есть дополнительный сервис записи результатов в PostgreSQL | `scoring_writer` читает topic `scores` и пишет данные в PostgreSQL |
| Topic `scores` содержит `transaction_id`, `score`, `fraud_flag` | Эти поля являются обязательными в сообщении результата скоринга |
| UI показывает 10 последних fraud-транзакций | Вкладка Streamlit `Посмотреть результаты` выводит последние записи с `fraud_flag == 1` |
| UI показывает гистограмму последних 100 скоров | Вкладка Streamlit `Посмотреть результаты` строит гистограмму `score` по последним 100 транзакциям |

Важно: по заданию для topic `scores` обязательными являются поля `transaction_id`, `score` и `fraud_flag`. В проекте дополнительно передаются и сохраняются поля `us_state`, `merch` и `cat_id`, так как они необходимы для реализации требований зачёта на 5 в Grafana.

---

### Зачёт на 5

| Требование | Реализация в проекте |
|-----------|----------------------|
| В Grafana есть фильтр по штатам (`us_state`) | Реализован фильтр `us_state` в dashboard `Realtime Fraud Detection PostgreSQL Dashboard` |
| В Grafana есть фильтр по мерчантам (`merch`) | Реализован фильтр `merch` в dashboard `Realtime Fraud Detection PostgreSQL Dashboard` |
| Пользователь может выбрать значения фильтров | Фильтры `us_state` и `merch` позволяют выбирать конкретные значения или `All` |
| График распределения скоров | Панель `Score density distribution` |
| TPS обработки транзакций | Панель `Transactions per second` |
| Barplot средней доли fraud по категории продукта | Панель `Fraud rate by product category, last 1000 transactions` |
| Barplot строится по последним 1000 транзакциям | SQL-запрос dashboard ограничивает данные последними 1000 транзакциями |

---