# Airflow DAG для ETL-процесса CRM и телеметрии

Этот DAG реализует ETL-процесс для извлечения данных из CRM-системы и телеметрии, их загрузки в OLAP-базу и построения витрины данных.

## Как запускается DAG?

**DAG запускается автоматически Apache Airflow** — это оркестратор задач для планирования и выполнения ETL-процессов.

### Процесс запуска:

1. **Автоматическое обнаружение**: Airflow Scheduler автоматически сканирует папку `dags/` и обнаруживает все Python-файлы с DAG'ами
2. **Планирование**: Scheduler проверяет расписание (`schedule="0 2 * * *"` — ежедневно в 02:00) и создаёт задачи для выполнения
3. **Выполнение**: Executor запускает задачи в соответствии с зависимостями
4. **Мониторинг**: Web UI позволяет отслеживать статус выполнения задач

### Важно:

- DAG **не запускается напрямую** как обычный Python-скрипт (`python crm_telemetry_mart_dag.py` не сработает)
- DAG должен находиться в папке `dags/` Airflow
- Airflow должен быть запущен (webserver + scheduler)

## Структура

```
airflow/
├── dags/
│   └── crm_telemetry_mart_dag.py  # Основной DAG
├── sql/
│   └── create_olap_tables.sql     # SQL-скрипты для создания таблиц
└── README.md                       # Документация
```

## Описание процесса

### 1. Извлечение данных (Extract)

- **extract_crm**: Извлекает данные о клиентах из CRM-системы через REST API
- **extract_telemetry**: Извлекает данные телеметрии из базы данных за предыдущий день

### 2. Загрузка в стейджинг (Load to Staging)

- **load_crm_to_olap**: Загружает данные CRM в стейджинг-таблицу `stg_crm_clients`
- **load_telemetry_to_olap**: Загружает данные телеметрии в стейджинг-таблицу `stg_telemetry`

### 3. Загрузка в ODS (Operational Data Store)

- **merge_crm_to_ods**: Объединяет данные CRM в ODS с использованием UPSERT
- **load_telemetry_to_ods**: Загружает телеметрию в ODS с партиционированием по дате

### 4. Построение витрины (Build Mart)

- **build_mart**: Агрегирует данные телеметрии по клиентам и объединяет с данными CRM в витрину `dm_client_telemetry`

## Расписание

DAG запускается **ежедневно в 02:00** и обрабатывает данные за предыдущий день.

## Быстрый старт с Docker Compose

Airflow уже настроен в `docker-compose.yaml`. Для запуска:

```bash
# Запуск всего стека включая Airflow
docker-compose up -d

# Airflow Web UI будет доступен на http://localhost:8081
# Логин: admin, Пароль: admin
```

После запуска:
1. Откройте http://localhost:8081
2. Найдите DAG `crm_telemetry_mart`
3. Включите DAG (переключатель слева)
4. DAG будет запускаться автоматически по расписанию (02:00) или можно запустить вручную

## Настройка

### 1. Airflow Connections

Создайте следующие подключения в Airflow через Web UI (Admin → Connections) или CLI:

**Через Web UI:**
1. Откройте http://localhost:8081 → Admin → Connections
2. Добавьте подключение `telemetry_db` (если есть отдельная БД телеметрии)
3. Добавьте подключение `olap_db`:
   - Connection Id: `olap_db`
   - Connection Type: `Postgres`
   - Host: `olap_db` (имя сервиса в docker-compose)
   - Schema: `olap_db`
   - Login: `olap_user`
   - Password: `olap_password`
   - Port: `5432`

**Через CLI (внутри контейнера):**
```bash
docker-compose exec airflow-webserver airflow connections add olap_db \
    --conn-type postgres \
    --conn-host olap_db \
    --conn-login olap_user \
    --conn-password olap_password \
    --conn-schema olap_db \
    --conn-port 5432
```

### 2. Airflow Variables

Установите переменные для доступа к CRM API через Web UI (Admin → Variables) или CLI:

**Через Web UI:**
1. Откройте http://localhost:8081 → Admin → Variables
2. Добавьте переменные:
   - Key: `CRM_API_URL`, Value: `http://crm-api:8080`
   - Key: `CRM_API_TOKEN`, Value: `your-api-token`

**Через CLI:**
```bash
docker-compose exec airflow-webserver airflow variables set CRM_API_URL "http://crm-api:8080"
docker-compose exec airflow-webserver airflow variables set CRM_API_TOKEN "your-api-token"
```

### 3. Создание таблиц в OLAP

Выполните SQL-скрипт для создания необходимых таблиц:

```bash
psql -h olap-db-host -U olap_user -d olap_db -f sql/create_olap_tables.sql
```

## Структура витрины данных

Таблица `dm_client_telemetry` содержит следующие поля:

- `ds` - Дата среза данных
- `client_id` - Идентификатор клиента
- `segment` - Сегмент клиента
- `country` - Страна
- `plan` - Тарифный план
- `events_cnt` - Количество событий
- `errors_cnt` - Количество ошибок
- `avg_latency_ms` - Средняя задержка (мс)
- `p95_latency_ms` - 95-й перцентиль задержки (мс)
- `sessions_cnt` - Количество сессий
- `last_event_at` - Время последнего события
- `loaded_at` - Время загрузки данных

## Зависимости

Убедитесь, что установлены следующие Python-пакеты:

```txt
apache-airflow>=2.7.0
apache-airflow-providers-postgres>=5.0.0
pandas>=2.0.0
requests>=2.31.0
```

## Мониторинг

- Проверяйте логи задач в Airflow UI
- Настройте алерты на неудачные выполнения
- Мониторьте размер данных в витрине

## Примеры использования

### Ручной запуск DAG

**Через Web UI:**
1. Откройте DAG `crm_telemetry_mart`
2. Нажмите кнопку "Play" (▶️) → "Trigger DAG"

**Через CLI:**
```bash
docker-compose exec airflow-webserver airflow dags trigger crm_telemetry_mart
```

### Запуск конкретной задачи

```bash
docker-compose exec airflow-webserver airflow tasks run crm_telemetry_mart extract_crm 2024-12-08
```

### Просмотр логов

**Через Web UI:**
1. Откройте DAG → выберите запуск → выберите задачу → нажмите "Log"

**Через CLI:**
```bash
docker-compose exec airflow-webserver airflow tasks logs crm_telemetry_mart extract_crm 2024-12-08
```

### Тестирование подключений

Создайте тестовый DAG или используйте Airflow CLI:

```bash
# Проверка подключения к OLAP
docker-compose exec airflow-webserver airflow connections test olap_db
```
