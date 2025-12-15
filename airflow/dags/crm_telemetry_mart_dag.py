"""
Airflow DAG для ETL-процесса:
1. Извлечение данных из CRM-системы
2. Извлечение данных телеметрии
3. Загрузка данных в OLAP-базу
4. Построение витрины данных (dm_client_telemetry)

Расписание: ежедневно в 02:00
"""
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
import requests


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# Расписание: ежедневно в 02:00
@dag(
    dag_id="crm_telemetry_mart",
    start_date=datetime(2024, 12, 1),
    schedule="0 2 * * *",  # Ежедневно в 02:00
    catchup=False,  # Не запускать пропущенные DAG'и
    max_active_runs=1,  # Только один активный запуск
    default_args=DEFAULT_ARGS,
    tags=["etl", "crm", "telemetry", "mart"],
    description="ETL-процесс для загрузки данных из CRM и телеметрии в витрину dm_client_telemetry",
)
def crm_telemetry_mart():
    """
    Основной DAG для построения витрины данных клиентов с телеметрией.
    """

    @task
    def extract_crm(**context) -> list[dict[str, Any]]:
        """
        Извлекает данные о клиентах из CRM-системы.
        Использует execution_date для инкрементальной загрузки.
        """
        execution_date = context["execution_date"]
        # Загружаем данные за предыдущий день
        since = (execution_date - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
        
        # Получаем настройки из Airflow Variables
        crm_api_url = Variable.get("CRM_API_URL", default_var="http://crm-api:8080")
        crm_api_token = Variable.get("CRM_API_TOKEN", default_var="")
        
        try:
            response = requests.get(
                f"{crm_api_url}/api/clients",
                params={"updated_from": since},
                headers={"Authorization": f"Bearer {crm_api_token}"},
                timeout=30,
            )
            response.raise_for_status()
            clients = response.json().get("clients", [])
            
            print(f"Извлечено {len(clients)} клиентов из CRM")
            return clients
        except Exception as e:
            print(f"Ошибка при извлечении данных из CRM: {e}")
            raise

    @task
    def extract_telemetry(**context) -> list[dict[str, Any]]:
        """
        Извлекает данные телеметрии за предыдущий день.
        """
        execution_date = context["execution_date"]
        ds = (execution_date - timedelta(days=1)).date()
        
        # Подключение к источнику телеметрии (пример: PostgreSQL)
        telemetry_hook = PostgresHook(postgres_conn_id="telemetry_db")
        
        query = """
            SELECT 
                client_id,
                event_type,
                duration_ms,
                status,
                session_id,
                event_ts
            FROM telemetry_events
            WHERE event_date = %s
        """
        
        try:
            df = telemetry_hook.get_pandas_df(query, parameters=[ds])
            records = df.to_dict(orient="records")
            print(f"Извлечено {len(records)} событий телеметрии за {ds}")
            return records
        except Exception as e:
            print(f"Ошибка при извлечении телеметрии: {e}")
            raise

    @task
    def load_crm_to_olap(crm_data: list[dict[str, Any]]) -> None:
        """
        Загружает данные CRM в стейджинг-таблицу OLAP.
        """
        if not crm_data:
            print("Нет данных CRM для загрузки")
            return
        
        olap_hook = PostgresHook(postgres_conn_id="olap_db")
        
        # Очистка стейджинга для текущей даты
        execution_date = datetime.now().date()
        olap_hook.run(
            "DELETE FROM stg_crm_clients WHERE load_date = %s",
            parameters=[execution_date],
        )
        
        # Вставка данных
        insert_query = """
            INSERT INTO stg_crm_clients (
                client_id, segment, country, plan, 
                created_at, updated_at, load_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = [
            (
                client["id"],
                client.get("segment"),
                client.get("country"),
                client.get("plan"),
                client.get("created_at"),
                client.get("updated_at"),
                execution_date,
            )
            for client in crm_data
        ]
        
        olap_hook.insert_rows(
            table="stg_crm_clients",
            rows=rows,
            target_fields=[
                "client_id", "segment", "country", "plan",
                "created_at", "updated_at", "load_date"
            ],
            commit_every=1000,
        )
        
        print(f"Загружено {len(rows)} записей CRM в стейджинг")

    @task
    def load_telemetry_to_olap(telemetry_data: list[dict[str, Any]]) -> None:
        """
        Загружает данные телеметрии в стейджинг-таблицу OLAP.
        """
        if not telemetry_data:
            print("Нет данных телеметрии для загрузки")
            return
        
        olap_hook = PostgresHook(postgres_conn_id="olap_db")
        
        execution_date = datetime.now().date()
        olap_hook.run(
            "DELETE FROM stg_telemetry WHERE load_date = %s",
            parameters=[execution_date],
        )
        
        insert_query = """
            INSERT INTO stg_telemetry (
                client_id, event_type, duration_ms, status,
                session_id, event_ts, load_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = [
            (
                event["client_id"],
                event.get("event_type"),
                event.get("duration_ms"),
                event.get("status"),
                event.get("session_id"),
                event.get("event_ts"),
                execution_date,
            )
            for event in telemetry_data
        ]
        
        olap_hook.insert_rows(
            table="stg_telemetry",
            rows=rows,
            target_fields=[
                "client_id", "event_type", "duration_ms", "status",
                "session_id", "event_ts", "load_date"
            ],
            commit_every=5000,
        )
        
        print(f"Загружено {len(rows)} событий телеметрии в стейджинг")

    @task
    def merge_crm_to_ods() -> None:
        """
        Объединяет данные из стейджинга в ODS (Operational Data Store).
        Использует UPSERT для обновления существующих записей.
        """
        olap_hook = PostgresHook(postgres_conn_id="olap_db")
        
        merge_query = """
            INSERT INTO ods_crm_clients (
                client_id, segment, country, plan, created_at, updated_at
            )
            SELECT DISTINCT
                client_id, segment, country, plan, created_at, updated_at
            FROM stg_crm_clients
            WHERE load_date = CURRENT_DATE
            ON CONFLICT (client_id) 
            DO UPDATE SET
                segment = EXCLUDED.segment,
                country = EXCLUDED.country,
                plan = EXCLUDED.plan,
                updated_at = EXCLUDED.updated_at;
        """
        
        olap_hook.run(merge_query)
        print("Данные CRM объединены в ODS")

    @task
    def load_telemetry_to_ods() -> None:
        """
        Загружает телеметрию в ODS с партиционированием по дате.
        """
        olap_hook = PostgresHook(postgres_conn_id="olap_db")
        
        load_query = """
            INSERT INTO ods_telemetry (
                client_id, event_type, duration_ms, status,
                session_id, event_ts, event_date
            )
            SELECT
                client_id, event_type, duration_ms, status,
                session_id, event_ts, DATE(event_ts) as event_date
            FROM stg_telemetry
            WHERE load_date = CURRENT_DATE;
        """
        
        olap_hook.run(load_query)
        print("Данные телеметрии загружены в ODS")

    build_mart = PostgresOperator(
        task_id="build_mart",
        postgres_conn_id="olap_db",
        sql="""
            -- Удаление данных за текущую дату перед пересчётом
            DELETE FROM dm_client_telemetry 
            WHERE ds = CURRENT_DATE - INTERVAL '1 day';
            
            -- Построение витрины данных
            INSERT INTO dm_client_telemetry (
                ds, client_id, segment, country, plan,
                events_cnt, errors_cnt, avg_latency_ms, p95_latency_ms,
                sessions_cnt, last_event_at, loaded_at
            )
            SELECT
                CURRENT_DATE - INTERVAL '1 day' AS ds,
                c.client_id,
                c.segment,
                c.country,
                c.plan,
                COUNT(*) AS events_cnt,
                COUNT(*) FILTER (WHERE t.status = 'error') AS errors_cnt,
                AVG(t.duration_ms) AS avg_latency_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY t.duration_ms) AS p95_latency_ms,
                COUNT(DISTINCT t.session_id) AS sessions_cnt,
                MAX(t.event_ts) AS last_event_at,
                NOW() AS loaded_at
            FROM ods_telemetry t
            INNER JOIN ods_crm_clients c ON c.client_id = t.client_id
            WHERE t.event_date = CURRENT_DATE - INTERVAL '1 day'
            GROUP BY c.client_id, c.segment, c.country, c.plan;
        """,
    )

    # Определение зависимостей задач
    crm_data = extract_crm()
    telemetry_data = extract_telemetry()
    
    load_crm = load_crm_to_olap(crm_data)
    load_telemetry = load_telemetry_to_olap(telemetry_data)
    
    merge_crm = merge_crm_to_ods()
    load_tel_ods = load_telemetry_to_ods()
    
    # Параллельная загрузка в стейджинг
    [load_crm, load_telemetry] >> [merge_crm, load_tel_ods] >> build_mart


# Создание экземпляра DAG
dag_instance = crm_telemetry_mart()
