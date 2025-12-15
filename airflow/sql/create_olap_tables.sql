-- SQL-скрипт для создания таблиц в OLAP-базе данных
-- Используется в процессе ETL для загрузки данных из CRM и телеметрии

-- Стейджинг-таблица для данных CRM
CREATE TABLE IF NOT EXISTS stg_crm_clients (
    client_id VARCHAR(64) NOT NULL,
    segment VARCHAR(32),
    country VARCHAR(32),
    plan VARCHAR(32),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    load_date DATE NOT NULL,
    PRIMARY KEY (client_id, load_date)
);

-- Стейджинг-таблица для данных телеметрии
CREATE TABLE IF NOT EXISTS stg_telemetry (
    client_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64),
    duration_ms NUMERIC,
    status VARCHAR(32),
    session_id VARCHAR(128),
    event_ts TIMESTAMP,
    load_date DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_telemetry_client_date 
    ON stg_telemetry(client_id, load_date);

-- ODS (Operational Data Store) - таблица клиентов
CREATE TABLE IF NOT EXISTS ods_crm_clients (
    client_id VARCHAR(64) PRIMARY KEY,
    segment VARCHAR(32),
    country VARCHAR(32),
    plan VARCHAR(32),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- ODS - таблица телеметрии
CREATE TABLE IF NOT EXISTS ods_telemetry (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64),
    duration_ms NUMERIC,
    status VARCHAR(32),
    session_id VARCHAR(128),
    event_ts TIMESTAMP NOT NULL,
    event_date DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ods_telemetry_client_date 
    ON ods_telemetry(client_id, event_date);
CREATE INDEX IF NOT EXISTS idx_ods_telemetry_date 
    ON ods_telemetry(event_date);

-- Витрина данных (Data Mart) - финальная таблица для отчётов
CREATE TABLE IF NOT EXISTS dm_client_telemetry (
    ds DATE NOT NULL,
    client_id VARCHAR(64) NOT NULL,
    segment VARCHAR(32),
    country VARCHAR(32),
    plan VARCHAR(32),
    events_cnt INTEGER,
    errors_cnt INTEGER,
    avg_latency_ms NUMERIC,
    p95_latency_ms NUMERIC,
    sessions_cnt INTEGER,
    last_event_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (ds, client_id)
);

-- Индексы для быстрого доступа к данным витрины
CREATE INDEX IF NOT EXISTS idx_dm_client_telemetry_client 
    ON dm_client_telemetry(client_id);
CREATE INDEX IF NOT EXISTS idx_dm_client_telemetry_ds 
    ON dm_client_telemetry(ds);

-- Комментарии к таблицам
COMMENT ON TABLE stg_crm_clients IS 'Стейджинг-таблица для временного хранения данных CRM перед загрузкой в ODS';
COMMENT ON TABLE stg_telemetry IS 'Стейджинг-таблица для временного хранения данных телеметрии';
COMMENT ON TABLE ods_crm_clients IS 'ODS: операционное хранилище данных о клиентах из CRM';
COMMENT ON TABLE ods_telemetry IS 'ODS: операционное хранилище данных телеметрии';
COMMENT ON TABLE dm_client_telemetry IS 'Витрина данных: агрегированная аналитика по клиентам и телеметрии';


