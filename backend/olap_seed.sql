CREATE TABLE IF NOT EXISTS dm_client_telemetry (
    ds date NOT NULL,
    client_id varchar(64) NOT NULL,
    segment varchar(32),
    country varchar(32),
    plan varchar(32),
    events_cnt integer,
    errors_cnt integer,
    avg_latency_ms numeric,
    p95_latency_ms numeric,
    sessions_cnt integer,
    last_event_at timestamp,
    loaded_at timestamp DEFAULT now(),
    PRIMARY KEY (ds, client_id)
);

INSERT INTO dm_client_telemetry (
    ds, client_id, segment, country, plan,
    events_cnt, errors_cnt, avg_latency_ms, p95_latency_ms,
    sessions_cnt, last_event_at
)
VALUES
    (CURRENT_DATE - INTERVAL '1 day', 'client_1', 'pro', 'US', 'gold', 1250, 7, 120.5, 250.1, 210, now() - INTERVAL '2 hours'),
    (CURRENT_DATE - INTERVAL '1 day', 'client_2', 'basic', 'DE', 'silver', 840, 2, 98.4, 200.5, 160, now() - INTERVAL '3 hours'),
    (CURRENT_DATE - INTERVAL '2 day', 'client_1', 'pro', 'US', 'gold', 1320, 5, 115.2, 240.7, 220, now() - INTERVAL '26 hours'),
    (CURRENT_DATE - INTERVAL '1 day', 'user1', 'basic', 'US', 'silver', 910, 3, 105.2, 210.4, 170, now() - INTERVAL '1 hour'),
    (CURRENT_DATE - INTERVAL '1 day', 'user2', 'basic', 'DE', 'silver', 780, 1, 99.8, 198.2, 150, now() - INTERVAL '2 hours'),
    (CURRENT_DATE - INTERVAL '1 day', 'prothetic1', 'pro', 'US', 'gold', 1400, 6, 110.0, 230.5, 240, now() - INTERVAL '90 minutes'),
    (CURRENT_DATE - INTERVAL '1 day', 'prothetic2', 'pro', 'CA', 'gold', 1320, 4, 112.3, 229.1, 230, now() - INTERVAL '100 minutes')
ON CONFLICT DO NOTHING;
