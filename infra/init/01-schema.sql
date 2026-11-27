-- Se ejecuta automáticamente SOLO la primera vez que se crea el volumen de PostgreSQL.

CREATE TABLE IF NOT EXISTS vehicle_telemetry (
    event_id    UUID             PRIMARY KEY,
    vehicle_id  VARCHAR(50)      NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    speed_kmh   DOUBLE PRECISION NOT NULL,
    occurred_at TIMESTAMPTZ      NOT NULL,
    received_at TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vehicle_telemetry_vehicle_time
    ON vehicle_telemetry (vehicle_id, occurred_at);

CREATE TABLE IF NOT EXISTS benchmark_run (
    id                           BIGSERIAL     PRIMARY KEY,
    strategy                     VARCHAR(30)   NOT NULL,
    total_events                 INTEGER       NOT NULL,
    persisted_events             INTEGER       NOT NULL,
    duplicated_events            INTEGER       NOT NULL DEFAULT 0,
    failed_events                INTEGER       NOT NULL DEFAULT 0,
    workers                      INTEGER       NOT NULL,
    jdbc_pool_size               INTEGER       NOT NULL,
    started_at                   TIMESTAMPTZ   NOT NULL,
    finished_at                  TIMESTAMPTZ   NOT NULL,
    duration_ms                  BIGINT        NOT NULL,
    throughput_events_per_second NUMERIC(12,2) NOT NULL,
    virtual_threads              BOOLEAN       NOT NULL DEFAULT FALSE,
    chunk_size                   INTEGER       NOT NULL DEFAULT 0   -- 0 = no aplica (sin Virtual Threads)
);