#!/bin/bash
# Uso: ./show-results.sh [group-id]     (por defecto vehicle-telemetry-single-cg)
GROUP="${1:-vehicle-telemetry-single-cg}"
PSQL="docker exec postgres psql -U postgres -d db_vehicle_telemetry"

echo
echo "=== 1) Filas en vehicle_telemetry ==="
$PSQL -c "SELECT count(*) AS filas, count(DISTINCT vehicle_id) AS vehiculos FROM vehicle_telemetry;"

echo "=== 2) Corridas medidas (benchmark_run) ==="
$PSQL -c "
SELECT id,
       strategy,
       total_events      AS total,
       persisted_events  AS persistidos,
       duplicated_events AS duplicados,
       failed_events     AS fallidos,
       workers,
       jdbc_pool_size    AS pool,
       duration_ms       AS ms,
       throughput_events_per_second AS ev_por_seg,
       round(duration_ms::numeric / NULLIF(total_events, 0), 3) AS ms_por_evento,
       round(throughput_events_per_second /
             NULLIF(avg(throughput_events_per_second) FILTER (WHERE strategy = 'single') OVER (), 0), 2)
             AS veces_vs_single
FROM benchmark_run
ORDER BY id;"

echo "=== 3) Lag del consumer group: $GROUP ==="
docker exec redpanda rpk group describe "$GROUP" --brokers localhost:29092