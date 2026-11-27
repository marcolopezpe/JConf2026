#!/bin/bash
# Tabla del experimento de Virtual Threads: una fila por (estrategia, workers, pool, vt, chunk).
# Solo corridas VALIDAS: persisted_events > 0 y el mismo total_events.
# "veces_vs_base" compara contra la base de la MISMA estrategia: workers=1, pool=1, sin Virtual Threads.
#
# Uso: ./compare-workers.sh [total_events]     (por defecto 10000)
TOTAL="${1:-10000}"
case "$TOTAL" in ''|*[!0-9]*) echo "total_events debe ser un numero"; exit 1;; esac

docker exec postgres psql -U postgres -d db_vehicle_telemetry -c "
WITH valid AS (
    SELECT * FROM benchmark_run
    WHERE persisted_events > 0 AND total_events = $TOTAL
),
agg AS (
    SELECT strategy, workers, jdbc_pool_size AS pool, virtual_threads AS vt, chunk_size AS chunk,
           count(*)                           AS corridas,
           avg(throughput_events_per_second)  AS tp,
           min(throughput_events_per_second)  AS tp_min,
           max(throughput_events_per_second)  AS tp_max,
           avg(duration_ms)                   AS dur
    FROM valid
    GROUP BY strategy, workers, jdbc_pool_size, virtual_threads, chunk_size
)
SELECT a.strategy                 AS estrategia,
       a.workers,
       a.pool,
       a.vt,
       a.chunk,
       a.corridas,
       round(a.tp, 2)             AS ev_por_seg_prom,
       round(a.tp_min, 2)         AS minimo,
       round(a.tp_max, 2)         AS maximo,
       round(a.dur)               AS duracion_ms,
       round(a.tp / NULLIF(b.tp, 0), 2) AS veces_vs_base
FROM agg a
LEFT JOIN agg b ON b.strategy = a.strategy AND b.workers = 1 AND b.pool = 1 AND b.vt = FALSE
ORDER BY a.strategy, a.vt, a.workers, a.pool, a.chunk;"