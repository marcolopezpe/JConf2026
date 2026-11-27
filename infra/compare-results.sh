#!/bin/bash
# Resumen por estrategia, listo para mostrar al publico.
# Solo cuenta corridas VALIDAS: persisted_events > 0 (excluye la de idempotencia)
# y el mismo volumen de eventos, para comparar peras con peras.
#
# Uso: ./compare-results.sh [total_events]     (por defecto 10000)
TOTAL="${1:-10000}"
case "$TOTAL" in ''|*[!0-9]*) echo "total_events debe ser un numero"; exit 1;; esac

docker exec postgres psql -U postgres -d db_vehicle_telemetry -c "
WITH valid AS (
    SELECT * FROM benchmark_run
    WHERE persisted_events > 0 AND total_events = $TOTAL
),
agg AS (
    SELECT strategy,
           count(*)                                      AS corridas,
           avg(throughput_events_per_second)             AS tp,
           min(throughput_events_per_second)             AS tp_min,
           max(throughput_events_per_second)             AS tp_max,
           avg(duration_ms)                              AS dur,
           avg(duration_ms::numeric / total_events)      AS mspe
    FROM valid
    GROUP BY strategy
)
SELECT strategy                    AS estrategia,
       corridas,
       round(tp, 2)                AS ev_por_seg_prom,
       round(tp_min, 2)            AS minimo,
       round(tp_max, 2)            AS maximo,
       round(dur)                  AS duracion_ms,
       round(mspe, 3)              AS ms_por_evento,
       round(tp / NULLIF((SELECT tp FROM agg WHERE strategy = 'single'), 0), 1) AS veces_vs_single
FROM agg
ORDER BY tp;"