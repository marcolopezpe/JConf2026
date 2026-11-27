#!/bin/bash
# Opcional: cuando compare-workers.sh NO muestra una corrida (por ejemplo la de
# 100 o 200 workers de la Prueba 4), o muestra cifras raras.
# Muestra TODAS las corridas guardadas, dice por que compare-workers oculta
# cada una y revisa lo que no se ve a simple vista: eventos acumulados en el
# topico, basura en la DLQ, filas dobles y errores de conexion en PostgreSQL.
# No cambia nada; solo mira.
#
# Uso (desde infra/):  sh ver-corridas.sh            # ultimas 20 corridas
#                      sh ver-corridas.sh 50         # ultimas 50

N="${1:-20}"
DB="db_vehicle_telemetry"
TOPIC="vehicle-telemetry-topic"
DLQ="vehicle-telemetry-dlq"
PSQL="docker exec postgres psql -U postgres -d $DB -P pager=off"
Q="$PSQL -t -A -c"

if ! $Q "SELECT 1" >/dev/null 2>&1; then
  echo "ERROR: no pude conectarme a PostgreSQL. ¿Esta arriba? (sh start-infra.sh)"; exit 1
fi

echo "=================================================================="
echo " 1. Todas las corridas (ultimas $N), tal como estan en benchmark_run"
echo "=================================================================="
$PSQL -c "SELECT * FROM (SELECT * FROM benchmark_run ORDER BY 1 DESC LIMIT $N) t ORDER BY 1;"

echo "=================================================================="
echo " 2. Por que compare-workers.sh oculta o marca cada corrida"
echo "    (solo muestra total=10000 y persisted>0)"
echo "=================================================================="
# Se lee cada fila como JSON: si una columna no existe, sale vacia en vez de fallar.
$PSQL -c "
SELECT j->>'id' AS id,
       j->>'total_events' AS total, j->>'persisted' AS persisted,
       j->>'duplicated' AS duplicated, j->>'failed' AS failed,
       CASE
         WHEN j->>'total_events' IS NULL THEN 'no encuentro la columna total_events'
         WHEN (j->>'total_events')::bigint < 10000
           THEN 'OCULTA: leyo menos de 10,000. Probable corte por 5 s sin mensajes (idle-timeout-ms)'
         WHEN (j->>'total_events')::bigint > 10000
           THEN 'OCULTA: leyo mas de 10,000. El topico tenia eventos acumulados (lag.sh, caida de PG o dlq.sh)'
         WHEN coalesce((j->>'persisted')::bigint,0) = 0
           THEN 'OCULTA: 0 guardados. La tabla ya tenia los eventos: solo hubo duplicados'
         WHEN coalesce((j->>'duplicated')::bigint,0) > 0
           THEN 'VISIBLE, pero repitio trabajo: duplicados con la tabla vacia (reintentos)'
         WHEN coalesce((j->>'failed')::bigint,0) > 0
           THEN 'VISIBLE, pero hay basura en el topico (fallidos a la DLQ)'
         ELSE 'OK'
       END AS diagnostico
FROM (SELECT to_jsonb(b) j FROM benchmark_run b
      ORDER BY (to_jsonb(b)->>'id')::bigint DESC NULLS LAST LIMIT $N) t
ORDER BY (j->>'id')::bigint NULLS LAST;"

echo "=================================================================="
echo " 3. Mas alla de lo evidente"
echo "=================================================================="
hw() {  # suma del HIGH-WATERMARK de todas las particiones = mensajes en el topico
  docker exec redpanda rpk topic describe "$1" -p --brokers localhost:29092 2>/dev/null |
    awk 'NR==1{for(i=1;i<=NF;i++) if($i=="HIGH-WATERMARK") c=i; next} c{s+=$c} END{print s+0}'
}
T=$(hw "$TOPIC"); D=$(hw "$DLQ")
echo "  Mensajes en $TOPIC: $T"
if [ "$T" -gt 10000 ] 2>/dev/null; then
  echo "    -> Hay mas de 10,000: cada corrida nueva lee TODO esto y compare-workers no la muestra."
  echo "       Para medir de nuevo: 'Antes de subir' del README (sh reset-demo.sh ...)."
elif [ "$T" -lt 10000 ] 2>/dev/null; then
  echo "    -> Hay menos de 10,000: falta la Prueba 1 (curl del load-test)."
fi
echo "  Mensajes en la DLQ $DLQ: $D"
[ "$D" -gt 0 ] 2>/dev/null && echo "    -> Hay basura de la Prueba 8: sh reset-demo.sh antes de volver a medir."

echo
echo "  Tabla vehicle_telemetry (filas, event_id distintos):"
$PSQL -c "SELECT count(*) AS filas, count(DISTINCT event_id) AS distintos,
                 count(*) - count(DISTINCT event_id) AS filas_dobles
          FROM vehicle_telemetry;"
echo "    -> filas_dobles debe ser 0: ON CONFLICT impide guardar dos veces el mismo evento."

echo "  Conexiones a PostgreSQL ahora y el limite:"
$PSQL -c "SELECT (SELECT count(*) FROM pg_stat_activity WHERE datname='$DB') AS conexiones_ahora,
                 current_setting('max_connections') AS max_connections;"
echo "    -> Si pides un pool mayor que max_connections, aparece 'too many clients already'."

echo "  Errores de conexion en el log de PostgreSQL (ultimos 60 min):"
ERR=$(docker logs postgres --since 60m 2>&1 | grep -i -E "too many clients|remaining connection slots|FATAL" | tail -5)
if [ -z "$ERR" ]; then echo "    (ninguno)"
else echo "$ERR" | sed 's/^/    /'
     echo "    -> Esto explica reintentos y duplicados en la corrida de muchos workers."
fi

echo
echo "  En el log del consumer (T1) busca tambien: 'acquisition timeout' o 'Unable to acquire JDBC Connection'."