#!/bin/bash
# Semaforo antes de subir al escenario: revisa que la demo arranque desde cero.
# No cambia nada; solo mira. Uso (desde infra/):  sh check-demo.sh
# Si algo sale en rojo, la linea dice que ejecutar.

TOPIC="vehicle-telemetry-topic"
SUBJECT="${TOPIC}-value"
DB="db_vehicle_telemetry"
OK=0; FAIL=0
ok()   { echo "  [OK]    $1"; OK=$((OK+1)); }
fail() { echo "  [FALTA] $1"; echo "          -> $2"; FAIL=$((FAIL+1)); }

echo ">> Revisando la demo..."

# 1. Contenedores
for c in redpanda postgres; do
  if docker ps --format '{{.Names}}' | grep -qx "$c"; then ok "contenedor $c arriba"
  else fail "contenedor $c no esta corriendo" "sh start-infra.sh"; fi
done

# 2. Schema Avro registrado
if docker exec redpanda rpk registry subject list 2>/dev/null | grep -q "$SUBJECT"; then ok "schema $SUBJECT registrado"
else fail "schema Avro no registrado" "sh reset-demo.sh"; fi

# 3. Topico existe y esta vacio (high watermark 0 en todas las particiones)
DESC=$(docker exec redpanda rpk topic describe "$TOPIC" -p --brokers localhost:29092 2>/dev/null)
if [ -z "$DESC" ]; then
  fail "topico $TOPIC no existe" "sh reset-demo.sh"
else
  MSGS=$(echo "$DESC" | awk 'NR==1{for(i=1;i<=NF;i++) if($i=="HIGH-WATERMARK") c=i; next} c{s+=$c} END{print s+0}')
  if [ "$MSGS" = "0" ]; then ok "topico $TOPIC vacio"
  else fail "topico $TOPIC ya tiene $MSGS mensajes" "sh reset-demo.sh (y reinicia el producer)"; fi
fi

# 4. Tablas vacias
PSQL="docker exec postgres psql -U postgres -d $DB -t -A -c"
N=$($PSQL "SELECT count(*) FROM vehicle_telemetry;" 2>/dev/null)
if [ "$N" = "0" ]; then ok "tabla vehicle_telemetry vacia"
elif [ -z "$N" ]; then fail "no pude leer vehicle_telemetry" "sh stop-infra.sh y sh start-infra.sh"
else fail "vehicle_telemetry tiene $N filas" "sh reset-demo.sh"; fi

R=$($PSQL "SELECT count(*) FROM benchmark_run;" 2>/dev/null)
if [ "$R" = "0" ]; then ok "comparacion (benchmark_run) vacia"
elif [ -z "$R" ]; then fail "no pude leer benchmark_run" "sh stop-infra.sh y sh start-infra.sh"
else fail "benchmark_run tiene $R corridas viejas" "sh reset-results.sh -y"; fi

# 5. Producer escuchando
if curl -s -o /dev/null --max-time 3 http://localhost:8001/; then ok "producer responde en el puerto 8001"
else fail "producer no responde en el puerto 8001" "en T0: cd vehicle-telemetry-producer && ./mvnw quarkus:dev"; fi

echo
if [ "$FAIL" = "0" ]; then echo ">> TODO LISTO ($OK/$OK). Puedes empezar: la Prueba 1 genera los eventos."
else echo ">> FALTAN $FAIL cosas. Corrige lo de arriba y vuelve a ejecutar sh check-demo.sh"; exit 1; fi
