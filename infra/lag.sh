#!/bin/bash
# Uso: sh lag.sh <group-id>
# Mide el lag del consumer, a los 3 s publica 10,000 eventos y muestra la curva.
GRUPO="${1:?Uso: sh lag.sh <group-id>   (el que imprime run-consumer.sh en >>> group-id=...)}"
OUT="$(mktemp)"

( for i in $(seq 1 80); do
    echo "$(date +%T) $(docker exec redpanda rpk group describe "$GRUPO" --brokers localhost:29092 | grep '^TOTAL-LAG')"
    sleep 0.3
  done > "$OUT" ) &
PID=$!

sleep 3
curl -s -X POST localhost:8001/telemetry/load-test/10000; echo
wait $PID
cat "$OUT"
rm -f "$OUT"