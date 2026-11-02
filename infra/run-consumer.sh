#!/bin/bash
# Uso:   ./run-consumer.sh single|batch|copy [--keep-data]
# Opcional (delante del comando; si no las defines, valen las del application.yml):
#   VIRTUAL_THREADS=true  VT_WORKERS=8  VT_CHUNK_SIZE=100  JDBC_POOL_SIZE=8  MAX_POLL_RECORDS=2000
# Luego: ./show-results.sh <group-id que imprime este script>
set -e

MODE="${1:?Uso: ./run-consumer.sh single|batch|copy [--keep-data]}"
case "$MODE" in single|batch|copy) ;; *) echo "mode debe ser single, batch o copy"; exit 1;; esac

HERE="$(cd "$(dirname "$0")" && pwd)"
GROUP="vehicle-telemetry-${MODE}-$(date +%H%M%S)"
if [ "${VIRTUAL_THREADS:-false}" = "true" ]; then
  GROUP="vehicle-telemetry-${MODE}-vt${VT_WORKERS:-1}-p${JDBC_POOL_SIZE:-1}-$(date +%H%M%S)"
fi

if [ "$2" != "--keep-data" ]; then
  "$HERE/truncate-data.sh"
fi

echo
echo ">>> mode=$MODE"
echo ">>> virtual-threads=${VIRTUAL_THREADS:-false}  workers=${VT_WORKERS:-1}  pool=${JDBC_POOL_SIZE:-1}  chunk=${VT_CHUNK_SIZE:-100}  poll=${MAX_POLL_RECORDS:-500}"
echo ">>> group-id=$GROUP"
echo ">>> Al terminar consumo, salir y ejecutar: ./show-results.sh $GROUP"
echo

cd "$HERE/../vehicle-telemetry-consumer"
TELEMETRY_MODE="$MODE" \
CONSUMER_GROUP_ID="$GROUP" \
./mvnw clean compile quarkus:dev