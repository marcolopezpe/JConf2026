#!/bin/bash
# Uso:   ./run-consumer.sh single|batch|copy [--keep-data]
# Luego: ./show-results.sh <group-id que imprime este script>
set -e

MODE="${1:?Uso: ./run-consumer.sh single|batch|copy [--keep-data]}"
case "$MODE" in single|batch|copy) ;; *) echo "mode debe ser single, batch o copy"; exit 1;; esac

HERE="$(cd "$(dirname "$0")" && pwd)"
GROUP="vehicle-telemetry-${MODE}-$(date +%H%M%S)"

if [ "$2" != "--keep-data" ]; then
  "$HERE/truncate-data.sh"
fi

echo
echo ">>> mode=$MODE"
echo ">>> group-id=$GROUP"
echo ">>> Al terminar consumo, salir y ejecutar: ./show-results.sh $GROUP"
echo

cd "$HERE/../vehicle-telemetry-consumer"
TELEMETRY_MODE="$MODE" \
CONSUMER_GROUP_ID="$GROUP" \
./mvnw quarkus:dev