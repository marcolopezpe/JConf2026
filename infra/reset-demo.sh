#!/bin/bash
# Deja el entorno limpio para repetir un benchmark SIN reiniciar los contenedores:
#   - TRUNCATE de la tabla de telemetria
#   - recrea el topic principal y la DLQ
#   - elimina el subject del Schema Registry
#   - vuelve a registrar el schema Avro desde infra/schema/
# Despues de ejecutarlo, reinicia el producer y usa un group-id NUEVO en el consumer.

TOPIC="vehicle-telemetry-topic"
DLQ_TOPIC="vehicle-telemetry-dlq"
SUBJECT="${TOPIC}-value"
REGISTRY="http://localhost:8081"
SCHEMA_FILE="schema/vehicle-telemetry.avsc"

# Ejecuta desde la carpeta del script (infra/), se llame desde donde se llame
cd "$(dirname "$0")" || exit 1

if [ ! -f "$SCHEMA_FILE" ]; then
  echo "ERROR: no se encontro $SCHEMA_FILE (debe estar en infra/schema/)"
  exit 1
fi

echo '>> TRUNCATE vehicle_telemetry'
docker exec postgres psql -U postgres -d db_vehicle_telemetry -c "TRUNCATE TABLE vehicle_telemetry;"

echo '>> Recreando topics'
docker exec redpanda rpk topic delete "$TOPIC" "$DLQ_TOPIC" --brokers localhost:29092 || true
docker exec redpanda rpk topic create "$TOPIC" --partitions 3 --replicas 1 --brokers localhost:29092
docker exec redpanda rpk topic create "$DLQ_TOPIC" --partitions 1 --replicas 1 --brokers localhost:29092

echo '>> Eliminando subject del Schema Registry (soft + permanent)'
curl -s -X DELETE "$REGISTRY/subjects/$SUBJECT" ; echo
curl -s -X DELETE "$REGISTRY/subjects/$SUBJECT?permanent=true" ; echo

echo '>> Registrando el schema Avro'
# MSYS_NO_PATHCONV=1: evita que Git Bash (Windows) convierta /tmp/... en una ruta de Windows.
# En macOS y Linux no hace nada.
MSYS_NO_PATHCONV=1 docker cp "$SCHEMA_FILE" redpanda:/tmp/vehicle-telemetry.avsc
if ! MSYS_NO_PATHCONV=1 docker exec redpanda rpk registry schema create "$SUBJECT" \
      --schema /tmp/vehicle-telemetry.avsc --type avro; then
  echo "ERROR: no se pudo registrar el schema"
  exit 1
fi

echo '>> Subjects registrados:'
docker exec redpanda rpk registry subject list

echo '>> Listo. Reinicia el producer y usa un group-id nuevo en el consumer.'