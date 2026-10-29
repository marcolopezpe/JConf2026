#!/bin/bash
# Deja el entorno limpio para repetir un benchmark SIN reiniciar los contenedores:
#   - TRUNCATE de la tabla de telemetria
#   - recrea el topic principal y la DLQ
#   - elimina el subject del Schema Registry
# Despues de ejecutarlo, usa un group-id NUEVO en el consumer.

TOPIC="vehicle-telemetry-topic"
DLQ_TOPIC="vehicle-telemetry-dlq"
SUBJECT="${TOPIC}-value"
REGISTRY="http://localhost:8081"

echo '>> TRUNCATE vehicle_telemetry'
docker exec postgres psql -U postgres -d db_vehicle_telemetry -c "TRUNCATE TABLE vehicle_telemetry;"

echo '>> Recreando topics'
docker exec redpanda rpk topic delete "$TOPIC" "$DLQ_TOPIC" --brokers localhost:29092 || true
docker exec redpanda rpk topic create "$TOPIC" --partitions 3 --replicas 1 --brokers localhost:29092
docker exec redpanda rpk topic create "$DLQ_TOPIC" --partitions 1 --replicas 1 --brokers localhost:29092

echo '>> Eliminando subject del Schema Registry (soft + permanent)'
curl -s -X DELETE "$REGISTRY/subjects/$SUBJECT" ; echo
curl -s -X DELETE "$REGISTRY/subjects/$SUBJECT?permanent=true" ; echo

echo '>> Listo. Recuerda cambiar el group-id del consumer.'