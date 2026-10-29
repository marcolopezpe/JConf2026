#!/bin/bash
set -e

TOPIC="vehicle-telemetry-topic"
DLQ_TOPIC="vehicle-telemetry-dlq"

echo ''
echo '###########################################################'
echo 'Creacion del stack de contenedores'
echo '###########################################################'

docker compose -f redpanda-server.yaml \
               -f postgres-server.yaml \
               -p jconf2026-app \
               up -d

echo ''
echo '###########################################################'
echo 'Esperando a que Redpanda este listo'
echo '###########################################################'

until docker exec redpanda rpk cluster info --brokers localhost:29092 >/dev/null 2>&1; do
  echo 'Redpanda aun no responde, reintentando en 2s...'
  sleep 2
done

echo ''
echo '###########################################################'
echo 'Creacion de los topicos de kafka'
echo '###########################################################'

docker exec redpanda rpk topic create "$TOPIC" --partitions 3 --replicas 1 --brokers localhost:29092 || true
docker exec redpanda rpk topic create "$DLQ_TOPIC" --partitions 1 --replicas 1 --brokers localhost:29092 || true

echo ''
echo 'Topicos disponibles:'
docker exec redpanda rpk topic list --brokers localhost:29092
echo ''
