#!/bin/bash
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
echo 'Creacion de los topicos de kafka'
echo '###########################################################'

docker exec redpanda rpk topic create vehicle-telemetry-topic --partitions 3 --replicas 1 --if-not-exists --brokers localhost:29092

echo ''