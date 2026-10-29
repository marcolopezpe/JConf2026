#!/bin/bash
echo ''
echo '###########################################################'
echo 'Eliminacion del stack de contenedores'
echo '###########################################################'

docker compose -f redpanda-server.yaml \
				-f postgres-server.yaml \
				-p jconf2026-app \
				down
