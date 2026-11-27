#!/bin/bash
# Uso:
#   ./stop-infra.sh       -> detiene contenedores y CONSERVA los datos
#   ./stop-infra.sh -v    -> detiene y BORRA el volumen de PostgreSQL
#                            (necesario para que se vuelva a ejecutar init/01-schema.sql)
echo ''
echo '###########################################################'
echo 'Eliminacion del stack de contenedores'
echo '###########################################################'

docker compose -f redpanda-server.yaml \
               -f postgres-server.yaml \
               -p jconf2026-app \
               down "$@"

echo ''
