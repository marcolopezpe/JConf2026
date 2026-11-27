#!/bin/bash
# Uso: sh conexiones.sh [segundos]     (por defecto 30)
# Cada segundo: hora, conexiones abiertas a la base y cuantas estan trabajando.
N="${1:-30}"
for i in $(seq 1 "$N"); do
  docker exec postgres psql -U postgres -d db_vehicle_telemetry -tAc \
    "select now()::time(0), count(*) as conexiones, count(*) filter (where state='active') as activas
     from pg_stat_activity where datname='db_vehicle_telemetry'"
  sleep 1
done