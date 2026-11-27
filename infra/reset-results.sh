#!/bin/bash
# Limpia los resultados que muestran compare-workers.sh y compare-results.sh
# (tabla benchmark_run), para empezar una tanda de pruebas desde cero.
#
# Antes de borrar, guarda una copia en infra/backups/ (CSV) para no perder mediciones.
#
# Uso:
#   sh reset-results.sh        # pide confirmacion
#   sh reset-results.sh -y     # sin preguntar (util en la charla)
#
# No toca la tabla de eventos (vehicle_telemetry), ni los topicos, ni el schema.
# Para eso esta reset-demo.sh.

DB="db_vehicle_telemetry"
TABLE="benchmark_run"

cd "$(dirname "$0")" || exit 1

PSQL="docker exec -i postgres psql -U postgres -d $DB -v ON_ERROR_STOP=1"

COUNT=$($PSQL -t -A -c "SELECT count(*) FROM $TABLE;" 2>/dev/null)
if [ -z "$COUNT" ]; then
  echo "ERROR: no pude leer $TABLE. ¿Esta arriba PostgreSQL? (sh start-infra.sh)"
  exit 1
fi

echo ">> Corridas guardadas en $TABLE: $COUNT"
if [ "$COUNT" = "0" ]; then
  echo ">> Ya esta vacia. Nada que limpiar."
  exit 0
fi

if [ "$1" != "-y" ]; then
  printf "¿Borrar las %s corridas? Se guarda una copia antes [s/N]: " "$COUNT"
  read -r RESP
  case "$RESP" in
    s|S|si|SI|y|Y) ;;
    *) echo ">> Cancelado. No se borro nada."; exit 0 ;;
  esac
fi

mkdir -p backups
BACKUP="backups/${TABLE}_$(date +%Y%m%d-%H%M%S).csv"
if ! $PSQL -c "COPY (SELECT * FROM $TABLE ORDER BY id) TO STDOUT WITH CSV HEADER" > "$BACKUP"; then
  echo "ERROR: no se pudo crear la copia. No se borro nada."
  rm -f "$BACKUP"
  exit 1
fi
echo ">> Copia guardada en infra/$BACKUP"

$PSQL -c "TRUNCATE TABLE $TABLE RESTART IDENTITY;" >/dev/null || { echo "ERROR: no se pudo limpiar $TABLE"; exit 1; }

echo ">> Listo: $TABLE vacia. compare-workers.sh empezara desde cero."