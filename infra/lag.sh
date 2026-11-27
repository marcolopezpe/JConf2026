#!/bin/bash
# Uso: sh lag.sh <group-id>      (el que imprime run-consumer.sh en ">>> group-id=...")
# Muestra el lag del consumer EN VIVO, una linea por muestra, publica 10,000 eventos
# a los 3 s y termina solo cuando el lag vuelve a 0 (o a los 120 s).

GRUPO="${1:?Uso: sh lag.sh <group-id>   (el que imprime run-consumer.sh en >>> group-id=...)}"
BROKERS="localhost:29092"

lag() {
  docker exec redpanda rpk group describe "$GRUPO" --brokers "$BROKERS" 2>/dev/null \
    | awk '$1=="TOTAL-LAG"{print $2; exit}'
}

ACTUAL="$(lag)"
if [ -z "$ACTUAL" ]; then
  echo "ERROR: no encuentro el grupo '$GRUPO'."
  echo "  - ¿El consumer de T1 sigue corriendo? (no lo pares despues del RESULTADO)"
  echo "  - Copia el group-id exacto de la linea '>>> group-id=...' de T1."
  echo "  Grupos que existen ahora:"
  docker exec redpanda rpk group list --brokers "$BROKERS"
  exit 1
fi

echo ">> Grupo $GRUPO · lag inicial: $ACTUAL"
echo ">> En 3 s se publican 10,000 eventos. Mira como sube y baja:"
echo

( sleep 3; R=$(curl -s -X POST localhost:8001/telemetry/load-test/10000); echo "   [producer] $R" ) &

PICO=0; INICIO=$(date +%s)
while :; do
  L="$(lag)"; L="${L:-0}"
  [ "$L" -gt "$PICO" ] 2>/dev/null && PICO=$L
  BARRA=$(printf '%*s' $(( L / 250 )) '' | tr ' ' '#')
  printf '%s  lag=%6s  %s\n' "$(date +%T)" "$L" "$BARRA"
  if [ "$PICO" -gt 0 ] && [ "$L" = "0" ]; then
    echo; echo ">> Listo: el lag subio a $PICO y volvio a 0 en $(( $(date +%s) - INICIO - 3 )) s aprox. No se perdio ningun evento."
    break
  fi
  if [ $(( $(date +%s) - INICIO )) -ge 120 ]; then
    echo; echo ">> Corte a los 120 s (pico: $PICO, lag actual: $L). ¿Sigue corriendo el consumer en T1?"
    break
  fi
  sleep 1
done
wait