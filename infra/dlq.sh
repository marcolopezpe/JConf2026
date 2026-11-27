#!/bin/bash
# DLQ en dos pasos (desde infra/):
#   sh dlq.sh inyectar   -> mete 3 mensajes basura en el topico principal (sin consumer todavia)
#   sh dlq.sh ver        -> con el consumer corriendo, muestra en vivo cuantos llegan a la DLQ (x/3)
#                           y al completar imprime los 3 con sus headers
#   sh dlq.sh            -> los dos pasos seguidos (si el consumer ya esta corriendo)

TOPIC="vehicle-telemetry-topic"
DLQ="vehicle-telemetry-dlq"
BROKERS="localhost:29092"
N=3
BASE_FILE="$(cd "$(dirname "$0")" && pwd)/.dlq-antes"
LIMITE=180

total() {  # suma del high watermark de todas las particiones del topico
  docker exec redpanda rpk topic describe "$1" -p --brokers "$BROKERS" 2>/dev/null \
    | awk 'NR==1{for(i=1;i<=NF;i++) if($i=="HIGH-WATERMARK") c=i; next} c{s+=$c} END{print s+0}'
}

inyectar() {
  ANTES=$(total "$DLQ"); echo "$ANTES" > "$BASE_FILE"
  echo ">> La DLQ tiene $ANTES mensajes. Inyectando $N mensajes basura en $TOPIC..."
  for i in $(seq 1 $N); do
    echo "basura-$i" | docker exec -i redpanda rpk topic produce "$TOPIC" -k "VH-BAD-00$i" --brokers "$BROKERS" >/dev/null \
      && echo "   enviado basura-$i"
  done
  echo ">> Listo. Quedan en el topico esperando a un consumer."
}

ver() {
  ANTES=$(cat "$BASE_FILE" 2>/dev/null || echo 0)
  echo ">> Esperando a que el consumer mande la basura a la DLQ (corta a los ${LIMITE} s)..."
  INICIO=$(date +%s)
  while :; do
    LLEGARON=$(( $(total "$DLQ") - ANTES ))
    [ "$LLEGARON" -gt "$N" ] && LLEGARON=$N
    BARRA=$(printf '%*s' "$LLEGARON" '' | tr ' ' '#'); FALTA=$(printf '%*s' $(( N - LLEGARON )) '' | tr ' ' '.')
    printf '\r   DLQ: [%s%s] %s/%s  (%ss)' "$BARRA" "$FALTA" "$LLEGARON" "$N" "$(( $(date +%s) - INICIO ))"
    [ "$LLEGARON" -ge "$N" ] && break
    if [ $(( $(date +%s) - INICIO )) -ge $LIMITE ]; then
      echo; echo
      echo ">> En ${LIMITE} s solo llegaron $LLEGARON de $N a la DLQ. Revisa:"
      echo "   - ¿El consumer esta corriendo en T1? Sin consumer, nadie los manda a la DLQ."
      echo "   - En T1 debe aparecer 'Mensaje invalido enviado a la DLQ=si'."
      echo "   - ¿Habia muchos eventos sin leer antes de la basura? Van al final de la fila: revisa el lag con rpk group describe."
      exit 1
    fi
    sleep 1
  done
  echo; echo
  echo ">> Los $N llegaron a la DLQ en $(( $(date +%s) - INICIO )) s y el consumer siguio vivo. Contenido:"
  echo
  docker exec redpanda rpk topic consume "$DLQ" -o "$ANTES:end" --brokers "$BROKERS" -f '%k | %v | %h{%k=%v; }\n'
  rm -f "$BASE_FILE"
}

case "${1:-todo}" in
  inyectar) inyectar ;;
  ver)      ver ;;
  todo)     inyectar; echo; ver ;;
  *) echo "Uso: sh dlq.sh [inyectar|ver]"; exit 1 ;;
esac