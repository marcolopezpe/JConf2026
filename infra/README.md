# Demo · Más hilos no siempre es más rendimiento

```
curl → producer (Quarkus + Avro) → Redpanda (3 particiones) → consumer (Quarkus) → PostgreSQL
                                         └→ vehicle-telemetry-dlq (mensajes inválidos)
```

- **Contrato Avro:** seis campos, todos `string` (`eventId`, `vehicleId`, `latitude`, `longitude`, `speedKmh`, `occurredAtEpochMillis`).
- **Consumer:** tres formas de guardar en PostgreSQL (`single`, `batch`, `copy`) y un interruptor de Virtual Threads.
- **Tus números serán distintos a los de la charla.** Importa la proporción entre pruebas, no el valor exacto.

## Antes de empezar

- Docker en marcha, Java 25, `curl`.
- Windows: usa **Git Bash** (en CMD no funcionan estos comandos). macOS y Linux: el Terminal.
- Tres terminales: **T0** producer (lo arrancas y no lo tocas) · **T1** consumer · **T2** comandos.
- Todo se ejecuta desde `infra/`, salvo T0.
- **Entre pruebas:** espera el bloque `RESULTADO DE LA CORRIDA` en T1 y pulsa Ctrl+C.

## Preparación

```bash
# T2
sh start-infra.sh
sh reset-demo.sh
```
```bash
# T0 (después de lo anterior)
cd vehicle-telemetry-producer
./mvnw quarkus:dev
```

## Las pruebas

### 1. Cargar 10.000 eventos · T2
```bash
curl -s -X POST localhost:8001/telemetry/load-test/10000
```
Verás `published=10000, failed=0`. Se genera **una sola vez**: cada prueba siguiente relee los mismos eventos.

### 2. single, batch y copy · T1
```bash
sh run-consumer.sh single
sh run-consumer.sh batch
sh run-consumer.sh copy
```
Compara en T2:
```bash
sh compare-workers.sh
```
Verás eventos por segundo y cuántas veces más rápida es cada una frente a la base.

### 3. Virtual Threads · T1
```bash
VIRTUAL_THREADS=true VT_WORKERS=4 JDBC_POOL_SIZE=4 VT_CHUNK_SIZE=25 sh run-consumer.sh single
```
Verás `Virtual Threads ACTIVOS: workers=4`. Compara con `sh compare-workers.sh`.

### 4. Demasiados hilos · T1 + T2
La pregunta: ¿qué pasa si subo los hilos mucho más allá de lo que la base aguanta?
```bash
# T1: una a la vez
VIRTUAL_THREADS=true VT_WORKERS=8   JDBC_POOL_SIZE=8   VT_CHUNK_SIZE=25 sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=100 JDBC_POOL_SIZE=4   VT_CHUNK_SIZE=5  sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=200 JDBC_POOL_SIZE=200 VT_CHUNK_SIZE=5 MAX_POLL_RECORDS=1000 sh run-consumer.sh single
```
```bash
# T2, mientras corre cada una
sh conexiones.sh
```
Mira: el throughput frente a la primera, `Concurrencia observada` en el log, errores (`acquisition timeout`, `too many clients already`) y cuántas conexiones hay de verdad.

> Con poll 500 y sublotes de 25 solo hay 20 sublotes, y más workers no cambia nada. Por eso las otras dos usan sublotes pequeños: así sí hay cientos de hilos.

### 5. Backpressure · T1 + T2
```bash
# T1: déjalo correr hasta el RESULTADO y NO lo pares. Anota el group-id que imprime.
sh run-consumer.sh single
```
```bash
# T2
sh lag.sh <group-id>
```
Verás el lag en 0, subir a ~10.000 y bajar por escalones hasta 0: el consumer es lento, pero no pierde nada.

### 6. Caída de PostgreSQL · T2
Con el consumer de la prueba anterior aún corriendo:
```bash
curl -s -X POST localhost:8001/telemetry/load-test/10000
sleep 5
docker stop postgres
```
En T1 verás `[infraestructura] Intento N fallo`. A los ~20 s:
```bash
docker start postgres
```
El consumer se recupera solo. Comprueba que no se perdió ni se duplicó nada:
```bash
docker exec postgres psql -U postgres -d db_vehicle_telemetry -c "SELECT count(*) AS filas, count(DISTINCT event_id) AS distintos FROM vehicle_telemetry;"
```
`filas` y `distintos` deben coincidir.

### 7. Idempotencia · T1
Ctrl+C en T1 y reprocesa todo con la tabla **ya llena**:
```bash
sh run-consumer.sh single --keep-data
```
Verás `persisted=0` y `duplicated` igual a todo lo leído, sin errores. Déjalo corriendo.

### 8. DLQ · T2
Lo último: los mensajes inválidos quedan en el tópico para siempre. Con el consumer de la prueba anterior aún corriendo:
```bash
for i in 1 2 3; do echo "basura-$i" | docker exec -i redpanda rpk topic produce vehicle-telemetry-topic -k VH-BAD-00$i --brokers localhost:29092; done
docker exec redpanda rpk topic consume vehicle-telemetry-dlq -o start -n 3 --brokers localhost:29092 -f '%k | %v | %h{%k=%v; }\n'
```
Verás 3 líneas `Mensaje invalido enviado a la DLQ=si` en T1, el consumer sigue vivo, y en la DLQ el mensaje original con headers de causa (`x-dlq-reason`, `x-dlq-origin-partition`, `x-dlq-error-message`...).

## Variables

Van **delante** del comando, en la misma línea. Revisa la línea `>>> virtual-threads=... workers=... pool=...` antes de esperar.

| Variable | Qué hace | Por defecto |
|---|---|---|
| `VIRTUAL_THREADS` | activa Virtual Threads | `false` |
| `VT_WORKERS` | sublotes a la vez | `1` |
| `JDBC_POOL_SIZE` | conexiones; debe ser ≥ workers | `1` |
| `VT_CHUNK_SIZE` | tamaño del sublote | `100` |
| `MAX_POLL_RECORDS` | eventos por poll | `500` |

## Si algo falla

| Síntoma | Solución |
|---|---|
| `Schema N not found` | Reinicia el producer (T0) y vuelve a generar. |
| Sale `workers=1 pool=1` aunque pediste más | Las variables van delante del comando y en Git Bash o Terminal. |
| `failed=3` en todas las corridas | Hay basura en el tópico (prueba 8): `sh reset-demo.sh`, reinicia el producer y genera de nuevo. |
| `too many clients already` | Pides más conexiones que `max_connections` (100) de PostgreSQL. Es lo que provoca la prueba 4. |
| `TOTAL-LAG` vacío | Copia el `group-id` exacto que imprime `run-consumer.sh`. |

## Referencia

En mi PC de pruebas (i7-6700, 8 hilos, 32 GB, SSD NVMe, todo en la misma máquina), `single` con 1 worker y pool de 1 dio entre 417,96 y 437,79 eventos/s en corridas de 10.000 eventos. Son cifras para comparar estrategias, no capacidad de producción.