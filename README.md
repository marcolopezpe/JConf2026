# Demo · Más hilos no siempre es más rendimiento

```
curl → producer (Quarkus + Avro) → Redpanda/Kafka (3 particiones) → consumer (Quarkus) → PostgreSQL
                                         └→ vehicle-telemetry-dlq (mensajes inválidos)
```

- **Contrato Avro:** seis campos, todos `string` (`eventId`, `vehicleId`, `latitude`, `longitude`, `speedKmh`, `occurredAtEpochMillis`).
- **Consumer:** tres formas de guardar en PostgreSQL (`single`, `batch`, `copy`) y un interruptor de Virtual Threads.
- **Redpanda o Kafka:** la demo usa Redpanda porque es liviano (un solo contenedor y trae Schema Registry integrado). El código usa el cliente de Kafka, así que el diseño aplica igual a Kafka; con Kafka necesitas además levantar un Schema Registry. No se probó esta demo con Kafka.
- **Tus números serán distintos a los de la charla.** Importa la proporción entre pruebas, no el valor exacto.

## Workers, sublotes y Virtual Threads en una línea

El consumer trae un **lote** de 500 eventos (el poll, `MAX_POLL_RECORDS`) y lo parte en **sublotes** del tamaño `VT_CHUNK_SIZE`. Cada sublote recibe **un Virtual Thread**. El **Semaphore** deja trabajar a `VT_WORKERS` a la vez, y cada uno usa una conexión del pool (`JDBC_POOL_SIZE`).

- Virtual Threads creados = 500 ÷ `VT_CHUNK_SIZE` (con 25 → **20**).
- Trabajando a la vez = el menor entre `VT_WORKERS` y los sublotes (con 4 workers → **4**).
- El pool debe tener **al menos** tantas conexiones como workers.

## Prerrequisitos: dejarlo todo listo desde cero

Hazlo **una vez, antes de la charla**, en este orden. Son 5 comandos y tardan unos minutos.

### Lo que necesitas instalado

- **Docker** abierto y corriendo (Docker Desktop u OrbStack).
- **Java 25**.
- **Una terminal:** en Windows, **Git Bash** (en CMD no funcionan estos comandos); en Mac o Linux, la Terminal.
- **El repositorio** descargado en tu computadora.
- **Solo en Mac, una vez:** dentro de `infra/`, ejecuta `chmod +x *.sh`.

Abre **3 terminales** y déjalas así toda la charla:

| Terminal | Para qué | Carpeta |
|---|---|---|
| **T0** | El producer (lo arrancas y no lo tocas) | `vehicle-telemetry-producer/` |
| **T1** | El consumer (las pruebas) | `infra/` |
| **T2** | Los demás comandos | `infra/` |

### Los 5 pasos

**1. Levantar la infraestructura** · T2
```bash
sh start-infra.sh
```
Levanta Redpanda/Kafka (con su Schema Registry) y PostgreSQL, y crea los tópicos. Termina mostrando la lista de tópicos.

**2. Dejar todo limpio y registrar el schema Avro** · T2
```bash
sh reset-demo.sh
```
Vacía la tabla de eventos, recrea los tópicos y registra el schema que está en `infra/schema/`. Termina mostrando `vehicle-telemetry-topic-value`: eso confirma que el schema quedó registrado.

**3. Borrar resultados de corridas anteriores** · T2
```bash
sh reset-results.sh -y
```
Deja vacía la comparación de `compare-workers.sh`, guardando antes una copia en `infra/backups/`. Si es la primera vez, dirá que no hay nada que limpiar.

**4. Levantar el producer** · T0
```bash
cd vehicle-telemetry-producer
./mvnw quarkus:dev
```
Espera a ver `Listening on: http://localhost:8001`. No lo cierres.

**5. Comprobar que todo está arriba** · T2
```bash
docker ps --format "{{.Names}}"
```
Deben aparecer `redpanda` y `postgres`. Con eso y el producer escuchando en el puerto 8001, **ya puedes empezar**: la Prueba 1 genera los eventos.

### Reglas durante la charla

- Todos los comandos de T1 y T2 se ejecutan desde `infra/`.
- **Entre pruebas:** espera a que T1 muestre el bloque `RESULTADO DE LA CORRIDA` y pulsa Ctrl+C.
- Si en algún momento corres `sh reset-demo.sh`, **reinicia el producer** (Ctrl+C en T0 y `./mvnw quarkus:dev` otra vez).

<details>
<summary>Registrar el schema a mano (solo si reset-demo.sh falla en ese paso)</summary>

```bash
MSYS_NO_PATHCONV=1 docker cp schema/vehicle-telemetry.avsc redpanda:/tmp/vehicle-telemetry.avsc
MSYS_NO_PATHCONV=1 docker exec redpanda rpk registry schema create vehicle-telemetry-topic-value --schema /tmp/vehicle-telemetry.avsc --type avro
docker exec redpanda rpk registry subject list
```
`MSYS_NO_PATHCONV=1` evita que Git Bash en Windows convierta `/tmp/...` en una ruta de Windows; en Mac y Linux no hace nada.
</details>

## Antes de subir: dejar la demo en cero (infra ya levantada)

Úsalo el día de la charla y en cada ensayo. Supone que Redpanda/Kafka y PostgreSQL **ya están arriba**: solo deja todo limpio, sin reiniciar contenedores. Hazlo unos 15 minutos antes de tu turno.

**1. Detén el consumer y el producer** si quedaron corriendo de un ensayo (Ctrl+C en T0 y T1).

**2. Limpia eventos, tópicos y schema** · T2, desde `infra/`
```bash
sh reset-demo.sh
```
Vacía la tabla, recrea los tópicos y vuelve a registrar el schema Avro.

**3. Limpia la comparación de corridas** · T2
```bash
sh reset-results.sh -y
```

**4. Levanta el producer** · T0
```bash
cd vehicle-telemetry-producer
./mvnw quarkus:dev
```
Espera `Listening on: http://localhost:8001`.

**5. Compila el consumer una vez**, para que la primera prueba no espere a Maven · T1, desde la raíz
```bash
cd vehicle-telemetry-consumer && ./mvnw -q compile && cd ../infra
```

**6. Semáforo final** · T2
```bash
sh check-demo.sh
```
Debe terminar con `TODO LISTO`. Si algo sale como `[FALTA]`, la línea dice qué ejecutar. No cambia nada, así que puedes correrlo las veces que quieras.

Con eso, el estado es: tópico vacío, tablas vacías, comparación vacía, schema registrado y producer arriba. La charla empieza en la **Prueba 1**, que genera los 10.000 eventos en vivo.

> **Ojo con el tiempo de cada corrida:** `run-consumer.sh` usa `./mvnw clean compile quarkus:dev`, que recompila todo en cada prueba. En el ensayo mide cuánto tarda en arrancar. Si pasa de 20 segundos, quita la palabra `clean` de la última línea del script: Quarkus reutiliza lo compilado y arranca mucho más rápido.

## Las pruebas

Cada prueba indica la diapositiva de la presentación v5 desde la que se ejecuta (insignia roja **▶ DEMO · README Prueba N**). Ejecutas solo esa sección y vuelves a la presentación.

> **El orden importa.** Las pruebas 1 a 4 **solo leen** los 10,000 eventos de la Prueba 1, así que se pueden repetir sin limpiar. Tres pruebas **agregan eventos al tópico**, y después de ellas las mediciones dejan de compararse (cada corrida lee todo lo acumulado y `compare-workers.sh` ya no la muestra):
>
> | Prueba | Agrega al tópico |
> |---|---|
> | 5 · Backpressure (`lag.sh`) | +10,000 cada vez |
> | 6 · Caída de PostgreSQL | +10,000 |
> | 8 · DLQ (`dlq.sh inyectar`) | +3 basura cada vez |
>
> Por eso van **al final** y, si quieres volver a medir algo de las pruebas 1 a 4, primero haz **"Antes de subir"** (`reset-demo.sh`, reiniciar el producer, Prueba 1 y `reset-results.sh -y`).

### 1. Cargar 10.000 eventos · T2
> ⏱ Minuto 0:08 · ≈1 min
> 📺 Diapositiva 7: **Publicar, consumir, persistir, confirmar**
```bash
curl -s -X POST localhost:8001/telemetry/load-test/10000
```
Requiere los prerrequisitos completos (infraestructura, schema y producer arriba). Verás `published=10000, failed=0`. Se genera **una sola vez**: cada prueba siguiente relee los mismos eventos.

### 2. single, batch y copy · T1
> ⏱ Minutos 0:10 (single) · 0:12 (batch) · 0:16 (copy)
> 📺 Diapositivas 8 **Single** (solo `single`) · 9 **Batch** (solo `batch`) · 12 **COPY** (solo `copy` + `compare-workers.sh`). En cada una ejecutas solo su línea y vuelves.
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
> ⏱ Minuto 0:23
> 📺 Diapositiva 16: **Los límites reales: Semaphore y pool JDBC** (antes ya explicaste las diapositivas 14 y 15: workers, sublotes y la historia del camión).
```bash
VIRTUAL_THREADS=true VT_WORKERS=4 JDBC_POOL_SIZE=4 VT_CHUNK_SIZE=25 sh run-consumer.sh single
```
Verás `Virtual Threads ACTIVOS: workers=4`. Compara con `sh compare-workers.sh`.

### 3b. Batch y COPY con Virtual Threads · T1
> ⏱ Minuto 0:29 · ≈3 min
> 📺 Diapositiva 19: **Batch y COPY con Virtual Threads, en vivo**. El resultado se compara en la 20, y la 21 explica por qué.

Los mismos 4 workers, pool de 4 y sublotes de 100, pero ahora con batch y con COPY. Una a la vez:
```bash
VIRTUAL_THREADS=true VT_WORKERS=4 JDBC_POOL_SIZE=4 VT_CHUNK_SIZE=100 sh run-consumer.sh batch
VIRTUAL_THREADS=true VT_WORKERS=4 JDBC_POOL_SIZE=4 VT_CHUNK_SIZE=100 sh run-consumer.sh copy
```
Compara en T2:
```bash
sh compare-workers.sh
```
En `veces_vs_base`, cada una se compara con su propia corrida sin Virtual Threads (la de la Prueba 2). En mi PC, **batch subió a 2.65×** (14,882.45 ev/s) y **COPY bajó a 0.56×** (3,231.39 ev/s): los mismos hilos ayudan a una estrategia y frenan a la otra.

**¿Por qué tanta diferencia?** (diapositiva 21) Con sublotes de 100 hay 100 sublotes, y cada uno paga su costo fijo. En batch ese costo es chico (enviar el lote y un commit); en COPY es grande (tabla temporal, COPY, `INSERT … SELECT` y commit). La pista está medida: COPY con **1 solo worker** ya cae a 0.25×, sin ningún paralelismo, y con sublotes de 125 o 250 vuelve a 0.92× y 0.98×. Sin Virtual Threads, COPY guarda los 500 eventos del poll de una vez y empata con batch. Qué paso de COPY cuesta más no se midió.

### 4. Demasiados hilos · T1 + T2
> ⏱ Minuto 0:32 · ≈3 min
> 📺 Diapositiva 22: **Demasiados hilos: dónde se satura**.
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

> Si la corrida de 100 workers termina con menos de 10,000 eventos: el consumer da por cerrada la corrida tras 5 s sin recibir mensajes (`idle-timeout-ms: 5000`), y con 100 hilos esperando 4 conexiones puede pasar más de 5 s sin pedir eventos. Para medirla completa, sube ese valor a `30000` en el `application.yml` del consumer.

> **Opcional · si `compare-workers.sh` no muestra la corrida o las cifras se ven raras** (T2):
> ```bash
> sh ver-corridas.sh
> ```
> Muestra **todas** las corridas de `benchmark_run`, incluso las que `compare-workers.sh` oculta, y dice por qué: leyó menos de 10,000 (corte por `idle-timeout-ms`), más de 10,000 (tópico con eventos acumulados), 0 guardados (la tabla ya tenía los eventos) o duplicados con la tabla vacía (trabajo repetido). Después revisa lo que no se ve a simple vista: mensajes en el tópico y en la DLQ, filas dobles en `vehicle_telemetry`, conexiones contra `max_connections` y errores `too many clients` en el log de PostgreSQL. No cambia nada; solo mira.

### 5. Backpressure · T1 + T2 · opcional en vivo
> ⏱ Minuto 0:35, **solo si vas a tiempo** · arranca el consumer al llegar a la diapositiva
> ⚠️ **Es la última prueba en vivo:** `lag.sh` agrega 10,000 eventos al tópico. Después de ella no vuelvas a las pruebas 1 a 4 sin limpiar.
> 📺 Diapositiva 23: **Resiliencia en un minuto** (tarjeta Backpressure).
```bash
# T1: déjalo correr hasta el RESULTADO y NO lo pares. Anota el group-id que imprime.
sh run-consumer.sh single
```
```bash
# T2
sh lag.sh <group-id>
```
El script muestra el lag **en vivo**, una línea por segundo con una barra. A los 3 s publica 10,000 eventos: verás el lag subir a ~10.000 y bajar por escalones hasta 0. Termina solo cuando vuelve a 0 y te dice el pico y cuánto tardó: el consumer es lento, pero no pierde nada.

Si dice `no encuentro el grupo`: el consumer de T1 no está corriendo o el group-id no es el exacto. El script lista los grupos que existen para que copies el correcto.

## Para repetir en casa (no van en vivo en la v5)

Sus resultados medidos están en la diapositiva 23. Hazlas **en este orden y empezando limpio**, porque cada una deja el tópico distinto:

**0. Empezar limpio** · T2
```bash
sh reset-demo.sh
```
Reinicia el producer (T0), genera los 10,000 eventos (Prueba 1) y arranca un consumer en T1 que se quede corriendo:
```bash
sh run-consumer.sh single
```
Espera su `RESULTADO DE LA CORRIDA` y **no lo pares**: las pruebas 6 y 7 lo usan.

### 6. Caída de PostgreSQL · T2
Con el consumer del paso 0 aún corriendo (esta prueba agrega 10,000 eventos más):
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
> Es la última: deja basura en el tópico. Al terminar, `sh reset-demo.sh` deja todo limpio otra vez.

Los mensajes inválidos quedan en el tópico para siempre. Antes de empezar, Ctrl+C en T1 (el consumer de la Prueba 7).

La DLQ solo recibe mensajes si **un consumer está leyendo el tópico principal**: es el consumer el que encuentra la basura, no puede leerla y la manda a la DLQ. Por eso va en tres pasos:

**1. Generar la basura** · T2 (todavía sin consumer)
```bash
sh dlq.sh inyectar
```
Mete 3 mensajes que no son Avro (`basura-1`, `basura-2`, `basura-3`) en `vehicle-telemetry-topic`. Quedan esperando en el tópico.

**2. Arrancar el consumer** · T1 (el más rápido, para que llegue pronto a la basura)
```bash
VIRTUAL_THREADS=true VT_WORKERS=4 JDBC_POOL_SIZE=4 VT_CHUNK_SIZE=100 sh run-consumer.sh batch
```
Al llegar a cada mensaje basura falla al leerlo, lo manda a la DLQ y sigue: verás 3 líneas `Mensaje invalido enviado a la DLQ=si`. **No lo pares.**

**3. Ver la DLQ** · T2
```bash
sh dlq.sh ver
```
Muestra en vivo cuántos llegaron (`DLQ: [##.] 2/3`). Al completar los 3, imprime cada mensaje original con sus headers de causa (`x-dlq-reason`, `x-dlq-origin-partition`, `x-dlq-error-message`...). En el `RESULTADO DE LA CORRIDA` de T1 verás `failed=3`. Si en 180 s no llegan, se corta y te dice qué revisar.

> Si el tópico tiene muchos eventos acumulados delante de la basura, el consumer los procesa primero. Con batch y 4 workers son segundos; para ensayar desde cero, corre antes `sh reset-demo.sh`.

> ¿Por qué el comando anterior se quedaba en blanco? `rpk topic consume -n 3` espera hasta recibir 3 mensajes, sin límite de tiempo: si ningún consumer había procesado la basura, la DLQ seguía vacía y se quedaba esperando para siempre.

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
| `Schema N not found` o `Subject not found` | Falta el schema: corre `sh reset-demo.sh` (lo registra de nuevo); luego reinicia el producer (T0) y vuelve a generar. |
| `unable to read "C:/Users/.../Temp/vehicle-telemetry.avsc"` | Git Bash convirtió la ruta `/tmp/...`. Usa el `reset-demo.sh` nuevo, o el registro a mano con `MSYS_NO_PATHCONV=1`. |
| `curl: (7) Failed to connect to localhost port 8001` | El producer no está arriba: paso 4 de los prerrequisitos. |
| Sale `workers=1 pool=1` aunque pediste más | Las variables van delante del comando y en Git Bash o Terminal. |
| `failed=3` en todas las corridas | Hay basura en el tópico (prueba 8): `sh reset-demo.sh` (también registra el schema), reinicia el producer y genera de nuevo. |
| `too many clients already` | Pides más conexiones que `max_connections` (100) de PostgreSQL. Es lo que provoca la prueba 4. |
| `compare-workers.sh` no muestra una corrida, o salen duplicados sin motivo | `sh ver-corridas.sh`: lista todas las corridas y dice por qué se ocultó cada una. |
| `compare-workers.sh` muestra corridas viejas | `sh reset-results.sh -y` (guarda una copia y vacía `benchmark_run`). |
| `relation "vehicle_telemetry" does not exist` | La base se creó sin tablas: `sh stop-infra.sh` y luego `sh start-infra.sh` (vuelve a crear las tablas). |
| `check-demo.sh` marca `[FALTA]` | Ejecuta el comando que indica esa misma línea y vuelve a correrlo. |
| `TOTAL-LAG` vacío | Copia el `group-id` exacto que imprime `run-consumer.sh`. |

## Referencia

En mi PC de pruebas (i7-6700, 8 hilos, 32 GB, SSD NVMe, todo en la misma máquina), `single` con 1 worker y pool de 1 dio entre 417,96 y 437,79 eventos/s en corridas de 10.000 eventos. Son cifras para comparar estrategias, no capacidad de producción.
