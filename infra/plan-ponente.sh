# Plan del ponente (no va en el repo)

Máquina del escenario: MacBook Pro 2020, i5 2.0 GHz, 16 GB. Los números de las diapositivas deben salir de **esta** máquina.

## El día anterior
- [ ] Medir toda la matriz (abajo) en el MacBook y poner esas cifras en las diapositivas, incluida "Entorno de pruebas".
- [ ] Conectado a la corriente, todo lo demás cerrado, y una corrida de calentamiento (un i5 de 2 GHz se calienta).
- [ ] Docker Desktop: fijar CPUs y RAM y comprobar con `docker info`.
- [ ] Revisar si `postgres-data` es una carpeta montada (en Mac es más lenta): `docker inspect postgres --format '{{json .Mounts}}'`
- [ ] `caffeinate -d` en un terminal aparte; letra grande; proyector probado.
- [ ] Video corto de cada prueba como respaldo.

## Matriz previa (2 corridas por fila, Ctrl+C entre una y otra)
```bash
sh reset-demo.sh          # reinicia el producer y genera 10.000 con la prueba 1 del README
sh run-consumer.sh single
sh run-consumer.sh batch
sh run-consumer.sh copy
VIRTUAL_THREADS=true VT_WORKERS=2  JDBC_POOL_SIZE=2  VT_CHUNK_SIZE=25 sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=4  JDBC_POOL_SIZE=4  VT_CHUNK_SIZE=25 sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=8  JDBC_POOL_SIZE=8  VT_CHUNK_SIZE=25 sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=16 JDBC_POOL_SIZE=16 VT_CHUNK_SIZE=25 sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=16 JDBC_POOL_SIZE=4  VT_CHUNK_SIZE=25 sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=2  JDBC_POOL_SIZE=2  sh run-consumer.sh batch
VIRTUAL_THREADS=true VT_WORKERS=4  JDBC_POOL_SIZE=4  sh run-consumer.sh batch
VIRTUAL_THREADS=true VT_WORKERS=2  JDBC_POOL_SIZE=2  sh run-consumer.sh copy
VIRTUAL_THREADS=true VT_WORKERS=4  JDBC_POOL_SIZE=4  sh run-consumer.sh copy
VIRTUAL_THREADS=true VT_WORKERS=100 JDBC_POOL_SIZE=4   VT_CHUNK_SIZE=5 sh run-consumer.sh single
VIRTUAL_THREADS=true VT_WORKERS=200 JDBC_POOL_SIZE=200 VT_CHUNK_SIZE=5 MAX_POLL_RECORDS=1000 sh run-consumer.sh single
sh compare-workers.sh
```
Para empezar de cero la tabla de resultados: `docker exec postgres psql -U postgres -d db_vehicle_telemetry -c "TRUNCATE TABLE benchmark_run;"`

La prueba 4 **no está medida todavía**. Hipótesis a confirmar (lo que salga manda): 100 hilos con 4 conexiones no superan a 4 con 4; con 200 conexiones PostgreSQL podría rechazar por `max_connections`.

## Diez minutos antes
- [ ] `sh start-infra.sh` y `sh reset-demo.sh`; producer (T0) **después** del reset.
- [ ] Prueba 1 del README y comprobar que el `HIGH-WATERMARK` suma 10.000.
- [ ] T1 y T2 abiertos en `infra/`, con `compare-workers.sh` ya impreso.

## Línea de tiempo (prueba del README → minuto)
| Minuto | Diapositiva | Prueba |
|---|---|---|
| 0:10 | Demo en vivo → Demo normal | 1 |
| 0:15 | Single | 2 (single) |
| 0:20 | Batch | 2 (batch) |
| 0:22 | COPY | 2 (copy) y comparación |
| 0:31 | Virtual Threads | 3 y, si hay tiempo, 4 (100 hilos con 4 conexiones) |
| 0:33 | Backpressure | 5 (arranca el consumer al empezar la teoría) |
| 0:36 | Caída de PostgreSQL | 6 (o el video) |
| 0:41 | Idempotencia | 7 |
| 0:44 | DLQ | 8 (la última, deja basura en el tópico) |

## Reglas
- Generar una vez y consumir varias.
- No editar diapositivas en vivo: la terminal muestra el resultado y la diapositiva respalda.
- Una corrida por caso en vivo; el resto sale de la tabla ya medida.
- La caída de PostgreSQL es la más arriesgada: ten el video listo.
- Si una corrida sale rara, no la repitas: "esta salió más lenta; la tabla trae el promedio".