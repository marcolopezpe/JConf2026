package pe.marcolopez.apps.jconf.telemetry.util;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Propiedades "telemetry.*" del consumer.
 *
 *   telemetry.persistence.mode            single | batch | copy
 *   telemetry.batch.size                  eventos por executeBatch() en la estrategia batch
 *   telemetry.concurrency.virtual-threads true = persistir sublotes en Virtual Threads (default false)
 *   (workers = application.internal-values.semaphore.permits, el semaforo que ya existe)
 *   telemetry.concurrency.chunk-size      eventos por sublote cuando workers > 1
 *   telemetry.retry.initial-backoff-ms    primera espera tras un fallo
 *   telemetry.retry.max-backoff-ms        tope de la espera exponencial
 *   telemetry.benchmark.idle-timeout-ms   sin mensajes por este tiempo = fin de la corrida
 */
@ConfigMapping(prefix = "telemetry")
public interface TelemetryValues {

    Persistence persistence();

    Batch batch();

    Concurrency concurrency();

    Retry retry();

    Dlq dlq();

    Benchmark benchmark();

    interface Persistence {

        @WithDefault("single")
        String mode();
    }

    interface Batch {

        /** Cuantos INSERT se envian juntos en cada executeBatch(). Todos dentro de una misma transaccion por poll. */
        @WithDefault("500")
        int size();
    }

    interface Concurrency {

        /** INTERRUPTOR. false = camino secuencial de siempre; true = los sublotes se persisten en Virtual Threads. */
        @WithDefault("false")
        boolean virtualThreads();

        // Los "workers" (sublotes a la vez) NO estan aqui: son los permisos del semaforo que ya existe,
        // application.internal-values.semaphore.permits (InternalValues.semaphore().permits()).

        /** Tamano de cada sublote. Con max-poll-records=500 y chunk-size=100 salen 5 sublotes por poll. */
        @WithDefault("100")
        int chunkSize();
    }

    interface Dlq {

        /** true = los mensajes invalidos se guardan en el topic DLQ antes de saltarlos. */
        @WithDefault("true")
        boolean enabled();

        @WithDefault("vehicle-telemetry-dlq")
        String topic();

        /** Espera maxima por la confirmacion de la DLQ (acks=all). Si vence, se reintenta; no se descarta. */
        @WithDefault("10000")
        long sendTimeoutMs();
    }

    interface Retry {

        @WithDefault("1000")
        long initialBackoffMs();

        @WithDefault("15000")
        long maxBackoffMs();
    }

    interface Benchmark {

        @WithDefault("5000")
        long idleTimeoutMs();
    }
}