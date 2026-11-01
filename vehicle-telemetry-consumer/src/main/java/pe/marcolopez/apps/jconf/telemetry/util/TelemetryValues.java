package pe.marcolopez.apps.jconf.telemetry.util;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Propiedades "telemetry.*" del consumer.
 *
 *   telemetry.persistence.mode            single | batch | copy
 *   telemetry.batch.size                  eventos por executeBatch() en la estrategia batch
 *   telemetry.retry.initial-backoff-ms    primera espera tras un fallo
 *   telemetry.retry.max-backoff-ms        tope de la espera exponencial
 *   telemetry.benchmark.idle-timeout-ms   sin mensajes por este tiempo = fin de la corrida
 */
@ConfigMapping(prefix = "application.telemetry")
public interface TelemetryValues {

    Persistence persistence();

    Batch batch();

    Retry retry();

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