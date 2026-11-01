package pe.marcolopez.apps.jconf.telemetry.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.benchmark.BenchmarkRecorder;
import pe.marcolopez.apps.jconf.telemetry.util.Backoff;
import pe.marcolopez.apps.jconf.telemetry.util.KafkaValues;
import pe.marcolopez.apps.jconf.telemetry.util.TelemetryValues;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;

/**
 * Ciclo de consumo. Corre en UN hilo (KafkaConsumer no es thread-safe).
 *
 * Garantia: este ciclo NO termina por errores. Solo termina cuando se pide el apagado.
 * Cualquier excepcion dentro de una iteracion se registra, se rebobina el poll al primer offset
 * (para no saltarse eventos) y se vuelve a intentar tras una espera.
 */
@Slf4j
@ApplicationScoped
public class TelemetryPollLoop {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    @Inject
    @Named("consumer")
    Consumer<String, VehicleTelemetry> consumer;

    @Inject
    KafkaValues kafkaValues;

    @Inject
    TelemetryValues values;

    @Inject
    ConsumerRunState state;

    @Inject
    RecordProcessor processor;

    @Inject
    BenchmarkRecorder recorder;

    /** Bloqueante. Se ejecuta en un virtual thread dedicado. */
    public void run() {
        var loopBackoff = new Backoff(values.retry().initialBackoffMs(), values.retry().maxBackoffMs());

        try {
            consumer.subscribe(Collections.singleton(kafkaValues.topic()));
            log.info("### Consumer suscrito al topic={}", kafkaValues.topic());

            while (state.isRunning()) {
                ConsumerRecords<String, VehicleTelemetry> records = ConsumerRecords.empty();

                try {
                    records = pollSafely();

                    if (records.isEmpty()) {
                        recorder.closeIfIdle();
                        continue;
                    }

                    processor.process(records);
                    loopBackoff.reset();
                } catch (WakeupException e) {
                    throw e;
                } catch (Exception e) {
                    if (!state.isRunning()) {
                        break;
                    }
                    long waitMs = loopBackoff.nextMs();
                    log.error("### Error inesperado en el ciclo. El consumer NO se detiene: reprocesa el poll en {} ms.",
                            waitMs, e);
                    rewind(records);
                    sleepQuietly(waitMs);
                }
            }
        } catch (WakeupException e) {
            if (state.isRunning()) {
                log.error("### Consumer despertado inesperadamente.", e);
            }
        } finally {
            try {
                recorder.flush();
            } catch (RuntimeException e) {
                log.warn("### No se pudo cerrar la corrida de benchmark", e);
            }
            consumer.close();
            log.info("### Consumer kafka cerrado.");
        }
    }

    /**
     * poll() lanza RecordDeserializationException con un mensaje ilegible. Antes eso mataba el loop.
     * Ahora: mensaje malo -> se salta y se cuenta (pendiente: enviarlo a la DLQ);
     * Schema Registry inaccesible -> NO se descarta nada, se espera y se reintenta.
     */
    private ConsumerRecords<String, VehicleTelemetry> pollSafely() {
        try {
            return consumer.poll(POLL_TIMEOUT);
        } catch (RecordDeserializationException e) {
            if (hasCause(e, IOException.class)) {
                log.error("### No se pudo deserializar (Schema Registry inaccesible?). Se reintenta. Causa: {}",
                        rootMessage(e));
                sleepQuietly(Math.min(values.retry().maxBackoffMs(), 5_000L));
            } else {
                TopicPartition partition = e.topicPartition();
                consumer.seek(partition, e.offset() + 1);
                recorder.addFailed(1);
                log.warn("### Mensaje invalido descartado (pendiente DLQ). partition={}, offset={}, causa={}",
                        partition.partition(), e.offset(), rootMessage(e));
            }
            return ConsumerRecords.empty();
        }
    }

    /** Vuelve cada particion al primer offset del poll fallido para que se relea completo. */
    private void rewind(ConsumerRecords<String, VehicleTelemetry> records) {
        try {
            for (TopicPartition partition : records.partitions()) {
                consumer.seek(partition, records.records(partition).get(0).offset());
            }
        } catch (RuntimeException e) {
            log.warn("### No se pudo rebobinar el poll (se releera por rebalance o reinicio): {}", e.getMessage());
        }
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            state.stop();
        }
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (type.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    private static String rootMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }
}