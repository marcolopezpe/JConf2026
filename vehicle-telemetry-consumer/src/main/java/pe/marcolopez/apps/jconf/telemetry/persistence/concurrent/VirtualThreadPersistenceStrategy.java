package pe.marcolopez.apps.jconf.telemetry.persistence.concurrent;

import lombok.extern.slf4j.Slf4j;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.persistence.PersistenceMode;
import pe.marcolopez.apps.jconf.telemetry.persistence.PersistenceResult;
import pe.marcolopez.apps.jconf.telemetry.persistence.TelemetryPersistenceException;
import pe.marcolopez.apps.jconf.telemetry.persistence.TelemetryPersistenceStrategy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Envuelve a la estrategia elegida (single, batch o copy) y le pasa SUBLOTES en paralelo:
 *   un poll -> sublotes de chunk-size eventos -> un Virtual Thread por sublote
 *   un Semaphore deja trabajar a lo sumo "workers" a la vez; el pool JDBC es el segundo limite
 *   el poll NO termina hasta que terminen TODOS los sublotes; despues el RecordProcessor hace el commit.
 * KafkaConsumer no es thread-safe: poll() y commit viven en el hilo del ciclo.
 * Errores: si un sublote falla por infraestructura, no se lanzan mas, y se lanza
 * TelemetryPersistenceException con partial = SOLO el prefijo contiguo de sublotes terminados.
 */
@Slf4j
public class VirtualThreadPersistenceStrategy implements TelemetryPersistenceStrategy {

    private final TelemetryPersistenceStrategy delegate;
    private final int workers;
    private final int chunkSize;
    private final Semaphore permits;
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxInFlight = new AtomicInteger();

    public VirtualThreadPersistenceStrategy(TelemetryPersistenceStrategy delegate, int workers, int chunkSize) {
        if (workers < 1 || chunkSize < 1) {
            throw new IllegalArgumentException("workers y chunk-size deben ser >= 1 (workers=%d, chunk-size=%d)"
                    .formatted(workers, chunkSize));
        }
        this.delegate = delegate;
        this.workers = workers;
        this.chunkSize = chunkSize;
        this.permits = new Semaphore(workers);
    }

    @Override
    public PersistenceMode mode() {
        return delegate.mode();
    }

    @Override
    public int workers() {
        return workers;
    }

    @Override
    public PersistenceResult persist(List<VehicleTelemetry> events) {
        if (events.size() <= chunkSize) {
            return delegate.persist(events);   // nada que repartir
        }

        List<List<VehicleTelemetry>> chunks = split(events);
        List<Future<PersistenceResult>> futures = new ArrayList<>(chunks.size());
        var aborted = new AtomicBoolean();

        // close() del executor espera a que TODAS las tareas terminen: el poll no avanza antes.
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var chunk : chunks) {
                futures.add(executor.submit(() -> runChunk(chunk, aborted)));
            }
        }

        return collect(futures);
    }

    /** @return null si el sublote se omitio porque otro ya habia fallado. */
    private PersistenceResult runChunk(List<VehicleTelemetry> chunk, AtomicBoolean aborted) throws InterruptedException {
        if (aborted.get()) {
            return null;
        }
        permits.acquire();
        try {
            if (aborted.get()) {
                return null;
            }
            int now = inFlight.incrementAndGet();
            int previousMax = maxInFlight.getAndAccumulate(now, Math::max);
            if (now > previousMax) {
                log.info("### Concurrencia observada: {} Virtual Threads persistiendo a la vez (limite workers={})",
                        now, workers);
            }
            try {
                return delegate.persist(chunk);
            } catch (RuntimeException e) {
                aborted.set(true);
                throw e;
            } finally {
                inFlight.decrementAndGet();
            }
        } finally {
            permits.release();
        }
    }

    private PersistenceResult collect(List<Future<PersistenceResult>> futures) {
        PersistenceResult prefix = PersistenceResult.EMPTY;
        boolean prefixOpen = true;
        Throwable firstError = null;

        for (var future : futures) {
            try {
                PersistenceResult result = future.get();   // ya termino: no bloquea
                if (result == null) {
                    prefixOpen = false;
                } else if (prefixOpen) {
                    prefix = prefix.plus(result);
                }
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (firstError == null) {
                    firstError = cause;
                }
                if (prefixOpen && cause instanceof TelemetryPersistenceException tpe) {
                    prefix = prefix.plus(tpe.partial());
                }
                prefixOpen = false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TelemetryPersistenceException("Interrumpido esperando los sublotes", e, prefix);
            }
        }

        if (firstError == null) {
            return prefix;
        }
        if (firstError instanceof TelemetryPersistenceException tpe) {
            throw new TelemetryPersistenceException(tpe.getMessage(), tpe, prefix);
        }
        if (firstError instanceof RuntimeException re) {
            throw re;
        }
        if (firstError instanceof Error err) {
            throw err;
        }
        throw new IllegalStateException("Fallo inesperado en un sublote", firstError);
    }

    private List<List<VehicleTelemetry>> split(List<VehicleTelemetry> events) {
        var chunks = new ArrayList<List<VehicleTelemetry>>((events.size() + chunkSize - 1) / chunkSize);
        for (int from = 0; from < events.size(); from += chunkSize) {
            chunks.add(new ArrayList<>(events.subList(from, Math.min(from + chunkSize, events.size()))));
        }
        return chunks;
    }
}