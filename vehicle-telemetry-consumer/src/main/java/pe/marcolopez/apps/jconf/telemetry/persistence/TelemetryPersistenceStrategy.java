package pe.marcolopez.apps.jconf.telemetry.persistence;

import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;

import java.util.List;

/**
 * Contrato comun de las tres iniciativas. El consumer solo conoce esta interfaz:
 * cambiar de single a batch o copy es cambiar telemetry.persistence.mode, sin tocar el flujo.
 *
 * Reglas que TODA estrategia debe cumplir:
 *  1. Idempotente: INSERT ... ON CONFLICT (event_id) DO NOTHING.
 *  2. Dato invalido de UN evento  -> se cuenta en failed y se sigue (no lanza excepcion).
 *  3. Fallo de infraestructura    -> TelemetryPersistenceException (el consumer reintenta).
 *  4. Nunca confirma offsets: eso lo hace el consumer despues de persistir.
 */
public interface TelemetryPersistenceStrategy {

    PersistenceMode mode();

    /** Hilos concurrentes contra la base. Se guarda en benchmark_run.workers. */
    default int workers() {
        return 1;
    }

    /** false mientras la iniciativa sea solo un esqueleto; el arranque falla con un mensaje claro. */
    default boolean implemented() {
        return true;
    }

    PersistenceResult persist(List<VehicleTelemetry> events);
}