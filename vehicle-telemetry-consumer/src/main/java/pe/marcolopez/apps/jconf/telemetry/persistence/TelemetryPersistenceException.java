package pe.marcolopez.apps.jconf.telemetry.persistence;

/**
 * Fallo TRANSITORIO de infraestructura (PostgreSQL caida, pool agotado, timeout...).
 * El consumer no confirma offsets y reintenta con backoff. Nunca descarta el evento.
 */
public class TelemetryPersistenceException extends RuntimeException {

    private final transient PersistenceResult partial;

    public TelemetryPersistenceException(String message, Throwable cause, PersistenceResult partial) {
        super(message, cause);
        this.partial = partial;
    }

    /** Lo procesado ANTES del fallo (el evento que fallo no esta incluido). Permite reintentar solo el resto. */
    public PersistenceResult partial() {
        return partial;
    }
}