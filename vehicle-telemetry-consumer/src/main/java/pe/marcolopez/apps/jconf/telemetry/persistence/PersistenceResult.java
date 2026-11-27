package pe.marcolopez.apps.jconf.telemetry.persistence;

/**
 * Resultado de persistir una lista de eventos.
 *
 * persisted  -> filas realmente insertadas
 * duplicated -> eventId que ya existia (ON CONFLICT DO NOTHING no inserto nada)
 * failed     -> eventos que no se pueden persistir nunca (dato invalido)
 */
public record PersistenceResult(int persisted, int duplicated, int failed) {

    public static final PersistenceResult EMPTY = new PersistenceResult(0, 0, 0);

    public int total() {
        return persisted + duplicated + failed;
    }

    public PersistenceResult plus(PersistenceResult other) {
        return new PersistenceResult(
                persisted + other.persisted,
                duplicated + other.duplicated,
                failed + other.failed);
    }
}
