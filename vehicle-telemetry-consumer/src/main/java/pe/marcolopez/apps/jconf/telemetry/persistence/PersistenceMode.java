package pe.marcolopez.apps.jconf.telemetry.persistence;

import java.util.Arrays;

public enum PersistenceMode {

    SINGLE("single"),
    BATCH("batch"),
    COPY("copy");

    private final String id;

    PersistenceMode(String id) {
        this.id = id;
    }

    /**
     * Texto que se escribe en la propiedad y se guarda en benchmark_run.strategy.
     */
    public String id() {
        return id;
    }

    public static PersistenceMode from(String value) {
        if (value != null) {
            for (var mode : values()) {
                if (mode.id.equalsIgnoreCase(value.trim())) {
                    return mode;
                }
            }
        }
        throw new IllegalArgumentException(
                "telemetry.persistence.mode='%s' no es valido. Valores permitidos: %s"
                        .formatted(value, Arrays.stream(values()).map(PersistenceMode::id).toList()));
    }
}
