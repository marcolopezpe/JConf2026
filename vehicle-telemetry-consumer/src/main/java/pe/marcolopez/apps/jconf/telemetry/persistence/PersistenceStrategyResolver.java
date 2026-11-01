package pe.marcolopez.apps.jconf.telemetry.persistence;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import pe.marcolopez.apps.jconf.telemetry.util.TelemetryValues;
import java.util.ArrayList;
import java.util.List;

/**
 * Elige UNA estrategia segun telemetry.persistence.mode. Falla al arrancar si el modo no existe
 * o aun no esta implementado, para no descubrirlo con datos en vuelo.
 */
@ApplicationScoped
public class PersistenceStrategyResolver {

    @Inject
    @Any
    Instance<TelemetryPersistenceStrategy> strategies;

    @Inject
    TelemetryValues values;

    public TelemetryPersistenceStrategy resolve() {
        PersistenceMode requested = PersistenceMode.from(values.persistence().mode());
        List<String> implemented = new ArrayList<>();
        TelemetryPersistenceStrategy selected = null;

        for (var candidate : strategies) {
            if (candidate.implemented()) {
                implemented.add(candidate.mode().id());
            }
            if (candidate.mode() == requested) {
                selected = candidate;
            }
        }

        if (selected == null || !selected.implemented()) {
            throw new IllegalStateException(
                    "telemetry.persistence.mode='%s' aun no esta implementada. Implementadas: %s"
                            .formatted(requested.id(), implemented));
        }
        return selected;
    }
}