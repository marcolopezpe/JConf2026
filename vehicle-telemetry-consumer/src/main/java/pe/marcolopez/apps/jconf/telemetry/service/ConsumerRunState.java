package pe.marcolopez.apps.jconf.telemetry.service;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicBoolean;

/** Bandera compartida: el consumer sigue vivo mientras no se pida el apagado. */
@ApplicationScoped
public class ConsumerRunState {

    private final AtomicBoolean running = new AtomicBoolean(true);

    public boolean isRunning() {
        return running.get();
    }

    public void stop() {
        running.set(false);
    }
}