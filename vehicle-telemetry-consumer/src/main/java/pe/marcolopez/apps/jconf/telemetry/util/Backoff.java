package pe.marcolopez.apps.jconf.telemetry.util;

/**
 * Espera exponencial con tope: initial, 2x, 4x ... hasta max. reset() vuelve al inicio.
 * No es thread-safe: cada flujo de reintento crea la suya.
 */
public final class Backoff {

    private final long initialMs;
    private final long maxMs;
    private long currentMs;

    public Backoff(long initialMs, long maxMs) {
        this.initialMs = Math.max(1L, initialMs);
        this.maxMs = Math.max(this.initialMs, maxMs);
        this.currentMs = this.initialMs;
    }

    /** Devuelve la espera actual y duplica la siguiente (hasta el tope). */
    public long nextMs() {
        long value = currentMs;
        currentMs = Math.min(currentMs * 2, maxMs);
        return value;
    }

    public void reset() {
        currentMs = initialMs;
    }
}