package pe.marcolopez.apps.jconf.telemetry.resource.dto;

/**
 * Resultado de la PUBLICACION en Redpanda. No mide persistencia:
 * el throughput de persistencia se mide en el consumer.
 */
public record LoadTestResponse(
        int requested,
        int published,
        int failed,
        int batches,
        long durationMs,
        double publishThroughputEventsPerSecond
) {
}