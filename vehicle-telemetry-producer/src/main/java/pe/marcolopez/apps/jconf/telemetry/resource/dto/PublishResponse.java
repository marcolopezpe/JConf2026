package pe.marcolopez.apps.jconf.telemetry.resource.dto;

public record PublishResponse(
        int received,
        int published,
        int failed
) {
}
