package pe.marcolopez.apps.jconf.telemetry.resource.dto;

import java.util.List;

public record TelemetryBatchRequest(
        List<VehicleTelemetryInput> events
) {
}