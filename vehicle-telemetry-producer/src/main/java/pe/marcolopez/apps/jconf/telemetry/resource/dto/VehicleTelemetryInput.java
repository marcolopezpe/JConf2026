package pe.marcolopez.apps.jconf.telemetry.resource.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.time.Instant;

public record VehicleTelemetryInput(
        @NotBlank(message = "Campo obligatorio")
        String eventId,
        @NotBlank(message = "Campo obligatorio")
        String vehicleId,
        @Min(value = -90, message = "El valor mínimo permitido para la latitud es -90")
        @Max(value = 90, message = "El valor máximo permitido para la latitud es 90")
        Double latitude,
        @Min(value = -180, message = "El valor mínimo permitido para la longitud es -180")
        @Max(value = 180, message = "El valor máximo permitido para la longitud es 180")
        Double longitude,
        @Min(value = 0, message = "El valor mínimo permitido para la velocidad es 0")
        @Max(value = 300, message = "El valor máximo permitido para la velocidad es 300")
        Double speedKmh,
        Instant occurredAt
) {
}