package pe.marcolopez.apps.jconf.telemetry.resource.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.time.Instant;

public record VehicleTelemetryInput(
        @NotBlank(message = "Campo obligatorio")
        @Pattern(
                regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
                message = "eventId debe ser un UUID válido")
        String eventId,

        @NotBlank(message = "Campo obligatorio")
        @Size(max = 50, message = "vehicleId admite máximo 50 caracteres")
        String vehicleId,

        @NotNull(message = "Campo obligatorio")
        @Min(value = -90, message = "El valor mínimo permitido para la latitud es -90")
        @Max(value = 90, message = "El valor máximo permitido para la latitud es 90")
        Double latitude,

        @NotNull(message = "Campo obligatorio")
        @Min(value = -180, message = "El valor mínimo permitido para la longitud es -180")
        @Max(value = 180, message = "El valor máximo permitido para la longitud es 180")
        Double longitude,

        @NotNull(message = "Campo obligatorio")
        @Min(value = 0, message = "El valor mínimo permitido para la velocidad es 0")
        @Max(value = 300, message = "El valor máximo permitido para la velocidad es 300")
        Double speedKmh,

        @NotNull(message = "Campo obligatorio")
        Instant occurredAt
) {
}