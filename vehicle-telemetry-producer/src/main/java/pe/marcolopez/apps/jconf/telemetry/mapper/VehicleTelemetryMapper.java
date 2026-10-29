package pe.marcolopez.apps.jconf.telemetry.mapper;

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.resource.dto.VehicleTelemetryInput;

@Mapper(componentModel = "jakarta")
public interface VehicleTelemetryMapper {

    @Mapping(
            target = "occurredAtEpochMillis",
            expression = "java(input.occurredAt() == null ? null : String.valueOf(input.occurredAt().toEpochMilli()))"
    )
    VehicleTelemetry toEvent(VehicleTelemetryInput input);
}
