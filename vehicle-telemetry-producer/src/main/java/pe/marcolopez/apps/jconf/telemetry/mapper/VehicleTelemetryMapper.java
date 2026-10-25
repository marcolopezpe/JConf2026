package pe.marcolopez.apps.jconf.telemetry.mapper;

import org.mapstruct.Mapper;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.resource.dto.VehicleTelemetryInput;

@Mapper(componentModel = "jakarta")
public interface VehicleTelemetryMapper {

    VehicleTelemetry toEvent(VehicleTelemetryInput input);
}
