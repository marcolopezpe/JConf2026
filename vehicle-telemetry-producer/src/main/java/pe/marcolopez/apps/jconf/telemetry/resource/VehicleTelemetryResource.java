package pe.marcolopez.apps.jconf.telemetry.resource;

import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import pe.marcolopez.apps.jconf.telemetry.resource.dto.VehicleTelemetryInput;
import pe.marcolopez.apps.jconf.telemetry.service.VehicleTelemetryService;
import java.util.List;

@Path("/vehicle-telemetry")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class VehicleTelemetryResource {

    int MAX_BATCH_SIZE = 1000;

    @Inject
    VehicleTelemetryService service;

    @Path("/batch")
    @POST
    @RunOnVirtualThread
    public Response publishBatch(@Valid List<VehicleTelemetryInput> inputs) {
        if (inputs == null) {
            return badRequest("Request body no puede ser null");
        }

        if (inputs.isEmpty()) {
            return badRequest("Al menos un evento de telemetría debe ser enviado");
        }

        if (inputs.size() > MAX_BATCH_SIZE) {
            return Response.status(Response.Status.REQUEST_ENTITY_TOO_LARGE)
                    .entity(new ErrorResponse(
                            "Maximo batch es " + MAX_BATCH_SIZE))
                    .build();
        }

        var result = service.publishAsync(inputs);

        if (result.failed() > 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(result)
                    .build();
        }

        return Response.ok(result).build();
    }

    private Response badRequest(String message) {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity(new ErrorResponse(message))
                .build();
    }

    public record ErrorResponse(String message) {
    }
}
