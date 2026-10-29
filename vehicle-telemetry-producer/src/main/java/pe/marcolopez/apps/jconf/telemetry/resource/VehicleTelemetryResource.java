package pe.marcolopez.apps.jconf.telemetry.resource;

import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import pe.marcolopez.apps.jconf.telemetry.resource.dto.VehicleTelemetryInput;
import pe.marcolopez.apps.jconf.telemetry.service.LoadTestService;
import pe.marcolopez.apps.jconf.telemetry.service.VehicleTelemetryService;
import java.util.List;

@Path("/telemetry")
@Produces(MediaType.APPLICATION_JSON)
public class VehicleTelemetryResource {

    int MAX_BATCH_SIZE = 1000;
    static final int MAX_LOAD_TEST_EVENTS = 1_000_000;

    @ConfigProperty(name = "application.load-test.enabled", defaultValue = "false")
    boolean loadTestEnabled;

    @Inject
    VehicleTelemetryService service;

    @Inject
    LoadTestService loadTestService;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @RunOnVirtualThread
    public Response publishBatch(List<@Valid VehicleTelemetryInput> inputs) {
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
            // CAMBIO: no es culpa del cliente (el body paso la validacion) -> 503, no 400.
            // El cliente puede reintentar el lote completo: el eventId hace el reintento idempotente.
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(result)
                    .build();
        }

        return Response.ok(result).build();
    }

    /**
     * POST /telemetry/load-test/{totalEvents}
     * Solo para la demo. Se habilita con application.load-test.enabled=true.
     */
    @POST
    @Path("/load-test/{totalEvents}")
    @RunOnVirtualThread
    public Response loadTest(@PathParam("totalEvents") int totalEvents) {
        if (!loadTestEnabled) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        if (totalEvents <= 0 || totalEvents > MAX_LOAD_TEST_EVENTS) {
            return badRequest("totalEvents debe estar entre 1 y " + MAX_LOAD_TEST_EVENTS);
        }

        return Response.ok(loadTestService.run(totalEvents)).build();
    }

    private Response badRequest(String message) {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity(new ErrorResponse(message))
                .build();
    }

    public record ErrorResponse(String message) {
    }
}
