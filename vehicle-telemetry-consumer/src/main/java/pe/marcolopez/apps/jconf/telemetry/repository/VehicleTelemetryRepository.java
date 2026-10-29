package pe.marcolopez.apps.jconf.telemetry.repository;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;
import pe.marcolopez.apps.jconf.telemetry.entity.VehicleTelemetryEntity;

import java.util.UUID;

@ApplicationScoped
public class VehicleTelemetryRepository
        implements PanacheRepositoryBase<VehicleTelemetryEntity, UUID> {

    public boolean alreadyExists(UUID eventId) {
        return findByIdOptional(eventId).isPresent();
    }

    public void insertOne(VehicleTelemetryEntity telemetry) {
        persistAndFlush(telemetry);
    }
}
