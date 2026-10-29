package pe.marcolopez.apps.jconf.telemetry.service;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.entity.VehicleTelemetryEntity;
import pe.marcolopez.apps.jconf.telemetry.repository.VehicleTelemetryRepository;
import pe.marcolopez.apps.jconf.telemetry.util.InternalValues;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Semaphore;

@Slf4j
@ApplicationScoped
public class SingleTelemetryPersistenceService {

    @Inject
    VehicleTelemetryRepository repository;

    @Inject
    InternalValues internalValues;

    Semaphore databaseSemaphore;

    @PostConstruct
    void init() {
        databaseSemaphore = new Semaphore(internalValues.semaphore().permits());
        log.info("### Persistencia single inicianda con permits={}", internalValues.semaphore().permits());
    }

    public void process(VehicleTelemetry event) {
        boolean acquired = false;

        try {
            databaseSemaphore.acquire();
            acquired = true;
            persistTransactionally(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("### Interrupcion al esperar disponibilidad de PostgreSQL", e);

            throw new IllegalStateException("### No fue posible procesar el evento", e);
        } finally {
            if (acquired) {
                databaseSemaphore.release();
            }
        }
    }

    @Transactional(rollbackOn = RuntimeException.class)
    public void persistTransactionally(VehicleTelemetry event) {
        var eventId = UUID.fromString(event.getEventId());

        if (repository.findByIdOptional(eventId).isPresent()) {
            log.warn("### Evento duplicado. eventId={}, vehicleId={}",
                    event.getEventId(), event.getVehicleId());
            return;
        }

        var entity = new VehicleTelemetryEntity(
                eventId,
                event.getVehicleId(),
                Double.parseDouble(event.getLatitude()),
                Double.parseDouble(event.getLongitude()),
                Double.parseDouble(event.getSpeedKmh()),
                Instant.ofEpochMilli(
                        Long.parseLong(event.getOccurredAtEpochMillis())
                ),
                Instant.now()
        );

        repository.persistAndFlush(entity);

        log.debug("### Evento persistido. eventId={}, vehicleId={}",
                event.getEventId(), event.getVehicleId());
    }
}
