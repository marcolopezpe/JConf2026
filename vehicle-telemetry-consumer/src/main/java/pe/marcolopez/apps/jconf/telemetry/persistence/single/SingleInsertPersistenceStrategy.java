package pe.marcolopez.apps.jconf.telemetry.persistence.single;

import io.agroal.api.AgroalDataSource;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.persistence.PersistenceMode;
import pe.marcolopez.apps.jconf.telemetry.persistence.PersistenceResult;
import pe.marcolopez.apps.jconf.telemetry.persistence.TelemetryPersistenceException;
import pe.marcolopez.apps.jconf.telemetry.persistence.TelemetryPersistenceStrategy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * INICIATIVA SINGLE (linea base). Por cada evento:
 *   conexion del pool -> transaccion -> UN INSERT ... ON CONFLICT DO NOTHING -> COMMIT.
 * N eventos = N viajes a PostgreSQL y N commits. Es lo que batch y copy van a reducir.
 */
@Slf4j
@Unremovable
@ApplicationScoped
public class SingleInsertPersistenceStrategy implements TelemetryPersistenceStrategy {

    private static final String INSERT_SQL = """
            INSERT INTO vehicle_telemetry
                (event_id, vehicle_id, latitude, longitude, speed_kmh, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    @Inject
    AgroalDataSource dataSource;

    @Override
    public PersistenceMode mode() {
        return PersistenceMode.SINGLE;
    }

    @Override
    public PersistenceResult persist(List<VehicleTelemetry> events) {
        int persisted = 0;
        int duplicated = 0;
        int failed = 0;

        for (var event : events) {
            try {
                if (insertOne(event) == 1) {
                    persisted++;
                } else {
                    duplicated++;   // el eventId ya existia: idempotencia en accion
                }
            } catch (IllegalArgumentException | NullPointerException e) {
                // Dato malo de UN evento (UUID invalido, campo nulo): reintentar no lo arregla.
                // Se aisla, se cuenta y el resto del lote sigue. (Pendiente: enviarlo a la DLQ.)
                failed++;
                log.warn("### Evento invalido descartado (eventId={}): {}", safeId(event), e.getMessage());
            } catch (SQLException e) {
                if (isPermanent(e)) {
                    failed++;
                    log.warn("### Evento rechazado por la base (eventId={}, sqlState={}): {}",
                            safeId(event), e.getSQLState(), e.getMessage());
                } else {
                    throw new TelemetryPersistenceException(
                            "PostgreSQL no disponible o con error transitorio: " + e.getMessage(),
                            e,
                            new PersistenceResult(persisted, duplicated, failed));
                }
            }
        }

        return new PersistenceResult(persisted, duplicated, failed);
    }

    /** Una conexion, una transaccion, un INSERT, un COMMIT. Devuelve filas afectadas (1 = nuevo, 0 = duplicado). */
    private int insertOne(VehicleTelemetry event) throws SQLException {
        UUID eventId = UUID.fromString(event.getEventId());

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
                ps.setObject(1, eventId);
                ps.setString(2, event.getVehicleId());
                ps.setDouble(3, Double.parseDouble(event.getLatitude()));
                ps.setDouble(4, Double.parseDouble(event.getLongitude()));
                ps.setDouble(5, Double.parseDouble(event.getSpeedKmh()));
                ps.setTimestamp(6, Timestamp.from(Instant.ofEpochMilli(Long.parseLong(event.getOccurredAtEpochMillis()))));

                int rows = ps.executeUpdate();
                connection.commit();
                return rows;
            } catch (SQLException e) {
                rollbackQuietly(connection);
                throw e;
            }
        }
    }

    private boolean isPermanent(SQLException e) {
        String state = e.getSQLState();
        return state != null && (state.startsWith("22") || state.startsWith("23"));
    }

    private void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // la conexion probablemente ya esta rota; el pool la descartara
        }
    }

    private String safeId(VehicleTelemetry event) {
        return event == null ? "null" : event.getEventId();
    }
}