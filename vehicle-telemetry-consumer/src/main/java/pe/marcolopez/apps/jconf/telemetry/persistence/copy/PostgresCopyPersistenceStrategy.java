package pe.marcolopez.apps.jconf.telemetry.persistence.copy;

import io.agroal.api.AgroalDataSource;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.persistence.PersistenceMode;
import pe.marcolopez.apps.jconf.telemetry.persistence.PersistenceResult;
import pe.marcolopez.apps.jconf.telemetry.persistence.TelemetryPersistenceException;
import pe.marcolopez.apps.jconf.telemetry.persistence.TelemetryPersistenceStrategy;
import pe.marcolopez.apps.jconf.telemetry.persistence.single.SingleInsertPersistenceStrategy;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * INICIATIVA COPY. Misma tabla final que single y batch; cambia como entran los datos:
 *   copy: N eventos -> UN flujo COPY ... FROM STDIN hacia una tabla temporal,
 *         luego UN INSERT ... SELECT ... ON CONFLICT DO NOTHING hacia la tabla final,
 *         un commit por poll.
 * La temporal es ON COMMIT DROP: la crea esta clase en cada transaccion y no hay nada que
 * preparar en el init. COPY no admite ON CONFLICT, por eso no se copia directo a la final.
 * Conteos: persisted = filas que inserto el INSERT ... SELECT; duplicated = enviadas - persisted.
 * Fecha: el stage guarda epoch en ms (BIGINT) y se convierte en SQL con to_timestamp().
 * Errores (igual que batch): dato invalido -> se excluye antes; la base rechaza por un dato
 * (22/23) -> rollback y se reprocesa con single; infraestructura -> TelemetryPersistenceException.
 */
@Slf4j
@Unremovable
@ApplicationScoped
public class PostgresCopyPersistenceStrategy implements TelemetryPersistenceStrategy {

    private static final String CREATE_STAGE_SQL = """
            CREATE TEMP TABLE telemetry_stage (
                event_id        UUID,
                vehicle_id      VARCHAR(50),
                latitude        DOUBLE PRECISION,
                longitude       DOUBLE PRECISION,
                speed_kmh       DOUBLE PRECISION,
                occurred_at_ms  BIGINT
            ) ON COMMIT DROP
            """;

    private static final String COPY_SQL = """
            COPY telemetry_stage (event_id, vehicle_id, latitude, longitude, speed_kmh, occurred_at_ms)
            FROM STDIN WITH (FORMAT csv)
            """;

    private static final String MERGE_SQL = """
            INSERT INTO vehicle_telemetry
                (event_id, vehicle_id, latitude, longitude, speed_kmh, occurred_at)
            SELECT event_id, vehicle_id, latitude, longitude, speed_kmh,
                   to_timestamp(occurred_at_ms / 1000.0)
            FROM telemetry_stage
            ON CONFLICT (event_id) DO NOTHING
            """;

    private static final int MAX_VEHICLE_ID_LENGTH = 50;   // VARCHAR(50)

    @Inject
    AgroalDataSource dataSource;

    /** Solo se usa para aislar un evento venenoso; no participa en el camino normal. */
    @Inject
    SingleInsertPersistenceStrategy isolationFallback;

    @Override
    public PersistenceMode mode() {
        return PersistenceMode.COPY;
    }

    @Override
    public PersistenceResult persist(List<VehicleTelemetry> events) {
        var rows = new ArrayList<Row>(events.size());
        int invalid = 0;

        for (var event : events) {
            try {
                rows.add(Row.of(event));
            } catch (IllegalArgumentException | NullPointerException e) {
                invalid++;
                log.warn("### Evento invalido descartado (eventId={}): {}",
                        event == null ? "null" : event.getEventId(), e.getMessage());
            }
        }

        if (rows.isEmpty()) {
            return new PersistenceResult(0, 0, invalid);
        }

        try {
            int inserted = copyAndMerge(rows);
            return new PersistenceResult(inserted, rows.size() - inserted, invalid);
        } catch (SQLException e) {
            if (isPermanent(e)) {
                log.warn("### PostgreSQL rechazo el COPY por un dato (sqlState={}). "
                        + "Se reprocesa evento por evento para aislarlo: {}", sqlState(e), e.getMessage());
                return isolationFallback.persist(events);
            }
            throw new TelemetryPersistenceException(
                    "PostgreSQL no disponible o con error transitorio: " + e.getMessage(),
                    e,
                    PersistenceResult.EMPTY);
        }
    }

    /** @return filas realmente insertadas en vehicle_telemetry (las duplicadas no cuentan). */
    private int copyAndMerge(List<Row> rows) throws SQLException {
        String csv = toCsv(rows);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement()) {
                statement.execute(CREATE_STAGE_SQL);

                CopyManager copyManager = connection.unwrap(PGConnection.class).getCopyAPI();
                copyManager.copyIn(COPY_SQL, new StringReader(csv));

                int inserted = statement.executeUpdate(MERGE_SQL);

                connection.commit();   // UN commit para todo el poll
                return inserted;
            } catch (IOException e) {
                rollbackQuietly(connection);
                throw new SQLException("Error enviando el flujo COPY: " + e.getMessage(), e);
            } catch (SQLException e) {
                rollbackQuietly(connection);
                throw e;
            }
        }
    }

    /** CSV en el mismo orden que COPY_SQL. El vehicleId va entre comillas por si trae comas o comillas. */
    private String toCsv(List<Row> rows) {
        var sb = new StringBuilder(rows.size() * 110);
        for (var row : rows) {
            var event = row.event();
            sb.append(row.eventId()).append(',')
                    .append('"').append(event.getVehicleId().replace("\"", "\"\"")).append('"').append(',')
                    .append(event.getLatitude()).append(',')
                    .append(event.getLongitude()).append(',')
                    .append(event.getSpeedKmh()).append(',')
                    .append(event.getOccurredAtEpochMillis())
                    .append('\n');
        }
        return sb.toString();
    }

    private boolean isPermanent(SQLException e) {
        String state = sqlState(e);
        return state != null && (state.startsWith("22") || state.startsWith("23"));
    }

    private String sqlState(SQLException e) {
        String state = e.getSQLState();
        if (state == null && e.getNextException() != null) {
            state = e.getNextException().getSQLState();
        }
        return state;
    }

    private void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // la conexion probablemente ya esta rota; el pool la descartara
        }
    }

    private record Row(UUID eventId, VehicleTelemetry event) {

        static Row of(VehicleTelemetry event) {
            UUID eventId = UUID.fromString(event.getEventId());

            String vehicleId = event.getVehicleId();
            if (vehicleId == null || vehicleId.isBlank() || vehicleId.length() > MAX_VEHICLE_ID_LENGTH) {
                throw new IllegalArgumentException("vehicleId vacio o mayor a " + MAX_VEHICLE_ID_LENGTH);
            }
            return new Row(eventId, event);
        }
    }
}