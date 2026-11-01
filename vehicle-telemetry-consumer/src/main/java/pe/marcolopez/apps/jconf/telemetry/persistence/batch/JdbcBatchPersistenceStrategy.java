package pe.marcolopez.apps.jconf.telemetry.persistence.batch;

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
import pe.marcolopez.apps.jconf.telemetry.persistence.single.SingleInsertPersistenceStrategy;
import pe.marcolopez.apps.jconf.telemetry.util.TelemetryValues;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * INICIATIVA BATCH. Misma tabla y MISMO SQL que single; cambia como se envia:
 *
 *   single: N eventos -> N viajes a PostgreSQL y N commits
 *   batch : N eventos -> addBatch() x N, executeBatch() cada telemetry.batch.size,
 *           UN solo commit por poll (una transaccion para todo el lote)
 *
 * Idempotencia: ON CONFLICT (event_id) DO NOTHING. Los update counts de executeBatch()
 * dicen que paso con cada fila: 1 = insertada, 0 = duplicada. Por eso NO se activa
 * reWriteBatchedInserts: con esa opcion pgjdbc devuelve SUCCESS_NO_INFO (-2) y se
 * perderia la distincion persisted / duplicated que necesita benchmark_run.
 *
 * Errores:
 *   - dato invalido de un evento (UUID, vehicleId)  -> se excluye antes del lote, failed++
 *   - la base rechaza el lote por un dato (clase 22/23) -> rollback y se reprocesa evento por
 *     evento con la estrategia single para aislar al culpable (el resto SI se guarda)
 *   - infraestructura (conexion, pool, timeout)     -> rollback y TelemetryPersistenceException;
 *     el lote es atomico, asi que no hay nada que conservar y el consumer reintenta todo
 */
@Slf4j
@Unremovable
@ApplicationScoped
public class JdbcBatchPersistenceStrategy implements TelemetryPersistenceStrategy {

    private static final String INSERT_SQL = """
            INSERT INTO vehicle_telemetry
                (event_id, vehicle_id, latitude, longitude, speed_kmh, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (event_id) DO NOTHING
            """;

    private static final int MAX_VEHICLE_ID_LENGTH = 50;   // VARCHAR(50)

    @Inject
    AgroalDataSource dataSource;

    @Inject
    TelemetryValues values;

    /** Solo se usa para aislar un evento venenoso; no participa en el camino normal. */
    @Inject
    SingleInsertPersistenceStrategy isolationFallback;

    @Override
    public PersistenceMode mode() {
        return PersistenceMode.BATCH;
    }

    @Override
    public PersistenceResult persist(List<VehicleTelemetry> events) {
        // 1) Validar y preparar fuera de la transaccion: lo invalido nunca entra al lote.
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

        // 2) Un lote, una transaccion.
        try {
            Counts counts = insertInBatches(rows);
            return new PersistenceResult(counts.persisted, counts.duplicated, invalid);
        } catch (SQLException e) {
            if (isPermanent(e)) {
                log.warn("### PostgreSQL rechazo el lote por un dato (sqlState={}). "
                        + "Se reprocesa evento por evento para aislarlo: {}", sqlState(e), e.getMessage());
                // Se pasa la lista ORIGINAL: asi el avance parcial que reporte coincide con lo que reintenta el consumer.
                return isolationFallback.persist(events);
            }
            throw new TelemetryPersistenceException(
                    "PostgreSQL no disponible o con error transitorio: " + e.getMessage(),
                    e,
                    PersistenceResult.EMPTY);   // lote atomico: no se confirmo nada
        }
    }

    private Counts insertInBatches(List<Row> rows) throws SQLException {
        int batchSize = Math.max(1, values.batch().size());
        var counts = new Counts();

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try (PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
                int pending = 0;

                for (var row : rows) {
                    bind(ps, row);
                    ps.addBatch();
                    pending++;

                    if (pending == batchSize) {
                        counts.add(ps.executeBatch());
                        pending = 0;
                    }
                }
                if (pending > 0) {
                    counts.add(ps.executeBatch());
                }

                connection.commit();   // UN commit para todo el poll
                return counts;
            } catch (SQLException e) {
                rollbackQuietly(connection);
                throw e;
            }
        }
    }

    private void bind(PreparedStatement ps, Row row) throws SQLException {
        var event = row.event();
        ps.setObject(1, row.eventId());
        ps.setString(2, event.getVehicleId());
        ps.setDouble(3, Double.parseDouble(event.getLatitude()));
        ps.setDouble(4, Double.parseDouble(event.getLongitude()));
        ps.setDouble(5, Double.parseDouble(event.getSpeedKmh()));
        ps.setTimestamp(6, row.occurredAt());
    }

    /** Clase 22 (dato invalido) y 23 (integridad): reintentar el mismo lote no lo arregla. */
    private boolean isPermanent(SQLException e) {
        String state = sqlState(e);
        return state != null && (state.startsWith("22") || state.startsWith("23"));
    }

    /** BatchUpdateException a veces trae el SQLState en la excepcion encadenada. */
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

    /** Evento ya validado y convertido; si algo falla, falla aqui y no dentro de la transaccion. */
    private record Row(UUID eventId, VehicleTelemetry event, Timestamp occurredAt) {

        static Row of(VehicleTelemetry event) {
            UUID eventId = UUID.fromString(event.getEventId());

            String vehicleId = event.getVehicleId();
            if (vehicleId == null || vehicleId.isBlank() || vehicleId.length() > MAX_VEHICLE_ID_LENGTH) {
                throw new IllegalArgumentException("vehicleId vacio o mayor a " + MAX_VEHICLE_ID_LENGTH);
            }

            return new Row(eventId, event, Timestamp.from(Instant.ofEpochMilli(Long.parseLong(event.getOccurredAtEpochMillis()))));
        }
    }

    /** Acumula los update counts de executeBatch(): 1 = insertada, 0 = duplicada. */
    private static final class Counts {

        int persisted;
        int duplicated;
        private boolean warned;

        void add(int[] updateCounts) {
            for (int count : updateCounts) {
                if (count == 0) {
                    duplicated++;
                } else {
                    persisted++;   // 1, o SUCCESS_NO_INFO (ver abajo)
                    if (count == Statement.SUCCESS_NO_INFO && !warned) {
                        warned = true;
                        log.warn("### El driver devolvio SUCCESS_NO_INFO: no se puede distinguir persistidos de "
                                + "duplicados. Revisa si la URL JDBC tiene reWriteBatchedInserts=true.");
                    }
                }
            }
        }
    }
}