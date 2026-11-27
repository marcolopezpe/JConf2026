package pe.marcolopez.apps.jconf.telemetry.benchmark;

import io.agroal.api.AgroalDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import pe.marcolopez.apps.jconf.telemetry.persistence.PersistenceResult;
import pe.marcolopez.apps.jconf.telemetry.util.TelemetryValues;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Mide una "corrida" completa y la guarda en benchmark_run (una fila por corrida).
 *
 * Una corrida empieza cuando se procesa el primer lote y termina cuando el consumer lleva
 * telemetry.benchmark.idle-timeout-ms sin recibir nada (lag = 0).
 *
 * duracion = fin del ultimo lote - inicio del primer lote  (no incluye el tiempo ocioso de espera)
 * throughput = eventos persistidos / duracion en segundos
 *
 * Solo lo usa el hilo del consumer, por eso no necesita sincronizacion.
 */
@Slf4j
@ApplicationScoped
public class BenchmarkRecorder {

    private static final String INSERT_SQL = """
            INSERT INTO benchmark_run
                (strategy, total_events, persisted_events, duplicated_events, failed_events,
                 workers, jdbc_pool_size, started_at, finished_at, duration_ms,
                 throughput_events_per_second, virtual_threads, chunk_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    @Inject
    AgroalDataSource dataSource;

    @ConfigProperty(name = "quarkus.datasource.jdbc.max-size", defaultValue = "20")
    int jdbcPoolSize;

    @Inject
    TelemetryValues values;

    private String strategy = "unknown";
    private int workers = 1;
    private boolean virtualThreads = false;
    private int chunkSize = 0;   // 0 = no aplica (sin Virtual Threads)

    // Estado de la corrida en curso
    private boolean active;
    private Instant startedAt;
    private long firstStartNanos;
    private long lastEndNanos;
    private long persisted;
    private long duplicated;
    private long failed;
    private long batches;
    private long sumBatchNanos;
    private long maxBatchNanos;

    public void configure(String strategy, int workers) {
        configure(strategy, workers, false, 0);
    }

    /** virtualThreads y chunkSize quedan en benchmark_run para distinguir cada corrida. */
    public void configure(String strategy, int workers, boolean virtualThreads, int chunkSize) {
        this.strategy = strategy;
        this.workers = workers;
        this.virtualThreads = virtualThreads;
        this.chunkSize = chunkSize;
    }

    /** Registra un lote (un poll) ya persistido y confirmado. */
    public void recordBatch(Instant batchStartedAt, long startNanos, long endNanos, PersistenceResult result) {
        begin(batchStartedAt, startNanos);

        persisted += result.persisted();
        duplicated += result.duplicated();
        failed += result.failed();
        batches++;

        long batchNanos = endNanos - startNanos;
        sumBatchNanos += batchNanos;
        maxBatchNanos = Math.max(maxBatchNanos, batchNanos);
        lastEndNanos = endNanos;
    }

    /** Eventos que ni siquiera llegaron a la estrategia (mensaje invalido, valor nulo). */
    public void addFailed(int count) {
        begin(Instant.now(), System.nanoTime());
        failed += count;
    }

    /** Se llama cuando poll() vuelve vacio: si ya paso el tiempo ocioso, cierra la corrida. */
    public void closeIfIdle() {
        if (active && (System.nanoTime() - lastEndNanos) / 1_000_000L >= values.benchmark().idleTimeoutMs()) {
            flush();
        }
    }

    /** Cierra la corrida en curso y la guarda. Si PostgreSQL falla, conserva el estado y reintenta luego. */
    public void flush() {
        if (!active) {
            return;
        }

        long durationNanos = Math.max(1L, lastEndNanos - firstStartNanos);
        long durationMs = Math.max(1L, durationNanos / 1_000_000L);
        long total = persisted + duplicated + failed;
        BigDecimal throughput = BigDecimal.valueOf(persisted * 1000.0 / durationMs)
                .setScale(2, RoundingMode.HALF_UP);
        Instant finishedAt = startedAt.plusNanos(durationNanos);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(INSERT_SQL)) {
            ps.setString(1, strategy);
            ps.setInt(2, (int) total);
            ps.setInt(3, (int) persisted);
            ps.setInt(4, (int) duplicated);
            ps.setInt(5, (int) failed);
            ps.setInt(6, workers);
            ps.setInt(7, jdbcPoolSize);
            ps.setTimestamp(8, Timestamp.from(startedAt));
            ps.setTimestamp(9, Timestamp.from(finishedAt));
            ps.setLong(10, durationMs);
            ps.setBigDecimal(11, throughput);
            ps.setBoolean(12, virtualThreads);
            ps.setInt(13, chunkSize);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("### No se pudo guardar benchmark_run (se reintentara): {}", e.getMessage());
            return;
        }

        log.info("""

                ### ===================== RESULTADO DE LA CORRIDA =====================
                ### strategy={}  workers={}  jdbc_pool_size={}  virtual_threads={}  chunk_size={}
                ### total={}  persisted={}  duplicated={}  failed={}
                ### duration_ms={}  throughput={} eventos/s
                ### ms_por_evento={}  lotes={}  ms_promedio_lote={}  ms_max_lote={}
                ### ===================================================================""",
                strategy, workers, jdbcPoolSize, virtualThreads, chunkSize,
                total, persisted, duplicated, failed,
                durationMs, throughput,
                total == 0 ? "-" : String.format("%.3f", (double) durationMs / total),
                batches,
                batches == 0 ? "-" : String.format("%.1f", sumBatchNanos / 1_000_000.0 / batches),
                String.format("%.1f", maxBatchNanos / 1_000_000.0));

        reset();
    }

    private void begin(Instant now, long nowNanos) {
        if (!active) {
            active = true;
            startedAt = now;
            firstStartNanos = nowNanos;
            lastEndNanos = nowNanos;
        }
    }

    private void reset() {
        active = false;
        startedAt = null;
        persisted = 0;
        duplicated = 0;
        failed = 0;
        batches = 0;
        sumBatchNanos = 0;
        maxBatchNanos = 0;
    }
}