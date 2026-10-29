package pe.marcolopez.apps.jconf.telemetry.service;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import pe.marcolopez.apps.jconf.telemetry.resource.dto.LoadTestResponse;
import pe.marcolopez.apps.jconf.telemetry.resource.dto.VehicleTelemetryInput;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Genera telemetria sintetica y la publica con el MISMO servicio que usa el endpoint normal.
 * Solo existe en memoria un lote (1,000) a la vez; nunca una lista gigante.
 */
@ApplicationScoped
public class LoadTestService {

    static final int BATCH_SIZE = 1_000;
    static final int VEHICLES = 100;

    // UUID v7: contiene el tiempo, ordenable aproximadamente. El JDK no lo trae, de ahi la libreria.
    private static final TimeBasedEpochGenerator UUID_V7 = Generators.timeBasedEpochGenerator();

    private static final String[] VEHICLE_IDS = new String[VEHICLES];

    static {
        for (int i = 0; i < VEHICLES; i++) {
            VEHICLE_IDS[i] = "VH-LIM-%03d".formatted(i + 1);
        }
    }

    // Centro de Lima
    private static final double BASE_LAT = -12.046374;
    private static final double BASE_LON = -77.042793;

    @Inject
    VehicleTelemetryService service;

    public LoadTestResponse run(int totalEvents) {
        long start = System.nanoTime();
        int generated = 0;
        int published = 0;
        int failed = 0;
        int batches = 0;

        while (generated < totalEvents) {
            int size = Math.min(BATCH_SIZE, totalEvents - generated);
            var batch = new ArrayList<VehicleTelemetryInput>(size);

            for (int i = 0; i < size; i++) {
                batch.add(randomEvent(generated + i));
            }

            var result = service.publishAsync(batch);
            published += result.published();
            failed += result.failed();
            generated += size;
            batches++;
        }

        long durationMs = Math.max(1L, (System.nanoTime() - start) / 1_000_000L);
        double throughput = published * 1000.0 / durationMs;

        return new LoadTestResponse(totalEvents, published, failed, batches, durationMs,
                Math.round(throughput * 100.0) / 100.0);
    }

    private VehicleTelemetryInput randomEvent(int sequence) {
        var rnd = ThreadLocalRandom.current();
        return new VehicleTelemetryInput(
                UUID_V7.generate().toString(),
                VEHICLE_IDS[sequence % VEHICLES],
                BASE_LAT + rnd.nextDouble(-0.05, 0.05),
                BASE_LON + rnd.nextDouble(-0.05, 0.05),
                Math.round(rnd.nextDouble(0, 90) * 10.0) / 10.0,
                Instant.now()
        );
    }
}