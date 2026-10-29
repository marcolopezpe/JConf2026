package pe.marcolopez.apps.jconf.telemetry.util;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "application.internal-values")
public interface InternalValues {

    int retries();

    Semaphore semaphore();

    interface Semaphore {

        int permits();
    }
}
