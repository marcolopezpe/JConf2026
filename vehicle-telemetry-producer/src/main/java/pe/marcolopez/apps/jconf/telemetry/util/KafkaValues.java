package pe.marcolopez.apps.jconf.telemetry.util;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "application.kafka-values")
public interface KafkaValues {

    String topic();

    String bootstrapServers();

    Schema schema();

    interface Schema {

        Registry registry();

        interface Registry {

            String url();

            boolean autoRegister();

            boolean useLatestVersion();
        }
    }
}
