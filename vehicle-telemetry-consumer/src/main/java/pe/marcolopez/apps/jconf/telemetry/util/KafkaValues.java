package pe.marcolopez.apps.jconf.telemetry.util;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "application.kafka-values")
public interface KafkaValues {

    String topic();

    String bootstrapServers();

    Schema schema();

    Consumer consumer();

    interface Schema {

        Registry registry();

        interface Registry {

            String url();

            boolean autoRegister();

            boolean useLatestVersion();
        }
    }

    interface Consumer {

        String groupId();

        String clientId();

        int maxPollRecords();

        Avro avro();

        Auto auto();

        Enable enable();

        interface Avro {

            boolean reader();
        }

        interface Auto {

            Offset offset();

            interface Offset {

                String reset();
            }
        }

        interface Enable {

            AutoCommit auto();

            interface AutoCommit {

                boolean commit();
            }
        }
    }
}
