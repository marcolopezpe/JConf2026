package pe.marcolopez.apps.jconf.telemetry.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.util.KafkaValues;
import java.util.Properties;

@Singleton
public class KafkaConfig {

    @Inject
    KafkaValues kafkaValues;

    @Produces
    @Named("producer")
    public Producer<String, VehicleTelemetry> createProducer() {
        Properties props = new Properties();

        /*
         * Kafka Bootstrap
         */
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaValues.bootstrapServers());

        /*
         * Deserializers for Keys and Values
         */
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        /*
         * Schema registry
         */
        props.put(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                kafkaValues.schema().registry().url()
        );
        props.put(
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                kafkaValues.schema().registry().autoRegister()
        );
        props.put(
                AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
                kafkaValues.schema().registry().useLatestVersion()
        );

        /*
         * Others configs
         */
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return new KafkaProducer<>(props);
    }
}
