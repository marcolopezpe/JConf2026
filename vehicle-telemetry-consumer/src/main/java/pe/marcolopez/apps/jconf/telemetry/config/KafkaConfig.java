package pe.marcolopez.apps.jconf.telemetry.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.util.KafkaValues;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

@Singleton
public class KafkaConfig {

    @Inject
    KafkaValues kafkaValues;

    @Produces
    @Named("consumer")
    public Consumer<String, VehicleTelemetry> createConsumer() {
        var props = new Properties();

        /*
         * Kafka Bootstrap
         */
        props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaValues.bootstrapServers());

        /*
         * Deserializers for Keys and Values
         */
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());

        /*
         * Schema registry
         */
        props.putIfAbsent(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                kafkaValues.schema().registry().url());
        props.putIfAbsent(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS,
                kafkaValues.schema().registry().autoRegister());
        props.putIfAbsent(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION,
                kafkaValues.schema().registry().useLatestVersion());

        /*
         * Config for consumer
         */
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG,
                kafkaValues.consumer().groupId());
        props.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG,
                kafkaValues.consumer().clientId() + "-" + getHostname());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                kafkaValues.consumer().auto().offset().reset());
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                kafkaValues.consumer().enable().auto().commit());
        props.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                kafkaValues.consumer().maxPollRecords());

        /*
         * Avro Reader
         */
        props.putIfAbsent(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
                kafkaValues.consumer().avro().reader());

        return new KafkaConsumer<>(props);
    }

    private String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "UnknownHost";
        }
    }
}
