package pe.marcolopez.apps.jconf.telemetry.messaging;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.kafka.clients.producer.Producer;
import pe.marcolopez.apps.jconf.telemetry.avro.VehicleTelemetry;
import pe.marcolopez.apps.jconf.telemetry.util.KafkaValues;

@ApplicationScoped
public class TelemetryKafkaProducer {

    @Inject
    KafkaValues kafkaValues;

    @Inject
    @Named("producer")
    Producer<String, VehicleTelemetry> producer;
}
