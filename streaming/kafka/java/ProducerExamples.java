package examples.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Curated from the legacy KafkaFlow repository.
 *
 * Demonstrates both unkeyed and keyed Kafka records while keeping broker/topic
 * configuration externalisable through environment variables.
 */
public final class ProducerExamples {
    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");
    private static final String TOPIC =
            System.getenv().getOrDefault("KAFKA_TOPIC", "movie_topic");

    private ProducerExamples() {}

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 5; i++) {
                producer.send(
                    new ProducerRecord<>(TOPIC, "unkeyed-message-" + i),
                    (metadata, error) -> {
                        if (error != null) {
                            error.printStackTrace();
                            return;
                        }
                        System.out.printf(
                            "topic=%s partition=%d offset=%d%n",
                            metadata.topic(), metadata.partition(), metadata.offset()
                        );
                    }
                );
            }

            for (int i = 0; i < 5; i++) {
                String key = "id-" + i;
                producer.send(new ProducerRecord<>(TOPIC, key, "keyed-message-" + i));
            }

            producer.flush();
        }
    }
}
