package com.network.producer.kafka;

import com.network.producer.config.ProducerConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Objects;

/**
 * Tạo KafkaProducer từ ProducerConfiguration.
 * Chương trình nãy sẽ gửi dữ liệu vào Kafka topic 
 */
public final class KafkaProducerFactory {

    /**
     * Constructor private vì class chỉ có static method.
     */
    private KafkaProducerFactory() {
    }

    /**
     * Tạo KafkaProducer có key và value đều là String.
     *
     * @param configuration cấu hình producer
     * @return KafkaProducer hoàn chỉnh
     */
    public static KafkaProducer<String, String> create(
            ProducerConfiguration configuration
    ) {
        Objects.requireNonNull(
                configuration,
                "configuration must not be null"
        );

        return new KafkaProducer<>(
                configuration.kafkaProperties()
        );
    }
}
