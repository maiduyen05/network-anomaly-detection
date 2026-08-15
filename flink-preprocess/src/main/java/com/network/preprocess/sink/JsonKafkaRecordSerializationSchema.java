package com.network.preprocess.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Serialize Java object thành Kafka ProducerRecord.
 *
 * Hỗ trợ:
 * - JSON snake_case;
 * - Kafka key;
 * - optional explicit Kafka partition.
 *
 * Nếu partitionExtractor = null thì Kafka producer tự chọn partition.
 */
public final class JsonKafkaRecordSerializationSchema<T>
        implements KafkaRecordSerializationSchema<T> {

    @FunctionalInterface
    public interface KeyExtractor<T> extends Serializable {
        String extract(T value);
    }

    @FunctionalInterface
    public interface PartitionExtractor<T> extends Serializable {
        Integer extract(T value);
    }

    private final String topic;
    private final KeyExtractor<T> keyExtractor;
    private final PartitionExtractor<T> partitionExtractor;

    private transient ObjectMapper objectMapper;

    /**
     * Constructor cũ:
     * Kafka tự chọn partition.
     */
    public JsonKafkaRecordSerializationSchema(
            String topic,
            KeyExtractor<T> keyExtractor
    ) {
        this(
                topic,
                keyExtractor,
                null
        );
    }

    /**
     * Constructor mới:
     * caller có thể chỉ định Kafka partition.
     */
    public JsonKafkaRecordSerializationSchema(
            String topic,
            KeyExtractor<T> keyExtractor,
            PartitionExtractor<T> partitionExtractor
    ) {
        this.topic = topic;
        this.keyExtractor = keyExtractor;
        this.partitionExtractor = partitionExtractor;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            T element,
            KafkaSinkContext context,
            Long timestamp
    ) {
        try {
            String key = keyExtractor.extract(
                    element
            );

            byte[] keyBytes =
                    key == null
                            ? null
                            : key.getBytes(
                                    StandardCharsets.UTF_8
                            );

            byte[] valueBytes =
                    mapper().writeValueAsBytes(
                            element
                    );

            Integer partition = null;

            if (partitionExtractor != null) {
                partition =
                        partitionExtractor.extract(
                                element
                        );

                if (partition != null
                        && partition < 0) {
                    throw new IllegalStateException(
                            "Kafka partition must not be negative: "
                                    + partition
                    );
                }
            }

            return new ProducerRecord<>(
                    topic,
                    partition,
                    null,
                    keyBytes,
                    valueBytes
            );

        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Could not serialize Kafka output JSON",
                    exception
            );
        }
    }

    private ObjectMapper mapper() {
        if (objectMapper == null) {
            objectMapper =
                    new ObjectMapper();

            objectMapper
                    .setPropertyNamingStrategy(
                            PropertyNamingStrategies
                                    .SNAKE_CASE
                    );
        }

        return objectMapper;
    }
}