package com.network.preprocess.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind
        .PropertyNamingStrategies;
import org.apache.flink.connector.kafka.sink
        .KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Chuyển 1 object java thành kafka message (Kafka ProducerRecord) trước khi ghi ra topic Kafka
 * Biến object Java thành JSON snake_case, tạo kafka key rồi đóng gói thành ProducerRecord để Kafka Sink gửi đi
 * 
 * KafkaRecordSerializationSchema: interface của Flink Kafka Sink
 * @param <T> kiểu event cần gửi Kafka
 */
public final class JsonKafkaRecordSerializationSchema<T>
        implements KafkaRecordSerializationSchema<T> {   

    /**
     * Hàm lấy Kafka key phải Serializable để Flink có thể chuyển
     * schema từ JobManager sang TaskManager.
     */
    @FunctionalInterface
    public interface KeyExtractor<T> extends Serializable {
        String extract(T value);
    }

    private final String topic;
    private final KeyExtractor<T> keyExtractor;

    /*
     * ObjectMapper không cần được Flink serialize.
     * Nó được tạo lại trên TaskManager khi dùng lần đầu.
     */
    private transient ObjectMapper objectMapper;

    public JsonKafkaRecordSerializationSchema(
            String topic,
            KeyExtractor<T> keyExtractor
    ) {
        this.topic = topic;
        this.keyExtractor = keyExtractor;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            T element,
            KafkaSinkContext context,
            Long timestamp
    ) {
        try {
            String key = keyExtractor.extract(element);

            byte[] keyBytes = key == null
                    ? null
                    : key.getBytes(StandardCharsets.UTF_8);

            byte[] valueBytes =
                    mapper().writeValueAsBytes(element);

            return new ProducerRecord<>(
                    topic,
                    null,
                    null,
                    keyBytes,
                    valueBytes
            );

        } catch (JsonProcessingException exception) {
            /*
             * Đây là lỗi serialize object nội bộ do code tạo ra.
             * Không route sang data DLQ vì input đã qua validation.
             */
            throw new IllegalStateException(
                    "Could not serialize Kafka output JSON",
                    exception
            );
        }
    }

    private ObjectMapper mapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();

            /*
             * schemaVersion -> schema_version
             * rawRecordId   -> raw_record_id
             */
            objectMapper.setPropertyNamingStrategy(
                    PropertyNamingStrategies.SNAKE_CASE
            );
        }

        return objectMapper;
    }
}