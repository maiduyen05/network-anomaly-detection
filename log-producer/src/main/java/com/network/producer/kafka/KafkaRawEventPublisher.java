package com.network.producer.kafka;

import com.network.producer.model.RawNetworkEvent;
import com.network.producer.serialization.RawNetworkEventJsonSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;

/**
 * Gửi RawNetworkEvent vào Kafka raw topic.
 * Class này chịu trách nhiệm:
 *     - Tạo Kafka key
 *     - Serialize event thành JSON
 *     - Tạo ProducerRecord (object đại diện cho 1 message gửi vào Kafka)
 *     - Gửi record bất đồng bộ
 * Chú ý: không đọc file và không parse 52 trường (bước này thực hiện trong Flink)
 */
public final class KafkaRawEventPublisher
        implements AutoCloseable {

    /**
     * Kafka Producer interface.
     *
     * <p>Dùng interface Producer thay vì KafkaProducer cụ thể
     * để unit test có thể truyền MockProducer.</p>
     */
    private final Producer<String, String> producer;

    /**
     * Topic đích.
     */
    private final String topic;

    /**
     * Tạo JSON value.
     */
    private final RawNetworkEventJsonSerializer serializer;

    /**
     * Tạo Kafka key.
     */
    private final KafkaMessageKeyResolver keyResolver;

    /**
     * Khởi tạo publisher.
     */
    public KafkaRawEventPublisher(
            Producer<String, String> producer,
            String topic,
            RawNetworkEventJsonSerializer serializer,
            KafkaMessageKeyResolver keyResolver
    ) {
        this.producer = Objects.requireNonNull(
                producer,
                "producer must not be null"
        );

        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException(
                    "topic must not be blank"
            );
        }

        this.topic = topic.trim();

        this.serializer = Objects.requireNonNull(
                serializer,
                "serializer must not be null"
        );

        this.keyResolver = Objects.requireNonNull(
                keyResolver,
                "keyResolver must not be null"
        );
    }

    /**
     * publish: gửi 1 message vào 1 topic
     * Gửi một raw event tự động vào Kafka 
     * (RawEvent --> Tạo key --> Serialize JSON --> ProducerRecord --> producer.send())
     *
     * <p>producer.send() là bất đồng bộ. Method này chỉ đưa
     * record vào buffer của Kafka client và trả về ngay.</p>
     *
     * @param event raw event cần gửi
     * @param callback callback nhận kết quả thành công/thất bại
     */
    public void publish(
            RawNetworkEvent event,
            Callback callback
    ) {
        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        // Tạo Kafka key.
        String key =
                keyResolver.resolve(event);

        // Tạo JSON Kafka value.
        String value =
                serializer.serialize(event);

        // Tạo record cho topic đích.
        ProducerRecord<String, String> record =
                new ProducerRecord<>(
                        topic,
                        key,
                        value
                );

        // Gửi record theo cơ chế bất đồng bộ.
        producer.send(
                record,
                callback   // Kafka gửi thành công hoặc thất bại, callback sẽ được gọi để xử lý 
        );
    }

    /**
     * Sau khi gửi hết vẫn còn các log cuối trong buffer do chưa đủ số lượng 
     * flush() để gửi nốt các log cuối cùng để tránh mất message
     */
    public void flush() {
        producer.flush();
    }

    /**
     * Đóng Kafka producer để đảm bảo tài nguyên mạng được giải phóng
     */
    @Override
    public void close() {
        producer.close();
    }
}