package com.network.preprocess.sink;

import com.network.preprocess.config.SilverJobConfig;
import com.network.preprocess.model.InvalidIdentityRecord;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.UnsupportedEventRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Objects;

/**
 * Factory tạo bốn Kafka sink của Silver Job.
 *
 * <ul>
 *     <li>Main output: silver.ue.event.</li>
 *     <li>Invalid identity: invalid-identity.</li>
 *     <li>Unsupported event: unsupported-event.</li>
 *     <li>Late event: late-ue-event.</li>
 * </ul>
 *
 * <p>Mỗi sink dùng transactional ID prefix riêng để các transaction
 * không xung đột với nhau.</p>
 */
public final class SilverKafkaSinks {

    private SilverKafkaSinks() {
    }

    /**
     * Sink ghi SilverEvent hợp lệ.
     *
     * <p>Dùng ueKey làm Kafka key để event của cùng UE được Kafka
     * partitioner đưa về cùng partition. Điều này có ích cho Gold Job
     * khi cần giữ thứ tự event của từng UE.</p>
     */
    public static KafkaSink<SilverEvent> eventSink(
            SilverJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return createSink(
                config.bootstrapServers(),
                config.outputTopic(),
                config.outputTransactionalIdPrefix(),
                SilverEvent::ueKey
        );
    }

    /**
     * Sink ghi record không resolve được identity.
     */
    public static KafkaSink<InvalidIdentityRecord>
    invalidIdentitySink(
            SilverJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return createSink(
                config.bootstrapServers(),
                config.invalidIdentityTopic(),
                config.invalidIdentityTransactionalIdPrefix(),
                InvalidIdentityRecord::invalidIdentityId
        );
    }

    /**
     * Sink ghi EVENT_ID không được model hỗ trợ.
     */
    public static KafkaSink<UnsupportedEventRecord>
    unsupportedEventSink(
            SilverJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return createSink(
                config.bootstrapServers(),
                config.unsupportedEventTopic(),
                config.unsupportedEventTransactionalIdPrefix(),
                UnsupportedEventRecord::unsupportedEventId
        );
    }

    /**
     * Sink ghi SilverEvent đến quá trễ.
     *
     * <p>Late event vẫn là SilverEvent hợp lệ về schema và identity.
     * Nó chỉ không còn hợp lệ về thời gian đối với tiến độ watermark.</p>
     */
    public static KafkaSink<SilverEvent> lateEventSink(
            SilverJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return createSink(
                config.bootstrapServers(),
                config.lateEventTopic(),
                config.lateEventTransactionalIdPrefix(),
                SilverEvent::ueKey
        );
    }

    /**
     * Tạo Kafka sink dùng chung.
     *
     * @param bootstrapServers Kafka broker
     * @param topic topic đích
     * @param transactionalIdPrefix prefix transaction riêng của sink
     * @param keyExtractor hàm lấy Kafka key từ event
     */
    private static <T> KafkaSink<T> createSink(
            String bootstrapServers,
            String topic,
            String transactionalIdPrefix,
            JsonKafkaRecordSerializationSchema.KeyExtractor<T>
                    keyExtractor
    ) {
        return KafkaSink
                .<T>builder()

                .setBootstrapServers(
                        bootstrapServers
                )

                /*
                 * Serialize object thành JSON snake_case
                 * và tạo ProducerRecord có Kafka key.
                 */
                .setRecordSerializer(
                        new JsonKafkaRecordSerializationSchema<>(
                                topic,
                                keyExtractor
                        )
                )

                /*
                 * Kafka transaction chỉ được commit khi checkpoint
                 * tương ứng của Flink hoàn thành.
                 */
                .setDeliveryGuarantee(
                        DeliveryGuarantee.EXACTLY_ONCE
                )

                /*
                 * Prefix phải duy nhất giữa:
                 * - bốn Silver sink;
                 * - các Silver Job đang chạy song song;
                 * - các application khác trên cùng Kafka cluster.
                 */
                .setTransactionalIdPrefix(
                        transactionalIdPrefix
                )

                .build();
    }
}