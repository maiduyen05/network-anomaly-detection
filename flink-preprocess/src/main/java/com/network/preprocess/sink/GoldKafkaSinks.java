package com.network.preprocess.sink;

import com.network.preprocess.config.GoldJobConfig;
import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceSample;
import com.network.preprocess.model.InvalidGoldFeatureRecord;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Objects;

/**
 * Factory tạo các Kafka sink của Gold Job.
 *
 * <p>Gold có ba output:</p>
 *
 * <ul>
 *     <li>GoldSequenceSample hợp lệ.</li>
 *     <li>GoldSequenceEvent đến quá trễ.</li>
 *     <li>InvalidGoldFeatureRecord vi phạm feature contract.</li>
 * </ul>
 */
public final class GoldKafkaSinks {

    private GoldKafkaSinks() {
    }

    /**
     * Sink chính ghi sample model-ready vào gold.ue.sequence.
     *
     * <p>Kafka key là ueKey để các sample của cùng một UE:</p>
     *
     * <ul>
     *     <li>Được đưa về cùng Kafka partition.</li>
     *     <li>Giữ thứ tự tương đối khi downstream đọc.</li>
     * </ul>
     *
     * <p>sampleId vẫn nằm trong JSON payload và định danh duy nhất
     * từng window.</p>
     */
    public static KafkaSink<GoldSequenceSample> sequenceSink(
            GoldJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return createSink(
                config.bootstrapServers(),
                config.outputTopic(),
                config.outputTransactionalIdPrefix(),
                GoldSequenceSample::ueKey
        );
    }

    /**
     * Sink ghi event đến sau watermark.
     */
    public static KafkaSink<GoldSequenceEvent> tooLateEventSink(
            GoldJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return createSink(
                config.bootstrapServers(),
                config.tooLateEventTopic(),
                config.tooLateEventTransactionalIdPrefix(),
                GoldSequenceEvent::ueKey
        );
    }

    /**
     * Sink ghi window không thể encode theo feature contract.
     */
    public static KafkaSink<InvalidGoldFeatureRecord>
    invalidFeatureSink(
            GoldJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return createSink(
                config.bootstrapServers(),
                config.invalidFeatureTopic(),
                config.invalidFeatureTransactionalIdPrefix(),

                /*
                 * InvalidGoldFeatureRecord hiện sử dụng JavaBean getter.
                 */
                InvalidGoldFeatureRecord::getUeKey
        );
    }

    /**
     * Tạo Kafka sink EXACTLY_ONCE dùng chung.
     *
     * @param bootstrapServers Kafka broker
     * @param topic topic đích
     * @param transactionalIdPrefix prefix riêng của sink
     * @param keyExtractor hàm lấy Kafka key
     * @param <T> kiểu record cần ghi
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

                /*
                 * Broker nhận output của Gold Job.
                 */
                .setBootstrapServers(
                        bootstrapServers
                )

                /*
                 * Serialize object thành JSON snake_case
                 * và gắn Kafka key.
                 */
                .setRecordSerializer(
                        new JsonKafkaRecordSerializationSchema<>(
                                topic,
                                keyExtractor
                        )
                )

                /*
                 * Record chỉ được Kafka consumer nhìn thấy
                 * sau khi Flink checkpoint tương ứng hoàn thành.
                 */
                .setDeliveryGuarantee(
                        DeliveryGuarantee.EXACTLY_ONCE
                )

                /*
                 * Mỗi sink phải có prefix riêng để không xung đột
                 * Kafka transactional.id.
                 */
                .setTransactionalIdPrefix(
                        transactionalIdPrefix
                )

                .build();
    }
}