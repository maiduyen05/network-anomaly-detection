package com.network.preprocess.sink;

import com.network.preprocess.config.BronzeJobConfig;
import com.network.preprocess.model.BronzeDlqRecord;
import com.network.preprocess.model.BronzeEvent;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Objects;

/**
 * Factory tạo Kafka sink của Bronze.
 */
public final class BronzeKafkaSinks {

    private static final String TRANSACTION_TIMEOUT_MS =
            "900000";

    private static final String MAX_BLOCK_MS =
            "180000";

    private static final String REQUEST_TIMEOUT_MS =
            "60000";

    private static final String DELIVERY_TIMEOUT_MS =
            "300000";

    private BronzeKafkaSinks() {
    }

    public static KafkaSink<BronzeEvent> eventSink(
            BronzeJobConfig config
    ) {
        return KafkaSink
                .<BronzeEvent>builder()

                .setBootstrapServers(
                        config.bootstrapServers()
                )

                /*
                 * QUAN TRỌNG:
                 *
                 * rawRecordId vẫn là Kafka key,
                 * nhưng Bronze output giữ đúng partition
                 * của raw Kafka source.
                 */
                .setRecordSerializer(
                        new JsonKafkaRecordSerializationSchema<>(
                                config.outputTopic(),
                                BronzeEvent::rawRecordId,
                                BronzeKafkaSinks::sourcePartition
                        )
                )

                .setProperty(
                        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                        TRANSACTION_TIMEOUT_MS
                )

                .setProperty(
                        ProducerConfig.MAX_BLOCK_MS_CONFIG,
                        MAX_BLOCK_MS
                )

                .setProperty(
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                        REQUEST_TIMEOUT_MS
                )

                .setProperty(
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                        DELIVERY_TIMEOUT_MS
                )

                .setDeliveryGuarantee(
                        DeliveryGuarantee.EXACTLY_ONCE
                )

                .setTransactionalIdPrefix(
                        config.outputTransactionalIdPrefix()
                )

                .build();
    }

    public static KafkaSink<BronzeDlqRecord> dlqSink(
            BronzeJobConfig config
    ) {
        return KafkaSink
                .<BronzeDlqRecord>builder()

                .setBootstrapServers(
                        config.bootstrapServers()
                )

                .setRecordSerializer(
                        new JsonKafkaRecordSerializationSchema<>(
                                config.dlqTopic(),
                                BronzeDlqRecord::dlqId
                        )
                )

                .setProperty(
                        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                        TRANSACTION_TIMEOUT_MS
                )

                .setProperty(
                        ProducerConfig.MAX_BLOCK_MS_CONFIG,
                        MAX_BLOCK_MS
                )

                .setProperty(
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                        REQUEST_TIMEOUT_MS
                )

                .setProperty(
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                        DELIVERY_TIMEOUT_MS
                )

                .setDeliveryGuarantee(
                        DeliveryGuarantee.EXACTLY_ONCE
                )

                .setTransactionalIdPrefix(
                        config.dlqTransactionalIdPrefix()
                )

                .build();
    }

    /**
     * Giữ partition của raw Kafka record.
     *
     * raw partition 0 -> bronze partition 0
     * raw partition 1 -> bronze partition 1
     * raw partition 2 -> bronze partition 2
     */
    private static Integer sourcePartition(
            BronzeEvent event
    ) {
        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        Objects.requireNonNull(
                event.source(),
                "BronzeEvent.source must not be null"
        );

        return event.source().partition();
    }
}