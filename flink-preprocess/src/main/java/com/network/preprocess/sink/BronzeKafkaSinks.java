package com.network.preprocess.sink;

import com.network.preprocess.config.BronzeJobConfig;
import com.network.preprocess.model.BronzeDlqRecord;
import com.network.preprocess.model.BronzeEvent;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Factory tạo 2 sink của Bronze
 * - output sink 
 * - DLQ sink 
 * 2 sink phải có prefix khác nhau để Flink tạo 2 transaction khác nhau, tránh ghi nhầm DLQ vào output topic.
 * Flink cũng yêu cầu prefix duy nhất giữa các application cùng chạy trên kafka cluster.
 */
public final class BronzeKafkaSinks {

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
                .setRecordSerializer(
                        new JsonKafkaRecordSerializationSchema<>(
                                config.outputTopic(),
                                BronzeEvent::rawRecordId
                        )
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
                .setDeliveryGuarantee(
                        DeliveryGuarantee.EXACTLY_ONCE
                )
                .setTransactionalIdPrefix(
                        config.dlqTransactionalIdPrefix()
                )
                .build();
    }
}