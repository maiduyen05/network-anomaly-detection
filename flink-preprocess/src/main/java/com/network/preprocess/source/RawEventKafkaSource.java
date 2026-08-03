package com.network.preprocess.source;

import com.network.preprocess.config.BronzeJobConfig;
import com.network.preprocess.model.KafkaRawRecord;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator
        .initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Locale;

/**
 * Factory dùng để tạo Kafka Source đầu vào cho tầng Bronze.
 *
 * <p>
 * Class này chịu trách nhiệm cấu hình cách Flink kết nối và đọc dữ liệu
 * từ Kafka, bao gồm:
 * </p>
 *
 * <ul>
 *     <li>địa chỉ Kafka broker;</li>
 *     <li>raw input topic cần đọc;</li>
 *     <li>consumer group;</li>
 *     <li>vị trí offset bắt đầu;</li>
 *     <li>isolation level;</li>
 *     <li>deserialization schema dùng để chuyển từng Kafka message
 *         thành {@link KafkaRawRecord}.</li>
 * </ul>
 *
 * <p>
 * Class này không trực tiếp parse JSON hoặc kiểm tra dữ liệu nghiệp vụ.
 * Việc chuyển byte của từng Kafka message thành {@link KafkaRawRecord}
 * được giao cho {@link KafkaRawRecordDeserializationSchema}.
 * </p>
 */

public final class RawEventKafkaSource {

    private RawEventKafkaSource() {
    }

    public static KafkaSource<KafkaRawRecord> create(
            BronzeJobConfig config
    ) {
        return KafkaSource
                .<KafkaRawRecord>builder()
                .setBootstrapServers(
                        config.bootstrapServers()
                )
                .setTopics(
                        config.inputTopic()
                )
                .setGroupId(
                        config.consumerGroupId()
                )
                .setStartingOffsets(
                        startingOffsets(
                                config.startingOffsets()
                        )
                )
                .setProperty(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        "read_committed"
                )
                .setProperty(
                        "client.id.prefix",
                        "flink-bronze-source-v1"
                )
                .setDeserializer(
                        new KafkaRawRecordDeserializationSchema()
                )
                .build();
    }

    private static OffsetsInitializer startingOffsets(
            String configuredValue
    ) {
        String value = configuredValue
                .trim()
                .toLowerCase(Locale.ROOT);

        return switch (value) {
            case "earliest" ->
                    OffsetsInitializer.earliest();

            case "latest" ->
                    OffsetsInitializer.latest();

            default ->
                    throw new IllegalArgumentException(
                            "Unsupported starting-offsets: "
                                    + configuredValue
                    );
        };
    }
}