package com.network.preprocess.source;

import com.network.preprocess.config.GoldJobConfig;
import com.network.preprocess.model.SilverEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator
        .initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Locale;
import java.util.Objects;

/**
 * Factory tạo Kafka source đầu vào cho Gold Job.
 *
 * <p>Luồng dữ liệu:</p>
 *
 * <pre>
 * silver.ue.event
 *      ↓
 * JSON snake_case
 *      ↓
 * SilverEvent
 * </pre>
 */
public final class SilverEventKafkaSource {

    private SilverEventKafkaSource() {
    }

    /**
     * Tạo KafkaSource đọc SilverEvent.
     */
    public static KafkaSource<SilverEvent> create(
            GoldJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return KafkaSource
                .<SilverEvent>builder()

                /*
                 * Kafka broker mà Gold Job kết nối.
                 */
                .setBootstrapServers(
                        config.bootstrapServers()
                )

                /*
                 * Topic đầu vào của Gold chính là output Silver.
                 */
                .setTopics(
                        config.inputTopic()
                )

                /*
                 * Consumer group riêng của Gold.
                 *
                 * Không dùng consumer group của Silver vì mỗi tầng
                 * phải quản lý offset độc lập.
                 */
                .setGroupId(
                        config.consumerGroupId()
                )

                /*
                 * Nếu group đã có committed offset:
                 *     tiếp tục từ committed offset.
                 *
                 * Nếu group chưa có offset:
                 *     dùng earliest hoặc latest trong YAML.
                 */
                .setStartingOffsets(
                        startingOffsets(
                                config.startingOffsets()
                        )
                )

                /*
                 * Silver Kafka sink sử dụng EXACTLY_ONCE.
                 *
                 * read_committed bảo đảm Gold không đọc các record
                 * thuộc Kafka transaction chưa commit.
                 */
                .setProperty(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        "read_committed"
                )

                /*
                 * Dùng để nhận diện Gold consumer trên Kafka.
                 */
                .setProperty(
                        "client.id.prefix",
                        "flink-gold-source-v1"
                )

                /*
                 * Kafka value được deserialize thành SilverEvent.
                 *
                 * JSON sử dụng snake_case:
                 * event_time -> eventTime
                 * raw_record_id -> rawRecordId
                 */
                .setValueOnlyDeserializer(
                        new JsonKafkaValueDeserializationSchema<>(
                                SilverEvent.class
                        )
                )

                .build();
    }

    /**
     * Chuyển starting-offsets trong YAML thành OffsetsInitializer.
     */
    private static OffsetsInitializer startingOffsets(
            String configuredValue
    ) {
        Objects.requireNonNull(
                configuredValue,
                "configuredValue must not be null"
        );

        String normalizedValue =
                configuredValue
                        .trim()
                        .toLowerCase(Locale.ROOT);

        return switch (normalizedValue) {
            case "earliest" ->
                    OffsetsInitializer.committedOffsets(
                            OffsetResetStrategy.EARLIEST
                    );

            case "latest" ->
                    OffsetsInitializer.committedOffsets(
                            OffsetResetStrategy.LATEST
                    );

            default ->
                    throw new IllegalArgumentException(
                            "Unsupported starting-offsets: "
                                    + configuredValue
                    );
        };
    }
}