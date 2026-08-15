package com.network.preprocess.source;

import com.network.preprocess.config.SilverJobConfig;
import com.network.preprocess.model.BronzeEvent;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator
        .initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Locale;
import java.util.Objects;

/**
 * Factory tạo Kafka source đầu vào cho Silver Job.
 *
 * <p>Source đọc JSON từ bronze.ue.event và chuyển trực tiếp
 * thành BronzeEvent.</p>
 */
public final class BronzeEventKafkaSource {

    private BronzeEventKafkaSource() {
    }

    /**
     * Tạo KafkaSource đã cấu hình đầy đủ.
     */
    public static KafkaSource<BronzeEvent> create(
            SilverJobConfig config
    ) {
        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        return KafkaSource
                .<BronzeEvent>builder()

                /*
                 * Kafka broker bên trong Docker network.
                 */
                .setBootstrapServers(
                        config.bootstrapServers()
                )

                /*
                 * Silver đọc output topic của Bronze.
                 */
                .setTopics(
                        config.inputTopic()
                )

                /*
                 * Consumer group riêng của Silver.
                 */
                .setGroupId(
                        config.consumerGroupId()
                )

                /*
                 * Nếu group đã có committed offset thì tiếp tục từ đó.
                 * Nếu chưa có thì dùng earliest/latest theo YAML.
                 */
                .setStartingOffsets(
                        startingOffsets(
                                config.startingOffsets()
                        )
                )

                /*
                 * Chỉ đọc transaction Kafka đã commit.
                 *
                 * Bronze sink sử dụng EXACTLY_ONCE nên Silver không được
                 * nhìn thấy record của transaction chưa hoàn thành.
                 */
                .setProperty(
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                        "read_committed"
                )

                .setProperty(
                        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                        "180000"
                )

                .setProperty(
                        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                        "60000"
                )

                /*
                 * Prefix giúp nhận diện consumer trên Kafka.
                 */
                .setProperty(
                        "client.id.prefix",
                        "flink-silver-source-v1"
                )

                /*
                 * Chỉ deserialize Kafka value.
                 * Kafka key của Bronze không cần dùng cho Silver dedup
                 * vì BronzeEvent đã giữ source topic/partition/offset gốc.
                 */
                .setValueOnlyDeserializer(
                        new JsonKafkaValueDeserializationSchema<>(
                                BronzeEvent.class
                        )
                )

                .build();
    }

    /**
     * Tạo OffsetInitializer theo cấu hình.
     *
     * <p>Khác với OffsetsInitializer.earliest(), cách này ưu tiên
     * committed offset của consumer group. earliest/latest chỉ đóng vai
     * trò fallback khi group chưa có committed offset.</p>
     */
    private static OffsetsInitializer startingOffsets(
            String configuredValue
    ) {
        Objects.requireNonNull(
                configuredValue,
                "configuredValue must not be null"
        );

        String value =
                configuredValue
                        .trim()
                        .toLowerCase(Locale.ROOT);

        return switch (value) {
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