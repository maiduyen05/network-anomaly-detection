package com.network.producer.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Cấu hình của ứng dụng log-producer.
 * Class này đọc application.properties và tách hai nhóm:
 *     - Cấu hình của application (ví dụ: app.topic trong application.properties)
 *     - Cấu hình chuẩn được truyền vào KafkaProducer.
 * Tách để chương trình gọi cho dễ 
 * File này chuẩn bị cấu hình để class khác tạo KafkaProducer 
 * Hiểu đơn giản: 
 * appliction.properties --> ProducerConfiguration (lấy topic, lấy kafka properties) 
 * --> KafkaProducer được tạo --> ProducerRecord được tạo với topic --> Gửi mesage vào kafka
 */

// final: không cho class khác kế thừa class này
public final class ProducerConfiguration {

    /**
     * Tên property chứa topic trong file 
     * định nghĩa lại do app.topic trong propoties không phải property chuẩn 
     */
    private static final String TOPIC_PROPERTY =
            "app.topic";

    /**
     * Tên Kafka topic đích.
     */
    private final String topic;

    /**
     * Cấu hình cho KafkaProducer.
     */
    private final Properties kafkaProperties;

    /**
     * Constructor private.
     * <p>Object được tạo thông qua phương thức load().</p>
     */
    private ProducerConfiguration(
            String topic,
            Properties kafkaProperties
    ) {
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Đọc cấu hình từ một file properties, tạo một object ProducerConfiguration
     *
     * @param configPath đường dẫn application.properties
     * @return cấu hình producer hoàn chỉnh
     * @throws IOException nếu không đọc được file
     */
    public static ProducerConfiguration load(
            Path configPath
    ) throws IOException {

        Objects.requireNonNull(
                configPath,
                "configPath must not be null"
        );

        if (!Files.isRegularFile(configPath)) {
            throw new IllegalArgumentException(
                    "Configuration file does not exist: "
                            + configPath
            );
        }

        /*
         * allProperties ban đầu chứa cả:
         *
         * app.topic
         * bootstrap.servers
         * acks
         * batch.size
         * ...
         */
        Properties allProperties =
                new Properties();

        /*
         * Đọc file bằng UTF-8.
         *
         * try-with-resources bảo đảm Reader được đóng
         * ngay cả khi xảy ra lỗi.
         */
        try (
                Reader reader = Files.newBufferedReader(
                        configPath,
                        StandardCharsets.UTF_8
                )
        ) {
            allProperties.load(reader);
        }

        // Lấy và kiểm tra topic.
        String topic = requireNonBlank(
                allProperties.getProperty(TOPIC_PROPERTY),
                TOPIC_PROPERTY
        );

        /*
         * Copy toàn bộ property sang một Properties mới
         * để không thay đổi object vừa đọc.
         */
        Properties kafkaProperties =
                new Properties();

        kafkaProperties.putAll(allProperties);

        /*
         * app.topic không phải cấu hình Kafka client.
         * Loại nó trước khi tạo KafkaProducer.
         */
        kafkaProperties.remove(TOPIC_PROPERTY);

        // Kafka bắt buộc phải biết ít nhất một broker.
        requireNonBlank(
                kafkaProperties.getProperty(
                        "bootstrap.servers"
                ),
                "bootstrap.servers"
        );

        return new ProducerConfiguration(
                topic,
                kafkaProperties
        );
    }

    /**
     * Trả về topic đích.
     */
    public String topic() {
        return topic;
    }

    /**
     * Trả về bản copy của Kafka Properties.
     *
     * <p>Không trả trực tiếp object nội bộ để code phía ngoài
     * không thể vô tình thay đổi cấu hình đang giữ.</p>
     */
    public Properties kafkaProperties() {

        Properties copy =
                new Properties();

        copy.putAll(kafkaProperties);

        return copy;
    }

    /**
     * Kiểm tra một property bắt buộc có giá trị.
     */
    private static String requireNonBlank(
            String value,
            String propertyName
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required property: "
                            + propertyName
            );
        }

        return value.trim();
    }

    private static final String DEFAULT_CONFIG_RESOURCE =
        "application.properties";

        public static ProducerConfiguration loadDefault()
        throws IOException {

    InputStream inputStream =
            ProducerConfiguration.class
                    .getClassLoader()
                    .getResourceAsStream(
                            DEFAULT_CONFIG_RESOURCE
                    );

    if (inputStream == null) {
        throw new IOException(
                "Classpath configuration not found: "
                        + DEFAULT_CONFIG_RESOURCE
        );
    }

    try (
            Reader reader = new InputStreamReader(
                    inputStream,
                    StandardCharsets.UTF_8
            )
    ) {
        return load(reader);
    }

    private static ProducerConfiguration load(
        Reader reader
) throws IOException {

    Properties allProperties = new Properties();
    allProperties.load(reader);

    String topic = requireNonBlank(
            allProperties.getProperty("app.topic"),
            "app.topic"
    );

    Properties kafkaProperties = new Properties();
    kafkaProperties.putAll(allProperties);
    kafkaProperties.remove("app.topic");

    requireNonBlank(
            kafkaProperties.getProperty("bootstrap.servers"),
            "bootstrap.servers"
    );

    requireNonBlank(
            kafkaProperties.getProperty("key.serializer"),
            "key.serializer"
    );

    requireNonBlank(
            kafkaProperties.getProperty("value.serializer"),
            "value.serializer"
    );

    return new ProducerConfiguration(
            topic,
            kafkaProperties
    );
}
}