package com.network.producer.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * Cấu hình của log-producer.
 *
 * <p>
 * Class này chịu trách nhiệm đọc application.properties
 * và tách cấu hình thành hai nhóm:
 * </p>
 *
 * <ul>
 *     <li>
 *         Cấu hình riêng của application:
 *         app.topic
 *     </li>
 *     <li>
 *         Cấu hình chuẩn của Kafka Producer:
 *         bootstrap.servers, serializer, acks, retries...
 *     </li>
 * </ul>
 *
 * <p>
 * Luồng:
 * </p>
 *
 * <pre>
 * application.properties
 *          ↓
 * ProducerConfiguration
 *          ↓
 * KafkaProducer
 * </pre>
 */
public final class ProducerConfiguration {

    /**
     * Property riêng của application chứa Kafka topic đích.
     */
    private static final String TOPIC_PROPERTY =
            "app.topic";

    /**
     * Resource mặc định được đóng gói trong classpath.
     */
    private static final String DEFAULT_CONFIG_RESOURCE =
            "application.properties";


    /**
     * Kafka topic mà producer sẽ ghi dữ liệu vào.
     */
    private final String topic;

    /**
     * Toàn bộ Kafka client properties.
     *
     * <p>
     * Không chứa app.topic vì app.topic không phải
     * property chuẩn của Kafka client.
     * </p>
     */
    private final Properties kafkaProperties;


    /**
     * Constructor private.
     *
     * <p>
     * Object chỉ được tạo thông qua:
     * </p>
     *
     * <ul>
     *     <li>load(Path)</li>
     *     <li>loadDefault()</li>
     * </ul>
     */
    private ProducerConfiguration(
            String topic,
            Properties kafkaProperties
    ) {

        this.topic =
                Objects.requireNonNull(
                        topic,
                        "topic must not be null"
                );

        this.kafkaProperties =
                new Properties();

        this.kafkaProperties.putAll(
                Objects.requireNonNull(
                        kafkaProperties,
                        "kafkaProperties must not be null"
                )
        );
    }


    /**
     * Load configuration từ một file properties bên ngoài.
     *
     * <p>
     * Ví dụ:
     * </p>
     *
     * <pre>
     * ProducerConfiguration.load(
     *     Path.of("/tmp/application.properties")
     * );
     * </pre>
     *
     * @param configPath đường dẫn file configuration
     * @return configuration đã parse và validate
     * @throws IOException nếu không đọc được file
     */
    public static ProducerConfiguration load(
            Path configPath
    ) throws IOException {

        Objects.requireNonNull(
                configPath,
                "configPath must not be null"
        );


        /*
         * Config path phải trỏ tới một file thực sự.
         */
        if (!Files.isRegularFile(
                configPath
        )) {

            throw new IllegalArgumentException(
                    "Configuration file does not exist: "
                            + configPath
            );
        }


        /*
         * Đọc file theo UTF-8.
         *
         * Sau đó giao toàn bộ việc parse/validate
         * cho load(Reader) dùng chung.
         */
        try (
                Reader reader =
                        Files.newBufferedReader(
                                configPath,
                                StandardCharsets.UTF_8
                        )
        ) {

            return load(
                    reader
            );
        }
    }


    /**
     * Load application.properties mặc định từ classpath.
     *
     * <p>
     * Resource này được lấy từ:
     * </p>
     *
     * <pre>
     * log-producer/src/main/resources/application.properties
     * </pre>
     *
     * <p>
     * Khi Maven build module, file này được copy vào:
     * </p>
     *
     * <pre>
     * target/classes/application.properties
     * </pre>
     */
    public static ProducerConfiguration loadDefault()
            throws IOException {

        InputStream inputStream =
                ProducerConfiguration.class
                        .getClassLoader()
                        .getResourceAsStream(
                                DEFAULT_CONFIG_RESOURCE
                        );


        /*
         * Không tìm thấy resource mặc định.
         */
        if (inputStream == null) {

            throw new IOException(
                    "Classpath configuration not found: "
                            + DEFAULT_CONFIG_RESOURCE
            );
        }


        /*
         * InputStream phải được đóng sau khi đọc xong.
         */
        try (
                Reader reader =
                        new InputStreamReader(
                                inputStream,
                                StandardCharsets.UTF_8
                        )
        ) {

            return load(
                    reader
            );
        }
    }


    /**
     * Parse configuration từ Reader.
     *
     * <p>
     * Method này được dùng chung bởi:
     * </p>
     *
     * <pre>
     * load(Path)
     * loadDefault()
     * </pre>
     *
     * <p>
     * Nhờ vậy hai cách load configuration luôn có
     * cùng validation behavior.
     * </p>
     */
    private static ProducerConfiguration load(
            Reader reader
    ) throws IOException {

        Objects.requireNonNull(
                reader,
                "reader must not be null"
        );


        /*
         * =========================================================
         * BƯỚC 1
         * ĐỌC TOÀN BỘ PROPERTIES
         * =========================================================
         */

        Properties allProperties =
                new Properties();

        allProperties.load(
                reader
        );


        /*
         * =========================================================
         * BƯỚC 2
         * LẤY APPLICATION TOPIC
         * =========================================================
         */

        String topic =
                requireNonBlank(
                        allProperties.getProperty(
                                TOPIC_PROPERTY
                        ),
                        TOPIC_PROPERTY
                );


        /*
         * =========================================================
         * BƯỚC 3
         * TÁCH KAFKA PROPERTIES
         * =========================================================
         */

        Properties kafkaProperties =
                new Properties();

        kafkaProperties.putAll(
                allProperties
        );


        /*
         * app.topic là property của application,
         * không được truyền cho KafkaProducer.
         */
        kafkaProperties.remove(
                TOPIC_PROPERTY
        );


        /*
         * =========================================================
         * BƯỚC 4
         * VALIDATE KAFKA CONFIGURATION BẮT BUỘC
         * =========================================================
         */

        requireNonBlank(
                kafkaProperties.getProperty(
                        "bootstrap.servers"
                ),
                "bootstrap.servers"
        );

        requireNonBlank(
                kafkaProperties.getProperty(
                        "key.serializer"
                ),
                "key.serializer"
        );

        requireNonBlank(
                kafkaProperties.getProperty(
                        "value.serializer"
                ),
                "value.serializer"
        );


        /*
         * =========================================================
         * BƯỚC 5
         * BUILD TYPED CONFIGURATION
         * =========================================================
         */

        return new ProducerConfiguration(
                topic,
                kafkaProperties
        );
    }


    /**
     * Tên Kafka topic đích.
     */
    public String topic() {

        return topic;
    }


    /**
     * Trả về bản copy của Kafka properties.
     *
     * <p>
     * Không trả trực tiếp object nội bộ để code bên ngoài
     * không thể vô tình sửa configuration sau khi load.
     * </p>
     */
    public Properties kafkaProperties() {

        Properties copy =
                new Properties();

        copy.putAll(
                kafkaProperties
        );

        return copy;
    }


    /**
     * Validate String bắt buộc.
     */
    private static String requireNonBlank(
            String value,
            String propertyName
    ) {

        if (value == null
                || value.isBlank()) {

            throw new IllegalArgumentException(
                    "Missing required property: "
                            + propertyName
            );
        }

        return value.trim();
    }
}