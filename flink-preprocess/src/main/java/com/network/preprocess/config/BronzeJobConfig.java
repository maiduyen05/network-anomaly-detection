package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Cấu hình dùng riêng cho Bronze Job.
 *
 * <p>Class này đọc các giá trị từ application.yaml và chuyển chúng
 * thành các field Java có kiểu dữ liệu rõ ràng.</p>
 *
 * <p>Ví dụ:</p>
 *
 * <pre>
 * BronzeJobConfig config =
 *         BronzeJobConfig.loadFromClasspath("application.yaml");
 *
 * String topic = config.inputTopic();
 * int fieldCount = config.fieldCount();
 * </pre>
 *
 * @param bootstrapServers Kafka bootstrap servers
 * @param startingOffsets vị trí bắt đầu đọc Kafka
 * @param inputTopic topic chứa raw envelope
 * @param outputTopic topic chứa Bronze event hợp lệ
 * @param dlqTopic topic chứa record không hợp lệ
 * @param consumerGroupId consumer group của Bronze Job
 * @param envelopeSchemaVersion schema version của raw envelope
 * @param outputSchemaVersion schema version của Bronze event
 * @param delimiter ký tự phân cách raw log
 * @param fieldCount số field bắt buộc của raw log
 * @param localTimezone timezone dùng cho EVENT_TIME không có offset
 */
public record BronzeJobConfig(
        String bootstrapServers,
        String startingOffsets,
        String inputTopic,
        String outputTopic,
        String dlqTopic,
        String consumerGroupId,
        String outputTransactionalIdPrefix,
        String dlqTransactionalIdPrefix,
        String envelopeSchemaVersion,
        String outputSchemaVersion,
        String delimiter,
        int fieldCount,
        String localTimezone
) {

    /**
     * Đọc cấu hình từ file nằm trong src/main/resources.
     *
     * @param resourceName tên resource, ví dụ application.yaml
     * @return cấu hình Bronze đã được kiểm tra
     * @throws IllegalStateException nếu không tìm thấy file hoặc file sai
     */
    public static BronzeJobConfig loadFromClasspath(
            String resourceName
    ) {
        /*
         * resourceName null/rỗng là lỗi lập trình,
         * không phải lỗi dữ liệu Kafka.
         */
        if (resourceName == null || resourceName.isBlank()) {
            throw new IllegalArgumentException(
                    "resourceName must not be blank"
            );
        }

        /*
         * ClassLoader yêu cầu đường dẫn không bắt đầu bằng "/".
         */
        String normalizedResourceName =
                resourceName.startsWith("/")
                        ? resourceName.substring(1)
                        : resourceName;

        ObjectMapper yamlMapper =
                new ObjectMapper(new YAMLFactory());

        ClassLoader classLoader =
                BronzeJobConfig.class.getClassLoader();

        try (InputStream inputStream =
                     classLoader.getResourceAsStream(
                             normalizedResourceName
                     )) {

            /*
             * Không tìm thấy application.yaml thì Job không thể chạy.
             */
            if (inputStream == null) {
                throw new IllegalStateException(
                        "Configuration resource not found: "
                                + normalizedResourceName
                );
            }

            /*
             * Đọc toàn bộ YAML thành cây JSON/YAML.
             */
            JsonNode root =
                    yamlMapper.readTree(inputStream);

            if (root == null || root.isNull()) {
                throw new IllegalStateException(
                        "Configuration resource is empty: "
                                + normalizedResourceName
                );
            }

            /*
             * Chuyển cấu trúc YAML lồng nhau thành cấu hình phẳng,
             * giúp code trong BronzeJob dễ sử dụng.
             */
            return new BronzeJobConfig(
                    requiredText(
                            root,
                            "kafka.bootstrap-servers"
                    ),
                    requiredText(
                            root,
                            "kafka.starting-offsets"
                    ),
                    requiredText(
                            root,
                            "bronze.input-topic"
                    ),
                    requiredText(
                            root,
                            "bronze.output-topic"
                    ),
                    requiredText(
                            root,
                            "bronze.dlq-topic"
                    ),
                    requiredText(
                            root,
                            "bronze.consumer-group-id"
                    ),
                    requiredText(
                            root,
                            "bronze.output-transactional-id-prefix"
                    ),
                    requiredText(
                            root,
                            "bronze.dlq-transactional-id-prefix"
                    ),
                    requiredText(
                            root,
                            "bronze.envelope-schema-version"
                    ),
                    requiredText(
                            root,
                            "bronze.output-schema-version"
                    ),
                    requiredText(
                            root,
                            "bronze.raw-log.delimiter"
                    ),
                    requiredPositiveInt(
                            root,
                            "bronze.raw-log.field-count"
                    ),
                    requiredText(
                            root,
                            "bronze.timestamp.local-timezone"
                    )
            );

        } catch (IOException exception) {
            /*
             * Lỗi đọc YAML là lỗi khởi động Job.
             * Không chuyển lỗi này thành Bronze DLQ.
             */
            throw new IllegalStateException(
                    "Cannot read configuration resource: "
                            + normalizedResourceName,
                    exception
            );
        }
    }

    /**
     * Lấy một giá trị String bắt buộc từ YAML.
     */
    private static String requiredText(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode valueNode =
                findNode(root, propertyPath);

        if (!valueNode.isTextual()) {
            throw new IllegalStateException(
                    "Configuration property must be text: "
                            + propertyPath
            );
        }

        String value =
                valueNode.asText().trim();

        if (value.isEmpty()) {
            throw new IllegalStateException(
                    "Configuration property must not be blank: "
                            + propertyPath
            );
        }

        return value;
    }

    /**
     * Lấy một số nguyên dương bắt buộc từ YAML.
     */
    private static int requiredPositiveInt(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode valueNode =
                findNode(root, propertyPath);

        if (!valueNode.canConvertToInt()) {
            throw new IllegalStateException(
                    "Configuration property must be an integer: "
                            + propertyPath
            );
        }

        int value =
                valueNode.asInt();

        if (value <= 0) {
            throw new IllegalStateException(
                    "Configuration property must be positive: "
                            + propertyPath
            );
        }

        return value;
    }

    /**
     * Tìm node dựa trên đường dẫn dạng:
     *
     * <pre>
     * bronze.raw-log.field-count
     * </pre>
     */
    private static JsonNode findNode(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode currentNode = root;

        for (String propertyName
                : propertyPath.split("\\.")) {

            currentNode =
                    currentNode.get(propertyName);

            if (currentNode == null
                    || currentNode.isNull()) {

                throw new IllegalStateException(
                        "Missing configuration property: "
                                + propertyPath
                );
            }
        }

        return currentNode;
    }
}