package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.network.preprocess.model.EventDefinition;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Cấu hình dùng riêng cho Silver Job.
 *
 * <p>Class này đọc các nhóm cấu hình:</p>
 *
 * <ul>
 *     <li>job: parallelism và checkpoint.</li>
 *     <li>kafka: broker và starting offset.</li>
 *     <li>silver: topic, watermark, state và reference data.</li>
 * </ul>
 *
 * <p>Sau khi load, SilverJob chỉ sử dụng các accessor có kiểu rõ ràng,
 * không cần tự đọc JsonNode hoặc hard-code giá trị.</p>
 */
public record SilverJobConfig(
        String jobName,

        int parallelism,
        long checkpointIntervalMs,
        long checkpointTimeoutMs,
        int maxConcurrentCheckpoints,
        long minPauseBetweenCheckpointsMs,

        String bootstrapServers,
        String startingOffsets,

        String inputTopic,
        String outputTopic,
        String invalidIdentityTopic,
        String unsupportedEventTopic,
        String lateEventTopic,
        String consumerGroupId,

        String outputTransactionalIdPrefix,
        String invalidIdentityTransactionalIdPrefix,
        String unsupportedEventTransactionalIdPrefix,
        String lateEventTransactionalIdPrefix,

        String outputSchemaVersion,

        long watermarkMaxOutOfOrdernessMs,
        long watermarkIdlenessMs,
        long stateTtlMs,

        Map<String, String> msisdnToImsi,
        Map<String, String> mtmsiToImsi,
        Map<String, EventDefinition> eventDefinitionsByAlias
) implements FlinkRuntimeConfig {

    /**
     * Tạo bản sao mutable của các map cấu hình.
     *
     * <p>Không giữ trực tiếp object Map do Jackson tạo ra.
     * Việc copy cũng tránh code bên ngoài thay đổi reference data
     * sau khi config đã được load.</p>
     */
    public SilverJobConfig {
        Objects.requireNonNull(
                msisdnToImsi,
                "msisdnToImsi must not be null"
        );

        Objects.requireNonNull(
                mtmsiToImsi,
                "mtmsiToImsi must not be null"
        );

        Objects.requireNonNull(
                eventDefinitionsByAlias,
                "eventDefinitionsByAlias must not be null"
        );

        msisdnToImsi =
                new LinkedHashMap<>(msisdnToImsi);

        mtmsiToImsi =
                new LinkedHashMap<>(mtmsiToImsi);

        eventDefinitionsByAlias =
                new LinkedHashMap<>(eventDefinitionsByAlias);
    }

    /**
     * Đọc Silver config từ file nằm trong src/main/resources.
     *
     * @param resourceName tên file, ví dụ application.yaml
     * @return cấu hình Silver đã được kiểm tra
     */
    public static SilverJobConfig loadFromClasspath(
            String resourceName
    ) {
        if (resourceName == null || resourceName.isBlank()) {
            throw new IllegalArgumentException(
                    "resourceName must not be blank"
            );
        }

        /*
         * ClassLoader yêu cầu resource path không bắt đầu bằng "/".
         */
        String normalizedResourceName =
                resourceName.startsWith("/")
                        ? resourceName.substring(1)
                        : resourceName;

        ObjectMapper yamlMapper =
                new ObjectMapper(new YAMLFactory());

        ClassLoader classLoader =
                SilverJobConfig.class.getClassLoader();

        try (InputStream inputStream =
                     classLoader.getResourceAsStream(
                             normalizedResourceName
                     )) {

            if (inputStream == null) {
                throw new IllegalStateException(
                        "Configuration resource not found: "
                                + normalizedResourceName
                );
            }

            JsonNode root =
                    yamlMapper.readTree(inputStream);

            if (root == null || root.isNull()) {
                throw new IllegalStateException(
                        "Configuration resource is empty: "
                                + normalizedResourceName
                );
            }

            return new SilverJobConfig(
                    requiredText(
                            root,
                            "silver.job-name"
                    ),

                    requiredPositiveInt(
                            root,
                            "job.parallelism"
                    ),
                    requiredPositiveLong(
                            root,
                            "job.checkpoint-interval-ms"
                    ),
                    requiredPositiveLong(
                            root,
                            "job.checkpoint-timeout-ms"
                    ),
                    requiredPositiveInt(
                            root,
                            "job.max-concurrent-checkpoints"
                    ),
                    requiredNonNegativeLong(
                            root,
                            "job.min-pause-between-checkpoints-ms"
                    ),

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
                            "silver.input-topic"
                    ),
                    requiredText(
                            root,
                            "silver.output-topic"
                    ),
                    requiredText(
                            root,
                            "silver.invalid-identity-topic"
                    ),
                    requiredText(
                            root,
                            "silver.unsupported-event-topic"
                    ),
                    requiredText(
                            root,
                            "silver.late-event-topic"
                    ),
                    requiredText(
                            root,
                            "silver.consumer-group-id"
                    ),

                    requiredText(
                            root,
                            "silver.output-transactional-id-prefix"
                    ),
                    requiredText(
                            root,
                            "silver.invalid-identity-transactional-id-prefix"
                    ),
                    requiredText(
                            root,
                            "silver.unsupported-event-transactional-id-prefix"
                    ),
                    requiredText(
                            root,
                            "silver.late-event-transactional-id-prefix"
                    ),

                    requiredText(
                            root,
                            "silver.output-schema-version"
                    ),

                    requiredNonNegativeLong(
                            root,
                            "silver.watermark-max-out-of-orderness-ms"
                    ),
                    requiredPositiveLong(
                            root,
                            "silver.watermark-idleness-ms"
                    ),
                    requiredPositiveLong(
                            root,
                            "silver.state-ttl-ms"
                    ),

                    requiredStringMap(
                            root,
                            "silver.identity-mappings.msisdn-to-imsi"
                    ),
                    requiredStringMap(
                            root,
                            "silver.identity-mappings.mtmsi-to-imsi"
                    ),
                    requiredEventCatalog(
                            root,
                            "silver.supported-events"
                    )
            );

        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Cannot read configuration resource: "
                            + normalizedResourceName,
                    exception
            );
        }
    }

    /**
     * Đọc một chuỗi bắt buộc.
     */
    private static String requiredText(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode node =
                findNode(root, propertyPath);

        if (!node.isTextual()) {
            throw new IllegalStateException(
                    "Configuration property must be text: "
                            + propertyPath
            );
        }

        String value =
                node.asText().trim();

        if (value.isEmpty()) {
            throw new IllegalStateException(
                    "Configuration property must not be blank: "
                            + propertyPath
            );
        }

        return value;
    }

    /**
     * Đọc số nguyên dương.
     */
    private static int requiredPositiveInt(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode node =
                findNode(root, propertyPath);

        if (!node.canConvertToInt()) {
            throw new IllegalStateException(
                    "Configuration property must be an integer: "
                            + propertyPath
            );
        }

        int value = node.asInt();

        if (value <= 0) {
            throw new IllegalStateException(
                    "Configuration property must be positive: "
                            + propertyPath
            );
        }

        return value;
    }

    /**
     * Đọc số long dương.
     */
    private static long requiredPositiveLong(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode node =
                findNode(root, propertyPath);

        if (!node.canConvertToLong()) {
            throw new IllegalStateException(
                    "Configuration property must be a long: "
                            + propertyPath
            );
        }

        long value = node.asLong();

        if (value <= 0) {
            throw new IllegalStateException(
                    "Configuration property must be positive: "
                            + propertyPath
            );
        }

        return value;
    }

    /**
     * Đọc số long cho phép bằng 0 nhưng không cho phép âm.
     */
    private static long requiredNonNegativeLong(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode node =
                findNode(root, propertyPath);

        if (!node.canConvertToLong()) {
            throw new IllegalStateException(
                    "Configuration property must be a long: "
                            + propertyPath
            );
        }

        long value = node.asLong();

        if (value < 0) {
            throw new IllegalStateException(
                    "Configuration property must not be negative: "
                            + propertyPath
            );
        }

        return value;
    }

    /**
     * Đọc map dạng:
     *
     * <pre>
     * msisdn-to-imsi:
     *   "84900000001": "452040000000001"
     * </pre>
     *
     * <p>Map rỗng vẫn hợp lệ vì event có direct IMSI không cần mapping.</p>
     */
    private static Map<String, String> requiredStringMap(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode node =
                findNode(root, propertyPath);

        if (!node.isObject()) {
            throw new IllegalStateException(
                    "Configuration property must be an object: "
                            + propertyPath
            );
        }

        Map<String, String> result =
                new LinkedHashMap<>();

        Iterator<Map.Entry<String, JsonNode>> fields =
                node.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field =
                    fields.next();

            String key =
                    field.getKey().trim();

            JsonNode valueNode =
                    field.getValue();

            if (key.isEmpty()
                    || !valueNode.isTextual()
                    || valueNode.asText().isBlank()) {

                throw new IllegalStateException(
                        "Invalid mapping entry in: "
                                + propertyPath
                );
            }

            result.put(
                    key,
                    valueNode.asText().trim()
            );
        }

        return result;
    }

    /**
     * Đọc danh sách event catalog.
     *
     * <p>Mỗi phần tử tạo một mapping:</p>
     *
     * <pre>
     * alias -> EventDefinition
     * </pre>
     */
    private static Map<String, EventDefinition>
    requiredEventCatalog(
            JsonNode root,
            String propertyPath
    ) {
        JsonNode arrayNode =
                findNode(root, propertyPath);

        if (!arrayNode.isArray()) {
            throw new IllegalStateException(
                    "Configuration property must be an array: "
                            + propertyPath
            );
        }

        if (arrayNode.isEmpty()) {
            throw new IllegalStateException(
                    "Event catalog must not be empty: "
                            + propertyPath
            );
        }

        Map<String, EventDefinition> result =
                new LinkedHashMap<>();

        for (JsonNode eventNode : arrayNode) {
            String alias =
                    requiredText(eventNode, "alias");

            String canonicalEventId =
                    requiredText(
                            eventNode,
                            "canonical-event-id"
                    );

            String displayName =
                    requiredText(
                            eventNode,
                            "display-name"
                    );

            EventDefinition definition =
                    new EventDefinition(
                            canonicalEventId,
                            displayName
                    );

            EventDefinition previous =
                    result.putIfAbsent(
                            alias,
                            definition
                    );

            if (previous != null
                    && !previous.equals(definition)) {

                throw new IllegalStateException(
                        "Conflicting event alias: "
                                + alias
                );
            }
        }

        return result;
    }

    /**
     * Tìm node bằng property path dạng:
     *
     * <pre>
     * silver.identity-mappings.msisdn-to-imsi
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