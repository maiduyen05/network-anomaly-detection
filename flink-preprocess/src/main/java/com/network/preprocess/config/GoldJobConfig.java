package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Cấu hình runtime và nghiệp vụ dành riêng cho Gold Job.
 *
 * <p>GoldJob không đọc trực tiếp JsonNode và không hard-code:</p>
 *
 * <ul>
 *     <li>Tên Kafka topic.</li>
 *     <li>Consumer group.</li>
 *     <li>Transactional ID prefix.</li>
 *     <li>Watermark.</li>
 *     <li>State TTL.</li>
 *     <li>Sequence length và stride.</li>
 *     <li>Schema version và feature version.</li>
 * </ul>
 */
public record GoldJobConfig(
        /*
         * =========================================================
         * FLINK JOB
         * =========================================================
         */
        String jobName,
        int parallelism,
        long checkpointIntervalMs,
        long checkpointTimeoutMs,
        int maxConcurrentCheckpoints,
        long minPauseBetweenCheckpointsMs,

        /*
         * =========================================================
         * KAFKA CONNECTION
         * =========================================================
         */
        String bootstrapServers,
        String startingOffsets,

        /*
         * =========================================================
         * KAFKA TOPICS VÀ CONSUMER GROUP
         * =========================================================
         */
        String inputTopic,
        String outputTopic,
        String tooLateEventTopic,
        String invalidFeatureTopic,
        String consumerGroupId,

        /*
         * =========================================================
         * EXACTLY-ONCE TRANSACTION PREFIX
         * =========================================================
         */
        String outputTransactionalIdPrefix,
        String tooLateEventTransactionalIdPrefix,
        String invalidFeatureTransactionalIdPrefix,

        /*
         * =========================================================
         * SCHEMA VÀ FEATURE CONTRACT
         * =========================================================
         */
        String outputSchemaVersion,
        String invalidFeatureSchemaVersion,
        String featureVersion,

        /*
         * =========================================================
         * SEQUENCE, WATERMARK VÀ STATE
         * =========================================================
         */
        int sequenceLength,
        int sequenceStride,
        long watermarkMaxOutOfOrdernessMs,
        long watermarkIdlenessMs,
        long stateTtlMs
) {

    /**
     * Đọc cấu hình Gold từ file nằm trong classpath.
     *
     * @param resourceName tên resource, ví dụ application.yaml
     * @return cấu hình Gold đã được kiểm tra
     */
    public static GoldJobConfig loadFromClasspath(
            String resourceName
    ) {
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
                new ObjectMapper(
                        new YAMLFactory()
                );

        ClassLoader classLoader =
                GoldJobConfig.class.getClassLoader();

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
                    yamlMapper.readTree(
                            inputStream
                    );

            if (root == null || root.isNull()) {
                throw new IllegalStateException(
                        "Configuration resource is empty: "
                                + normalizedResourceName
                );
            }

            /*
             * Đọc các giá trị được dùng nhiều lần trước.
             */
            String featureVersion =
                    requiredText(
                            root,
                            "gold.feature-version"
                    );

            int sequenceLength =
                    requiredPositiveInt(
                            root,
                            "gold.sequence-length"
                    );

            int sequenceStride =
                    requiredPositiveInt(
                            root,
                            "gold.sequence-stride"
                    );

            if (sequenceStride > sequenceLength) {
                throw new IllegalStateException(
                        "gold.sequence-stride must not be greater "
                                + "than gold.sequence-length"
                );
            }

            GoldJobConfig config =
                    new GoldJobConfig(
                            /*
                             * Flink Job.
                             */
                            requiredText(
                                    root,
                                    "gold.job-name"
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

                            /*
                             * Kafka connection.
                             */
                            requiredText(
                                    root,
                                    "kafka.bootstrap-servers"
                            ),
                            requiredText(
                                    root,
                                    "kafka.starting-offsets"
                            ),

                            /*
                             * Kafka topics và consumer group.
                             */
                            requiredText(
                                    root,
                                    "gold.input-topic"
                            ),
                            requiredText(
                                    root,
                                    "gold.output-topic"
                            ),
                            requiredText(
                                    root,
                                    "gold.too-late-event-topic"
                            ),
                            requiredText(
                                    root,
                                    "gold.invalid-feature-topic"
                            ),
                            requiredText(
                                    root,
                                    "gold.consumer-group-id"
                            ),

                            /*
                             * Transactional ID prefix.
                             */
                            requiredText(
                                    root,
                                    "gold.output-transactional-id-prefix"
                            ),
                            requiredText(
                                    root,
                                    "gold.too-late-event-transactional-id-prefix"
                            ),
                            requiredText(
                                    root,
                                    "gold.invalid-feature-transactional-id-prefix"
                            ),

                            /*
                             * Schema và feature version.
                             */
                            requiredText(
                                    root,
                                    "gold.output-schema-version"
                            ),
                            requiredText(
                                    root,
                                    "gold.invalid-feature-schema-version"
                            ),
                            featureVersion,

                            /*
                             * Sequence, watermark và state.
                             */
                            sequenceLength,
                            sequenceStride,
                            requiredNonNegativeLong(
                                    root,
                                    "gold.watermark-max-out-of-orderness-ms"
                            ),
                            requiredPositiveLong(
                                    root,
                                    "gold.watermark-idleness-ms"
                            ),
                            requiredPositiveLong(
                                    root,
                                    "gold.state-ttl-ms"
                            )
                    );

            /*
             * Không cho phần gold và feature-contract bị lệch nhau.
             */
            validateFeatureContract(
                    root,
                    config
            );

            /*
             * EXACTLY_ONCE yêu cầu mỗi sink dùng prefix riêng.
             */
            validateTransactionalPrefixes(
                    config
            );

            return config;

        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Could not read configuration resource: "
                            + normalizedResourceName,
                    exception
            );
        }
    }

    /**
     * Kiểm tra cấu hình tạo sequence và feature contract đồng nhất.
     */
    private static void validateFeatureContract(
            JsonNode root,
            GoldJobConfig config
    ) {
        String contractFeatureVersion =
                requiredText(
                        root,
                        "feature-contract.feature-version"
                );

        int contractSequenceLength =
                requiredPositiveInt(
                        root,
                        "feature-contract.sequence.length"
                );

        int contractSequenceStride =
                requiredPositiveInt(
                        root,
                        "feature-contract.sequence.stride"
                );

        if (!config.featureVersion().equals(
                contractFeatureVersion
        )) {
            throw new IllegalStateException(
                    "gold.feature-version must match "
                            + "feature-contract.feature-version"
            );
        }

        if (config.sequenceLength()
                != contractSequenceLength) {

            throw new IllegalStateException(
                    "gold.sequence-length must match "
                            + "feature-contract.sequence.length"
            );
        }

        if (config.sequenceStride()
                != contractSequenceStride) {

            throw new IllegalStateException(
                    "gold.sequence-stride must match "
                            + "feature-contract.sequence.stride"
            );
        }
    }

    /**
     * Kiểm tra ba Kafka sink không dùng trùng transactional prefix.
     */
    private static void validateTransactionalPrefixes(
            GoldJobConfig config
    ) {
        Set<String> prefixes =
                new HashSet<>();

        prefixes.add(
                config.outputTransactionalIdPrefix()
        );

        prefixes.add(
                config.tooLateEventTransactionalIdPrefix()
        );

        prefixes.add(
                config.invalidFeatureTransactionalIdPrefix()
        );

        if (prefixes.size() != 3) {
            throw new IllegalStateException(
                    "Every Gold Kafka sink must use "
                            + "a different transactional ID prefix"
            );
        }
    }

    /**
     * Lấy một node bắt buộc từ đường dẫn dạng:
     *
     * <pre>
     * gold.output-topic
     * feature-contract.sequence.length
     * </pre>
     */
    private static JsonNode requiredNode(
            JsonNode root,
            String path
    ) {
        String jsonPointer =
                "/"
                        + path.replace(
                                ".",
                                "/"
                        );

        JsonNode node =
                root.at(
                        jsonPointer
                );

        if (node.isMissingNode() || node.isNull()) {
            throw new IllegalStateException(
                    "Missing required configuration: "
                            + path
            );
        }

        return node;
    }

    /**
     * Đọc một chuỗi bắt buộc và loại bỏ khoảng trắng hai đầu.
     */
    private static String requiredText(
            JsonNode root,
            String path
    ) {
        JsonNode node =
                requiredNode(
                        root,
                        path
                );

        if (!node.isTextual()
                || node.asText().isBlank()) {

            throw new IllegalStateException(
                    "Configuration must be non-blank text: "
                            + path
            );
        }

        return node
                .asText()
                .trim();
    }

    /**
     * Đọc số nguyên dương lớn hơn 0.
     */
    private static int requiredPositiveInt(
            JsonNode root,
            String path
    ) {
        JsonNode node =
                requiredNode(
                        root,
                        path
                );

        if (!node.isIntegralNumber()
                || !node.canConvertToInt()
                || node.intValue() <= 0) {

            throw new IllegalStateException(
                    "Configuration must be a positive integer: "
                            + path
            );
        }

        return node.intValue();
    }

    /**
     * Đọc số long dương lớn hơn 0.
     */
    private static long requiredPositiveLong(
            JsonNode root,
            String path
    ) {
        JsonNode node =
                requiredNode(
                        root,
                        path
                );

        if (!node.isIntegralNumber()
                || !node.canConvertToLong()
                || node.longValue() <= 0L) {

            throw new IllegalStateException(
                    "Configuration must be a positive long: "
                            + path
            );
        }

        return node.longValue();
    }

    /**
     * Đọc số long lớn hơn hoặc bằng 0.
     *
     * <p>Watermark out-of-orderness có thể bằng 0 nếu muốn
     * giả định event đến đúng thứ tự.</p>
     */
    private static long requiredNonNegativeLong(
            JsonNode root,
            String path
    ) {
        JsonNode node =
                requiredNode(
                        root,
                        path
                );

        if (!node.isIntegralNumber()
                || !node.canConvertToLong()
                || node.longValue() < 0L) {

            throw new IllegalStateException(
                    "Configuration must be a non-negative long: "
                            + path
            );
        }

        return node.longValue();
    }
}