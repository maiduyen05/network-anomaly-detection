package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Cấu hình runtime và nghiệp vụ dành cho Bronze Job.
 *
 * <p>
 * BronzeJob không tự hard-code:
 * </p>
 *
 * <ul>
 *     <li>job name;</li>
 *     <li>parallelism;</li>
 *     <li>checkpoint interval;</li>
 *     <li>checkpoint timeout;</li>
 *     <li>max concurrent checkpoints;</li>
 *     <li>minimum pause giữa các checkpoint.</li>
 * </ul>
 *
 * <p>
 * Các giá trị runtime chung được lấy từ:
 * </p>
 *
 * <pre>
 * job:
 *   parallelism: ...
 *   checkpoint-interval-ms: ...
 *   checkpoint-timeout-ms: ...
 *   max-concurrent-checkpoints: ...
 *   min-pause-between-checkpoints-ms: ...
 * </pre>
 *
 * <p>
 * Các cấu hình riêng của Bronze vẫn nằm trong:
 * </p>
 *
 * <pre>
 * bronze:
 *   ...
 * </pre>
 */
public record BronzeJobConfig(

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
         * KAFKA TOPICS
         * =========================================================
         */

        String inputTopic,

        String outputTopic,

        String dlqTopic,

        String consumerGroupId,


        /*
         * =========================================================
         * EXACTLY-ONCE TRANSACTION PREFIX
         * =========================================================
         */

        String outputTransactionalIdPrefix,

        String dlqTransactionalIdPrefix,


        /*
         * =========================================================
         * SCHEMA
         * =========================================================
         */

        String envelopeSchemaVersion,

        String outputSchemaVersion,


        /*
         * =========================================================
         * RAW LOG
         * =========================================================
         */

        String delimiter,

        int fieldCount,

        String localTimezone

) implements FlinkRuntimeConfig {

    /**
     * Load Bronze configuration từ application.yaml.
     *
     * @param resourceName tên resource, ví dụ application.yaml
     * @return BronzeJobConfig đã được validate
     */
    public static BronzeJobConfig loadFromClasspath(
            String resourceName
    ) {

        /*
         * Không cho phép resource name null hoặc rỗng.
         */
        if (resourceName == null
                || resourceName.isBlank()) {

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
                new ObjectMapper(
                        new YAMLFactory()
                );


        ClassLoader classLoader =
                BronzeJobConfig.class
                        .getClassLoader();


        try (
                InputStream inputStream =
                        classLoader.getResourceAsStream(
                                normalizedResourceName
                        )
        ) {

            /*
             * Không có application.yaml thì Bronze không thể chạy.
             */
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


            if (root == null
                    || root.isNull()) {

                throw new IllegalStateException(
                        "Configuration resource is empty: "
                                + normalizedResourceName
                );
            }


            /*
             * =====================================================
             * BUILD TYPED CONFIG
             * =====================================================
             */

            return new BronzeJobConfig(

                    /*
                     * ---------------------------------------------
                     * FLINK JOB
                     * ---------------------------------------------
                     */

                    requiredText(
                            root,
                            "bronze.job-name"
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
                     * ---------------------------------------------
                     * KAFKA
                     * ---------------------------------------------
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
                     * ---------------------------------------------
                     * TOPICS + CONSUMER GROUP
                     * ---------------------------------------------
                     */

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


                    /*
                     * ---------------------------------------------
                     * EXACTLY-ONCE
                     * ---------------------------------------------
                     */

                    requiredText(
                            root,
                            "bronze.output-transactional-id-prefix"
                    ),

                    requiredText(
                            root,
                            "bronze.dlq-transactional-id-prefix"
                    ),


                    /*
                     * ---------------------------------------------
                     * SCHEMA
                     * ---------------------------------------------
                     */

                    requiredText(
                            root,
                            "bronze.envelope-schema-version"
                    ),

                    requiredText(
                            root,
                            "bronze.output-schema-version"
                    ),


                    /*
                     * ---------------------------------------------
                     * RAW LOG
                     * ---------------------------------------------
                     */

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
             * Đây là lỗi startup/configuration,
             * không phải lỗi dữ liệu để đưa vào DLQ.
             */
            throw new IllegalStateException(
                    "Cannot read configuration resource: "
                            + normalizedResourceName,
                    exception
            );
        }
    }


    /**
     * Đọc String bắt buộc.
     */
    private static String requiredText(
            JsonNode root,
            String propertyPath
    ) {

        JsonNode valueNode =
                findNode(
                        root,
                        propertyPath
                );


        if (!valueNode.isTextual()) {

            throw new IllegalStateException(
                    "Configuration property must be text: "
                            + propertyPath
            );
        }


        String value =
                valueNode
                        .asText()
                        .trim();


        if (value.isEmpty()) {

            throw new IllegalStateException(
                    "Configuration property must not be blank: "
                            + propertyPath
            );
        }


        return value;
    }


    /**
     * Đọc int > 0.
     *
     * <p>
     * Dùng cho:
     * </p>
     *
     * <ul>
     *     <li>parallelism;</li>
     *     <li>max concurrent checkpoints;</li>
     *     <li>raw field count.</li>
     * </ul>
     */
    private static int requiredPositiveInt(
            JsonNode root,
            String propertyPath
    ) {

        JsonNode valueNode =
                findNode(
                        root,
                        propertyPath
                );


        if (!valueNode.isIntegralNumber()
                || !valueNode.canConvertToInt()) {

            throw new IllegalStateException(
                    "Configuration property must be an integer: "
                            + propertyPath
            );
        }


        int value =
                valueNode.intValue();


        if (value <= 0) {

            throw new IllegalStateException(
                    "Configuration property must be positive: "
                            + propertyPath
            );
        }


        return value;
    }


    /**
     * Đọc long > 0.
     *
     * <p>
     * Dùng cho checkpoint interval và timeout.
     * </p>
     */
    private static long requiredPositiveLong(
            JsonNode root,
            String propertyPath
    ) {

        JsonNode valueNode =
                findNode(
                        root,
                        propertyPath
                );


        if (!valueNode.isIntegralNumber()
                || !valueNode.canConvertToLong()) {

            throw new IllegalStateException(
                    "Configuration property must be a long: "
                            + propertyPath
            );
        }


        long value =
                valueNode.longValue();


        if (value <= 0L) {

            throw new IllegalStateException(
                    "Configuration property must be positive: "
                            + propertyPath
            );
        }


        return value;
    }


    /**
     * Đọc long >= 0.
     *
     * <p>
     * Minimum pause giữa checkpoint được phép bằng 0.
     * </p>
     */
    private static long requiredNonNegativeLong(
            JsonNode root,
            String propertyPath
    ) {

        JsonNode valueNode =
                findNode(
                        root,
                        propertyPath
                );


        if (!valueNode.isIntegralNumber()
                || !valueNode.canConvertToLong()) {

            throw new IllegalStateException(
                    "Configuration property must be a long: "
                            + propertyPath
            );
        }


        long value =
                valueNode.longValue();


        if (value < 0L) {

            throw new IllegalStateException(
                    "Configuration property must not be negative: "
                            + propertyPath
            );
        }


        return value;
    }


    /**
     * Tìm node theo đường dẫn dạng:
     *
     * <pre>
     * bronze.raw-log.field-count
     * </pre>
     */
    private static JsonNode findNode(
            JsonNode root,
            String propertyPath
    ) {

        JsonNode currentNode =
                root;


        for (
                String propertyName
                        : propertyPath.split("\\.")
        ) {

            currentNode =
                    currentNode.get(
                            propertyName
                    );


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