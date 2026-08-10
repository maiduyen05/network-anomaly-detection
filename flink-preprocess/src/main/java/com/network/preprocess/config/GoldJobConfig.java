package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

/**
 * Cấu hình runtime dành riêng cho Gold Job.
 *
 * <p>
 * GoldJob không trực tiếp đọc YAML hoặc JsonNode.
 * Tất cả cấu hình runtime được load một lần tại đây
 * và expose ra ngoài thông qua các accessor có kiểu rõ ràng.
 * </p>
 *
 * <p>
 * Cấu hình được chia thành hai nhóm:
 * </p>
 *
 * <ul>
 *     <li>
 *         Runtime/job configuration:
 *         Kafka, topic, checkpoint, watermark, state TTL...
 *     </li>
 *     <li>
 *         Model feature contract:
 *         sequence length, stride, categorical features,
 *         numeric features, vocabulary...
 *     </li>
 * </ul>
 *
 * <p>
 * Điểm quan trọng:
 * GoldJobConfig KHÔNG giữ thêm một bản riêng của:
 * </p>
 *
 * <ul>
 *     <li>featureVersion;</li>
 *     <li>sequenceLength;</li>
 *     <li>sequenceStride.</li>
 * </ul>
 *
 * <p>
 * Ba giá trị trên chỉ tồn tại trong {@link GoldFeatureContract}.
 * Điều này tránh việc:
 * </p>
 *
 * <pre>
 * gold.sequence-length
 *          !=
 * feature-contract.sequence.length
 * </pre>
 *
 * <p>
 * Feature contract là source of truth duy nhất cho model.
 * </p>
 */
public record GoldJobConfig(

        /*
         * =========================================================
         * FLINK JOB
         * =========================================================
         */

        /**
         * Tên Gold Job hiển thị trên Flink Web UI.
         */
        String jobName,

        /**
         * Parallelism của Gold Job.
         */
        int parallelism,

        /**
         * Khoảng thời gian giữa hai checkpoint.
         */
        long checkpointIntervalMs,

        /**
         * Timeout tối đa của một checkpoint.
         */
        long checkpointTimeoutMs,

        /**
         * Số checkpoint tối đa được chạy đồng thời.
         */
        int maxConcurrentCheckpoints,

        /**
         * Khoảng nghỉ tối thiểu giữa các checkpoint.
         */
        long minPauseBetweenCheckpointsMs,


        /*
         * =========================================================
         * KAFKA CONNECTION
         * =========================================================
         */

        /**
         * Kafka bootstrap servers.
         */
        String bootstrapServers,

        /**
         * Cách xử lý khi consumer group chưa có committed offset.
         *
         * Ví dụ:
         *
         * earliest
         * latest
         */
        String startingOffsets,


        /*
         * =========================================================
         * KAFKA TOPICS + CONSUMER GROUP
         * =========================================================
         */

        /**
         * Silver output topic mà Gold consume.
         */
        String inputTopic,

        /**
         * Main output chứa GoldSequenceSample model-ready.
         */
        String outputTopic,

        /**
         * Topic chứa event đến quá trễ.
         */
        String tooLateEventTopic,

        /**
         * Topic chứa window vi phạm feature contract.
         */
        String invalidFeatureTopic,

        /**
         * Consumer group riêng của Gold.
         */
        String consumerGroupId,


        /*
         * =========================================================
         * EXACTLY-ONCE TRANSACTION PREFIX
         * =========================================================
         */

        /**
         * Transactional ID prefix của main output sink.
         */
        String outputTransactionalIdPrefix,

        /**
         * Transactional ID prefix của too-late sink.
         */
        String tooLateEventTransactionalIdPrefix,

        /**
         * Transactional ID prefix của invalid-feature sink.
         */
        String invalidFeatureTransactionalIdPrefix,


        /*
         * =========================================================
         * OUTPUT SCHEMA
         * =========================================================
         */

        /**
         * Schema version của GoldSequenceSample.
         */
        String outputSchemaVersion,

        /**
         * Schema version của InvalidGoldFeatureRecord.
         */
        String invalidFeatureSchemaVersion,


        /*
         * =========================================================
         * MODEL FEATURE CONTRACT
         * =========================================================
         */

        GoldFeatureContract featureContract,

        /*
        * =========================================================
        * CONFIGURABLE EVIDENCE
        * =========================================================
        */

        /**
         * Danh sách metadata được phép đưa vào evidence/display
         * của từng Gold event.
         *
         * <p>
         * Danh sách lấy từ:
         * </p>
         *
         * <pre>
         * gold:
         *   evidence:
         *     fields:
         *       - event_id
         *       - event_time
         *       - imsi
         *       ...
         * </pre>
         *
         * Việc thêm/bớt field ở đây không thay đổi x_cat/x_num.
         */
        List<String> evidenceFields,

        long watermarkMaxOutOfOrdernessMs,
        long watermarkIdlenessMs,
        long stateTtlMs

) {

    /*
     * =============================================================
     * RECORD VALIDATION
     * =============================================================
     */

    /**
     * Compact constructor.
     *
     * <p>
     * Các giá trị lấy từ YAML đã được validate bởi các helper
     * trong loadFromClasspath().
     * </p>
     *
     * <p>
     * Riêng featureContract phải luôn tồn tại vì đây là contract
     * bắt buộc để Gold tạo dữ liệu model-ready.
     * </p>
     */
        public GoldJobConfig {

        Objects.requireNonNull(
                featureContract,
                "featureContract must not be null"
        );

        Objects.requireNonNull(
                evidenceFields,
                "evidenceFields must not be null"
        );

        /*
        * Tạo mutable copy.
        *
        * Không giữ trực tiếp List của Jackson/YAML.
        */
        evidenceFields =
                new ArrayList<>(
                        evidenceFields
                );

        if (evidenceFields.isEmpty()) {
                throw new IllegalArgumentException(
                        "evidenceFields must not be empty"
                );
        }
        }


    /*
     * =============================================================
     * COMPATIBILITY ACCESSORS
     * =============================================================
     *
     * Các method này giữ compatibility với GoldJob hiện tại.
     *
     * GoldJob vẫn có thể gọi:
     *
     * config.featureVersion()
     * config.sequenceLength()
     * config.sequenceStride()
     *
     * Nhưng dữ liệu thực tế được lấy từ featureContract.
     *
     * Nhờ vậy ta chưa cần sửa toàn bộ Gold topology trong
     * checkpoint này.
     * =============================================================
     */

    /**
     * Feature version hiện tại của model.
     */
    public String featureVersion() {

        return featureContract
                .featureVersion();
    }

    /**
     * Số event trong một Gold sequence.
     *
     * Model hiện tại yêu cầu 32.
     */
    public int sequenceLength() {

        return featureContract
                .sequenceLength();
    }

    /**
     * Số event dịch chuyển sau khi phát một sequence.
     *
     * Ví dụ:
     *
     * sequenceLength = 32
     * stride = 8
     *
     * Window:
     *
     * 1..32
     * 9..40
     * 17..48
     */
    public int sequenceStride() {

        return featureContract
                .sequenceStride();
    }


    /*
     * =============================================================
     * LOAD CONFIGURATION
     * =============================================================
     */

    /**
     * Đọc cấu hình Gold từ file nằm trong classpath.
     *
     * <p>
     * Ví dụ:
     * </p>
     *
     * <pre>
     * GoldJobConfig config =
     *     GoldJobConfig.loadFromClasspath(
     *         "application.yaml"
     *     );
     * </pre>
     *
     * @param resourceName tên resource, ví dụ application.yaml
     * @return Gold configuration đã được parse và validate
     */
    public static GoldJobConfig loadFromClasspath(
            String resourceName
    ) {

        if (resourceName == null
                || resourceName.isBlank()) {

            throw new IllegalArgumentException(
                    "resourceName must not be blank"
            );
        }

        /*
         * ClassLoader yêu cầu resource path
         * không bắt đầu bằng "/".
         *
         * Cả hai dạng:
         *
         * application.yaml
         *
         * và:
         *
         * /application.yaml
         *
         * đều được hỗ trợ.
         */
        String normalizedResourceName =
                resourceName.startsWith("/")
                        ? resourceName.substring(1)
                        : resourceName;

        /*
         * ObjectMapper chuyên đọc YAML.
         */
        ObjectMapper yamlMapper =
                new ObjectMapper(
                        new YAMLFactory()
                );

        /*
         * Dùng class loader của chính GoldJobConfig
         * để tìm resource trong:
         *
         * src/main/resources
         */
        ClassLoader classLoader =
                GoldJobConfig.class
                        .getClassLoader();

        try (
                InputStream inputStream =
                        classLoader.getResourceAsStream(
                                normalizedResourceName
                        )
        ) {

            /*
             * Không tìm thấy application.yaml.
             */
            if (inputStream == null) {

                throw new IllegalStateException(
                        "Configuration resource not found: "
                                + normalizedResourceName
                );
            }

            /*
             * Parse toàn bộ YAML thành JsonNode tree.
             */
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
             * MODEL FEATURE CONTRACT
             * =====================================================
             *
             * Đây là nơi DUY NHẤT đọc:
             *
             * feature-contract:
             *   feature-version:
             *   sequence:
             *     length:
             *     stride:
             *   categorical:
             *   numeric:
             *
             * Không còn đọc:
             *
             * gold.feature-version
             * gold.sequence-length
             * gold.sequence-stride
             */
            GoldFeatureContract featureContract =
                    GoldFeatureContract.fromRoot(
                            root
                    );


            /*
             * =====================================================
             * BUILD TYPED CONFIG
             * =====================================================
             */

            GoldJobConfig config =
                    new GoldJobConfig(

                            /*
                             * -------------------------------------
                             * Flink Job
                             * -------------------------------------
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
                             * -------------------------------------
                             * Kafka connection
                             * -------------------------------------
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
                             * -------------------------------------
                             * Kafka topics + consumer group
                             * -------------------------------------
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
                             * -------------------------------------
                             * Transactional ID prefix
                             * -------------------------------------
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
                             * -------------------------------------
                             * Output schema
                             * -------------------------------------
                             */

                            requiredText(
                                    root,
                                    "gold.output-schema-version"
                            ),

                            requiredText(
                                    root,
                                    "gold.invalid-feature-schema-version"
                            ),


                            /*
                             * -------------------------------------
                             * Model feature contract
                             * -------------------------------------
                             *
                             * Không truyền featureVersion,
                             * sequenceLength hoặc sequenceStride
                             * riêng nữa.
                             */
                            featureContract,

                            /*
                        * Các field evidence tùy chọn.
                        *
                        * Không ảnh hưởng model input.
                        */
                        requiredTextList(
                                root,
                                "gold.evidence.fields"
                        ),



                            /*
                             * -------------------------------------
                             * Event time + state
                             * -------------------------------------
                             */

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
             * =====================================================
             * EXACTLY-ONCE VALIDATION
             * =====================================================
             *
             * Ba Kafka sink của Gold phải dùng ba prefix khác nhau.
             *
             * Nếu dùng chung transactional ID prefix,
             * producer Kafka có thể fence lẫn nhau.
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


    /*
     * =============================================================
     * TRANSACTIONAL PREFIX VALIDATION
     * =============================================================
     */

    /**
     * Kiểm tra ba Kafka sink EXACTLY_ONCE
     * không sử dụng cùng transactional prefix.
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

        /*
         * HashSet chỉ còn size = 3 nếu cả ba
         * prefix đều khác nhau.
         */
        if (prefixes.size() != 3) {

            throw new IllegalStateException(
                    "Every Gold Kafka sink must use "
                            + "a different transactional ID prefix"
            );
        }
    }


    /*
     * =============================================================
     * YAML HELPER METHODS
     * =============================================================
     */

    /**
     * Lấy một node bắt buộc từ đường dẫn dạng:
     *
     * <pre>
     * gold.output-topic
     * job.parallelism
     * kafka.bootstrap-servers
     * </pre>
     *
     * <p>
     * Đường dẫn dạng dấu "." được chuyển sang
     * JSON Pointer.
     * </p>
     *
     * Ví dụ:
     *
     * <pre>
     * gold.output-topic
     *
     * thành:
     *
     * /gold/output-topic
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

        if (node.isMissingNode()
                || node.isNull()) {

            throw new IllegalStateException(
                    "Missing required configuration: "
                            + path
            );
        }

        return node;
    }


    /**
     * Đọc String bắt buộc.
     *
     * <p>
     * Giá trị:
     *
     * - phải tồn tại;
     * - phải là text;
     * - không được blank.
     * </p>
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
     * Đọc integer bắt buộc và phải lớn hơn 0.
     *
     * Ví dụ:
     *
     * job.parallelism: 3
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
     * Đọc long bắt buộc và phải lớn hơn 0.
     *
     * Ví dụ:
     *
     * checkpoint-interval-ms: 60000
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
     * Đọc long bắt buộc và phải >= 0.
     *
     * <p>
     * Dùng cho các giá trị có thể bằng 0,
     * ví dụ watermark out-of-orderness.
     * </p>
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

        /**
         * Đọc một danh sách String bắt buộc từ YAML.
         *
         * <p>
         * Ví dụ:
         * </p>
         *
         * <pre>
         * gold:
         *   evidence:
         *     fields:
         *       - event_id
         *       - event_time
         *       - imsi
         * </pre>
         */
        private static List<String> requiredTextList(
                JsonNode root,
                String path
        ) {

        JsonNode node =
                requiredNode(
                        root,
                        path
                );

        if (!node.isArray()) {
                throw new IllegalStateException(
                        "Configuration must be an array: "
                                + path
                );
        }

        List<String> result =
                new ArrayList<>();

        for (JsonNode item : node) {

                if (!item.isTextual()
                        || item.asText().isBlank()) {

                throw new IllegalStateException(
                        "Every item in "
                                + path
                                + " must be non-blank text"
                );
                }

                String value =
                        item
                                .asText()
                                .trim();

                /*
                * Không cho khai báo cùng một evidence field hai lần.
                */
                if (result.contains(value)) {
                throw new IllegalStateException(
                        "Duplicated evidence field: "
                                + value
                );
                }

                result.add(
                        value
                );
        }

        if (result.isEmpty()) {
                throw new IllegalStateException(
                        "Configuration list must not be empty: "
                                + path
                );
        }

        return result;
        }
}

