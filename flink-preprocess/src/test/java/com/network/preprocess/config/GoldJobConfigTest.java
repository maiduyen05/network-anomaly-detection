package com.network.preprocess.config;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit test cho GoldJobConfig.
 *
 * <p>
 * Mục tiêu của test:
 * </p>
 *
 * <ul>
 *     <li>Đảm bảo application.yaml được load đúng.</li>
 *     <li>Đảm bảo Gold runtime config đúng topic/schema.</li>
 *     <li>Đảm bảo feature contract được load từ
 *         feature-contract thay vì hard-code trong GoldJobConfig.</li>
 *     <li>Đảm bảo model hiện tại vẫn sử dụng sequence 32.</li>
 *     <li>Đảm bảo stride hiện tại bằng 8.</li>
 *     <li>Đảm bảo model input có 4 categorical feature
 *         và 2 numeric feature.</li>
 *     <li>Đảm bảo Gold không phát partial window.</li>
 *     <li>Đảm bảo ba Kafka EXACTLY_ONCE sink
 *         sử dụng transactional prefix khác nhau.</li>
 * </ul>
 */
class GoldJobConfigTest {

    /**
     * Kiểm tra toàn bộ Gold configuration chính
     * được load đúng từ application.yaml.
     */
    @Test
    void shouldLoadGoldConfigurationFromApplicationYaml() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        /*
         * =========================================================
         * FLINK JOB
         * =========================================================
         */

        assertEquals(
                "flink-gold-v1",
                config.jobName()
        );

        assertEquals(
                3,
                config.parallelism()
        );

        assertEquals(
                60_000L,
                config.checkpointIntervalMs()
        );

        assertEquals(
                300_000L,
                config.checkpointTimeoutMs()
        );

        assertEquals(
                1,
                config.maxConcurrentCheckpoints()
        );

        assertEquals(
                30_000L,
                config.minPauseBetweenCheckpointsMs()
        );


        /*
         * =========================================================
         * KAFKA CONNECTION
         * =========================================================
         */

        assertEquals(
                "kafka:29092",
                config.bootstrapServers()
        );

        assertEquals(
                "earliest",
                config.startingOffsets()
        );


        /*
         * =========================================================
         * TOPICS
         * =========================================================
         */

        assertEquals(
                "silver.ue.event",
                config.inputTopic()
        );

        assertEquals(
                "gold.ue.sequence",
                config.outputTopic()
        );

        assertEquals(
                "gold-too-late-event",
                config.tooLateEventTopic()
        );

        assertEquals(
                "invalid-gold-feature",
                config.invalidFeatureTopic()
        );

        assertEquals(
                "flink-gold-v1",
                config.consumerGroupId()
        );


        /*
         * =========================================================
         * OUTPUT SCHEMA
         * =========================================================
         */

        assertEquals(
                "gold-sequence-v1",
                config.outputSchemaVersion()
        );

        assertEquals(
                "invalid-gold-feature-v1",
                config.invalidFeatureSchemaVersion()
        );


        /*
         * =========================================================
         * FEATURE CONTRACT
         * =========================================================
         */

        /*
         * Feature contract là bắt buộc.
         */
        assertNotNull(
                config.featureContract()
        );

        /*
         * Convenience accessor của GoldJobConfig.
         *
         * Giá trị thực tế phải lấy từ:
         *
         * feature-contract.feature-version
         */
        assertEquals(
                "gold-ue-sequence-feature-v1",
                config.featureVersion()
        );

        /*
         * Model hiện tại nhận sequence 32 event.
         *
         * Giá trị này phải lấy từ:
         *
         * feature-contract.sequence.length
         */
        assertEquals(
                32,
                config.sequenceLength()
        );

        /*
         * Window hiện tại:
         *
         * 1..32
         * 9..40
         * 17..48
         *
         * nên stride = 8.
         */
        assertEquals(
                8,
                config.sequenceStride()
        );

        /*
         * Contract hiện tại:
         *
         * x_cat[32][4]
         */
        assertEquals(
                4,
                config
                        .featureContract()
                        .categoricalFeatureCount()
        );

        /*
         * Contract hiện tại:
         *
         * x_num[32][2]
         */
        assertEquals(
                2,
                config
                        .featureContract()
                        .numericFeatureCount()
        );

        /*
         * Pipeline hiện tại chỉ emit khi đủ 32 event.
         *
         * Không padding partial window.
         */
        assertFalse(
                config
                        .featureContract()
                        .emitPartialWindows()
        );


        /*
         * =========================================================
         * EVENT TIME + STATE
         * =========================================================
         */

        assertEquals(
                30_000L,
                config.watermarkMaxOutOfOrdernessMs()
        );

        assertEquals(
                60_000L,
                config.watermarkIdlenessMs()
        );

        assertEquals(
                86_400_000L,
                config.stateTtlMs()
        );
    }


    /**
     * Kiểm tra GoldJobConfig thực sự lấy sequence config
     * từ GoldFeatureContract.
     *
     * <p>
     * Test này giúp bảo vệ kiến trúc mới:
     * </p>
     *
     * <pre>
     * application.yaml
     *      ↓
     * feature-contract
     *      ↓
     * GoldFeatureContract
     *      ↓
     * GoldJobConfig
     * </pre>
     */
    @Test
    void shouldExposeSequenceConfigurationFromFeatureContract() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        GoldFeatureContract contract =
                config.featureContract();

        assertEquals(
                contract.featureVersion(),
                config.featureVersion()
        );

        assertEquals(
                contract.sequenceLength(),
                config.sequenceLength()
        );

        assertEquals(
                contract.sequenceStride(),
                config.sequenceStride()
        );
    }


    /**
     * Kiểm tra shape đầu vào model hiện tại.
     *
     * <p>
     * Gold phải tạo:
     * </p>
     *
     * <pre>
     * x_cat[32][4]
     * x_num[32][2]
     * </pre>
     */
    @Test
    void shouldLoadCurrentModelInputShape() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        GoldFeatureContract contract =
                config.featureContract();

        assertEquals(
                32,
                contract.sequenceLength()
        );

        assertEquals(
                4,
                contract.categoricalFeatureCount()
        );

        assertEquals(
                2,
                contract.numericFeatureCount()
        );

        assertEquals(
                "INT64",
                contract.categoricalDtype()
        );

        assertEquals(
                "FLOAT32",
                contract.numericDtype()
        );
    }


    /**
     * Kiểm tra numeric contract cơ bản.
     */
    @Test
    void shouldLoadNumericFeatureContract() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        GoldFeatureContract contract =
                config.featureContract();

        /*
         * Missing numeric value hiện tại.
         */
        assertEquals(
                -1.0F,
                contract.numericMissingValue()
        );

        /*
         * Output numeric hợp lệ sau normalize
         * phải nằm trong khoảng [0, 1].
         */
        assertEquals(
                0.0F,
                contract.normalizedMin()
        );

        assertEquals(
                1.0F,
                contract.normalizedMax()
        );

        /*
         * Hai numeric feature:
         *
         * 0 -> duration_ms
         * 1 -> request_retries
         */
        assertEquals(
                2,
                contract.numericFeatures().size()
        );

        assertEquals(
                "duration_ms",
                contract
                        .numericFeatures()
                        .get(0)
                        .name()
        );

        assertEquals(
                "request_retries",
                contract
                        .numericFeatures()
                        .get(1)
                        .name()
        );
    }


    /**
     * Kiểm tra categorical contract cơ bản.
     */
    @Test
    void shouldLoadCategoricalFeatureContract() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        GoldFeatureContract contract =
                config.featureContract();

        assertEquals(
                4,
                contract.categoricalFeatures().size()
        );

        /*
         * Feature 0:
         * EVENT_ID -> event_code.
         */
        assertEquals(
                "event_code",
                contract
                        .categoricalFeatures()
                        .get(0)
                        .name()
        );

        /*
         * Feature 1:
         * EVENT_RESULT -> event_result_code.
         */
        assertEquals(
                "event_result_code",
                contract
                        .categoricalFeatures()
                        .get(1)
                        .name()
        );

        /*
         * Feature 2:
         * CAUSE_CODE.
         */
        assertEquals(
                "normalized_cause_code",
                contract
                        .categoricalFeatures()
                        .get(2)
                        .name()
        );

        /*
         * Feature 3:
         * SUB_CAUSE_CODE.
         */
        assertEquals(
                "sub_cause_code",
                contract
                        .categoricalFeatures()
                        .get(3)
                        .name()
        );
    }


    /**
     * Empty string của cause code là category hợp lệ.
     *
     * <p>
     * Không được vô tình coi:
     * </p>
     *
     * <pre>
     * ""
     * </pre>
     *
     * là missing value.
     */
    @Test
    void shouldPreserveEmptyCauseCategories() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        GoldFeatureContract contract =
                config.featureContract();

        assertEquals(
                0,
                contract
                        .categoricalFeatures()
                        .get(2)
                        .vocabulary()
                        .get("")
        );

        assertEquals(
                0,
                contract
                        .categoricalFeatures()
                        .get(3)
                        .vocabulary()
                        .get("")
        );
    }


    /**
     * Mỗi Kafka EXACTLY_ONCE sink phải có
     * transactional ID prefix riêng.
     *
     * <p>
     * Không dùng Set.of(...) ở đây.
     * </p>
     *
     * <p>
     * Nếu hai prefix giống nhau thì Set.of(...)
     * sẽ throw IllegalArgumentException trước assert,
     * khiến test failure khó đọc hơn.
     * </p>
     */
    @Test
    void shouldUseDifferentTransactionalPrefixForEveryGoldSink() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

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
         * Nếu ba prefix khác nhau thì HashSet
         * phải có đúng ba phần tử.
         */
        assertEquals(
                3,
                prefixes.size()
        );
    }
}