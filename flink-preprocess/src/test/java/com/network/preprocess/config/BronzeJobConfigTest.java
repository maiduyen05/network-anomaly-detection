package com.network.preprocess.config;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test configuration của Bronze Job.
 *
 * <p>
 * Mục tiêu:
 * </p>
 *
 * <ul>
 *     <li>Đảm bảo Bronze đọc runtime config chung từ job.*.</li>
 *     <li>Đảm bảo Bronze đọc đúng Kafka/topic/schema config.</li>
 *     <li>Đảm bảo hai EXACTLY_ONCE sink có transactional prefix khác nhau.</li>
 * </ul>
 */
class BronzeJobConfigTest {

    /**
     * application.yaml phải được load thành BronzeJobConfig
     * với đầy đủ runtime configuration.
     */
    @Test
    void shouldLoadBronzeConfigurationFromApplicationYaml() {

        BronzeJobConfig config =
                BronzeJobConfig.loadFromClasspath(
                        "application.yaml"
                );


        /*
         * =========================================================
         * FLINK JOB
         * =========================================================
         */

        assertEquals(
                "flink-bronze-v1",
                config.jobName()
        );

        assertEquals(
                2,
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
         * KAFKA
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
                "raw.ue.log.line",
                config.inputTopic()
        );

        assertEquals(
                "bronze.ue.event",
                config.outputTopic()
        );

        assertEquals(
                "dlq.ue.log.line",
                config.dlqTopic()
        );

        assertEquals(
                "flink-bronze-v1",
                config.consumerGroupId()
        );


        /*
         * =========================================================
         * SCHEMA
         * =========================================================
         */

        assertEquals(
                "raw-envelope-v1",
                config.envelopeSchemaVersion()
        );

        assertEquals(
                "bronze-v1",
                config.outputSchemaVersion()
        );


        /*
         * =========================================================
         * RAW LOG
         * =========================================================
         */

        assertEquals(
                ";",
                config.delimiter()
        );

        assertEquals(
                52,
                config.fieldCount()
        );

        assertEquals(
                "Asia/Ho_Chi_Minh",
                config.localTimezone()
        );
    }


    /**
     * Main output và DLQ không được dùng chung
     * transactional ID prefix.
     *
     * <p>
     * Hai KafkaSink EXACTLY_ONCE có prefix riêng
     * để tránh transaction của hai sink đụng nhau.
     * </p>
     */
    @Test
    void shouldUseDifferentTransactionalPrefixForBronzeSinks() {

        BronzeJobConfig config =
                BronzeJobConfig.loadFromClasspath(
                        "application.yaml"
                );


        Set<String> prefixes =
                new HashSet<>();


        prefixes.add(
                config.outputTransactionalIdPrefix()
        );

        prefixes.add(
                config.dlqTransactionalIdPrefix()
        );


        assertEquals(
                2,
                prefixes.size()
        );
    }
}