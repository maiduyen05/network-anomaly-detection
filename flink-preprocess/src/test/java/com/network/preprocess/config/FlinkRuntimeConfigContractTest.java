package com.network.preprocess.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Contract test cho runtime configuration chung
 * của Bronze, Silver và Gold.
 *
 * <p>
 * Cả ba JobConfig đều implement
 * FlinkRuntimeConfig và dùng cùng các giá trị job.*.
 * </p>
 */
class FlinkRuntimeConfigContractTest {

    /**
     * Cả ba JobConfig phải implement FlinkRuntimeConfig.
     */
    @Test
    void shouldMakeAllJobConfigsImplementCommonRuntimeContract() {

        BronzeJobConfig bronze =
                BronzeJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        SilverJobConfig silver =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        GoldJobConfig gold =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );


        assertInstanceOf(
                FlinkRuntimeConfig.class,
                bronze
        );

        assertInstanceOf(
                FlinkRuntimeConfig.class,
                silver
        );

        assertInstanceOf(
                FlinkRuntimeConfig.class,
                gold
        );
    }


    /**
     * Các runtime settings trong block job.*
     * phải giống nhau ở cả ba layer.
     */
    @Test
    void shouldExposeSameSharedRuntimeConfigurationForAllJobs() {

        FlinkRuntimeConfig bronze =
                BronzeJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        FlinkRuntimeConfig silver =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        FlinkRuntimeConfig gold =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );


        /*
         * =========================================================
         * PARALLELISM
         * =========================================================
         */

        assertEquals(
                bronze.parallelism(),
                silver.parallelism()
        );

        assertEquals(
                bronze.parallelism(),
                gold.parallelism()
        );


        /*
         * =========================================================
         * CHECKPOINT INTERVAL
         * =========================================================
         */

        assertEquals(
                bronze.checkpointIntervalMs(),
                silver.checkpointIntervalMs()
        );

        assertEquals(
                bronze.checkpointIntervalMs(),
                gold.checkpointIntervalMs()
        );


        /*
         * =========================================================
         * CHECKPOINT TIMEOUT
         * =========================================================
         */

        assertEquals(
                bronze.checkpointTimeoutMs(),
                silver.checkpointTimeoutMs()
        );

        assertEquals(
                bronze.checkpointTimeoutMs(),
                gold.checkpointTimeoutMs()
        );


        /*
         * =========================================================
         * MAX CONCURRENT CHECKPOINTS
         * =========================================================
         */

        assertEquals(
                bronze.maxConcurrentCheckpoints(),
                silver.maxConcurrentCheckpoints()
        );

        assertEquals(
                bronze.maxConcurrentCheckpoints(),
                gold.maxConcurrentCheckpoints()
        );


        /*
         * =========================================================
         * MIN PAUSE
         * =========================================================
         */

        assertEquals(
                bronze.minPauseBetweenCheckpointsMs(),
                silver.minPauseBetweenCheckpointsMs()
        );

        assertEquals(
                bronze.minPauseBetweenCheckpointsMs(),
                gold.minPauseBetweenCheckpointsMs()
        );
    }


    /**
     * Runtime policy được chia sẻ nhưng tên job phải riêng biệt.
     *
     * <p>
     * Flink UI phải nhìn thấy ba job độc lập:
     * </p>
     *
     * <pre>
     * flink-bronze-v1
     * flink-silver-v1
     * flink-gold-v1
     * </pre>
     */
    @Test
    void shouldKeepDifferentJobNamesForEachLayer() {

        FlinkRuntimeConfig bronze =
                BronzeJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        FlinkRuntimeConfig silver =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        FlinkRuntimeConfig gold =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );


        assertNotEquals(
                bronze.jobName(),
                silver.jobName()
        );

        assertNotEquals(
                silver.jobName(),
                gold.jobName()
        );

        assertNotEquals(
                bronze.jobName(),
                gold.jobName()
        );


        assertEquals(
                "flink-bronze-v1",
                bronze.jobName()
        );

        assertEquals(
                "flink-silver-v1",
                silver.jobName()
        );

        assertEquals(
                "flink-gold-v1",
                gold.jobName()
        );
    }
}