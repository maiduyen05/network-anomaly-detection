package com.network.preprocess.silver;

import com.network.preprocess.config.SilverJobConfig;
import org.apache.flink.streaming.api.environment
        .StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SilverJobTopologyTest {

    @Test
    void shouldBuildCompleteSilverTopology() {
        SilverJobConfig config =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        /*
         * Chỉ xây job graph.
         * Test không gọi env.execute() nên không kết nối Kafka.
         */
        SilverJob.buildPipeline(
                env,
                config
        );

        String executionPlan =
                env.getExecutionPlan();

        /*
         * Kiểm tra các operator quan trọng đã xuất hiện
         * trong Silver job graph.
         */
        assertTrue(
                executionPlan.contains(
                        "silver-bronze-event-source"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-resolve-identity"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-normalize-event"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-deduplicate-source-offset"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-late-event-router"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-main-kafka-sink"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-invalid-identity-kafka-sink"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-unsupported-event-kafka-sink"
                )
        );

        assertTrue(
                executionPlan.contains(
                        "silver-late-event-kafka-sink"
                )
        );
    }
}