package com.network.preprocess.gold;

import com.network.preprocess.config.GoldJobConfig;
import org.apache.flink.streaming.api.environment
        .StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GoldJobTopologyTest {

    @Test
    void shouldBuildCompleteGoldTopology() {
        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        /*
         * Chỉ xây dựng job graph.
         *
         * Không gọi env.execute(), do đó test:
         * - không kết nối Kafka;
         * - không đọc dữ liệu;
         * - không mở transaction thật.
         */
        GoldJob.buildPipeline(
                env,
                config
        );

        String executionPlan =
                env.getExecutionPlan();

        /*
         * Source Silver phải tồn tại.
         */
        assertTrue(
                executionPlan.contains(
                        "gold-silver-event-source"
                )
        );

        /*
         * Mapper SilverEvent → GoldSequenceEvent phải tồn tại.
         */
        assertTrue(
                executionPlan.contains(
                        "gold-map-sequence-event"
                )
        );

        /*
         * Operator tạo sliding window phải tồn tại.
         */
        assertTrue(
                executionPlan.contains(
                        "gold-build-sequence-window"
                )
        );

        /*
         * Operator encode feature phải tồn tại.
         */
        assertTrue(
                executionPlan.contains(
                        "gold-encode-model-feature"
                )
        );

        /*
         * Main sink phải tồn tại.
         */
        assertTrue(
                executionPlan.contains(
                        "gold-main-kafka-sink"
                )
        );

        /*
         * Sink chứa event quá trễ phải tồn tại.
         */
        assertTrue(
                executionPlan.contains(
                        "gold-too-late-kafka-sink"
                )
        );

        /*
         * Sink chứa window vi phạm feature contract phải tồn tại.
         */
        assertTrue(
                executionPlan.contains(
                        "gold-invalid-feature-kafka-sink"
                )
        );
    }
}