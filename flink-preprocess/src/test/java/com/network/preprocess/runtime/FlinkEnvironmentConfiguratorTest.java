package com.network.preprocess.runtime;

import com.network.preprocess.config.FlinkRuntimeConfig;

import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test cho FlinkEnvironmentConfigurator.
 *
 * <p>
 * Test này không kết nối Kafka và không execute Flink Job.
 * Nó chỉ tạo StreamExecutionEnvironment local,
 * gọi configurator, sau đó kiểm tra configuration
 * đã được apply đúng hay chưa.
 * </p>
 */
class FlinkEnvironmentConfiguratorTest {

    /**
     * Configurator phải áp dụng toàn bộ runtime setting
     * từ FlinkRuntimeConfig vào StreamExecutionEnvironment.
     */
    @Test
    void shouldApplyRuntimeConfigurationToEnvironment() {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();


        /*
         * Không dùng application.yaml ở test này.
         *
         * Ta cố tình dùng các giá trị khác production
         * để chứng minh configurator thực sự đọc config
         * được truyền vào, chứ không hard-code 3 / 60000 / ...
         */
        FlinkRuntimeConfig config =
                testConfig();


        FlinkEnvironmentConfigurator.configure(
                env,
                config
        );


        /*
         * =========================================================
         * PARALLELISM
         * =========================================================
         */

        assertEquals(
                4,
                env.getConfig()
                        .getParallelism()
        );


        CheckpointConfig checkpointConfig =
                env.getCheckpointConfig();


        /*
         * =========================================================
         * CHECKPOINT ENABLED
         * =========================================================
         */

        assertTrue(
                checkpointConfig
                        .isCheckpointingEnabled()
        );


        /*
         * =========================================================
         * CHECKPOINT MODE
         * =========================================================
         */

        assertEquals(
                CheckpointingMode.EXACTLY_ONCE,
                checkpointConfig
                        .getCheckpointingConsistencyMode()
        );


        /*
         * =========================================================
         * CHECKPOINT INTERVAL
         * =========================================================
         */

        assertEquals(
                12_345L,
                checkpointConfig
                        .getCheckpointInterval()
        );


        /*
         * =========================================================
         * CHECKPOINT TIMEOUT
         * =========================================================
         */

        assertEquals(
                67_890L,
                checkpointConfig
                        .getCheckpointTimeout()
        );


        /*
         * =========================================================
         * MAX CONCURRENT CHECKPOINTS
         * =========================================================
         */

        assertEquals(
                2,
                checkpointConfig
                        .getMaxConcurrentCheckpoints()
        );


        /*
         * =========================================================
         * MIN PAUSE
         * =========================================================
         */

        assertEquals(
                3_000L,
                checkpointConfig
                        .getMinPauseBetweenCheckpoints()
        );
    }


    /**
     * Không được gọi configurator khi environment = null.
     */
    @Test
    void shouldRejectNullEnvironment() {

        assertThrows(
                NullPointerException.class,
                () ->
                        FlinkEnvironmentConfigurator.configure(
                                null,
                                testConfig()
                        )
        );
    }


    /**
     * Không được gọi configurator khi config = null.
     */
    @Test
    void shouldRejectNullRuntimeConfig() {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();


        assertThrows(
                NullPointerException.class,
                () ->
                        FlinkEnvironmentConfigurator.configure(
                                env,
                                null
                        )
        );
    }


    /**
     * Tạo runtime config riêng cho unit test.
     *
     * <p>
     * Giá trị cố tình khác application.yaml để test
     * không vô tình pass vì production constants.
     * </p>
     */
    private static FlinkRuntimeConfig testConfig() {

        return new FlinkRuntimeConfig() {

            @Override
            public String jobName() {
                return "runtime-config-test";
            }

            @Override
            public int parallelism() {
                return 4;
            }

            @Override
            public long checkpointIntervalMs() {
                return 12_345L;
            }

            @Override
            public long checkpointTimeoutMs() {
                return 67_890L;
            }

            @Override
            public int maxConcurrentCheckpoints() {
                return 2;
            }

            @Override
            public long minPauseBetweenCheckpointsMs() {
                return 3_000L;
            }
        };
    }
}