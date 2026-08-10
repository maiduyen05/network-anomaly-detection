package com.network.preprocess.runtime;

import com.network.preprocess.config.FlinkRuntimeConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * Cấu hình Flink execution environment theo một contract chung.
 *
 * <p>
 * Bronze, Silver và Gold phải đi qua class này
 * thay vì mỗi job tự cấu hình checkpoint riêng.
 * </p>
 *
 * <p>
 * Điều này ngăn tình trạng:
 * </p>
 *
 * <pre>
 * application.yaml:
 *   checkpoint-timeout-ms: 300000
 *
 * Silver -> dùng 300000
 * Gold   -> dùng 300000
 * Bronze -> bỏ qua
 * </pre>
 */
public final class FlinkEnvironmentConfigurator {

    private FlinkEnvironmentConfigurator() {
    }

    /**
     * Áp dụng runtime configuration chung cho một Flink job.
     *
     * @param env Flink execution environment
     * @param config runtime config của Bronze/Silver/Gold
     */
    public static void configure(
            StreamExecutionEnvironment env,
            FlinkRuntimeConfig config
    ) {

        Objects.requireNonNull(
                env,
                "env must not be null"
        );

        Objects.requireNonNull(
                config,
                "config must not be null"
        );


        /*
         * =========================================================
         * PARALLELISM
         * =========================================================
         */

        env.setParallelism(
                config.parallelism()
        );


        /*
         * =========================================================
         * CHECKPOINTING
         * =========================================================
         *
         * Kafka EXACTLY_ONCE sink cần Flink checkpoint
         * để commit transaction.
         */

        env.enableCheckpointing(
                config.checkpointIntervalMs(),
                CheckpointingMode.EXACTLY_ONCE
        );


        /*
         * Nếu checkpoint chạy quá lâu,
         * Flink hủy checkpoint đó.
         */

        env.getCheckpointConfig()
                .setCheckpointTimeout(
                        config.checkpointTimeoutMs()
                );


        /*
         * Giới hạn số checkpoint chạy cùng lúc.
         */

        env.getCheckpointConfig()
                .setMaxConcurrentCheckpoints(
                        config.maxConcurrentCheckpoints()
                );


        /*
         * Tránh tạo checkpoint liên tục
         * ngay sau khi checkpoint trước hoàn thành.
         */

        env.getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(
                        config.minPauseBetweenCheckpointsMs()
                );
    }
}