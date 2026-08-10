package com.network.preprocess.config;

/**
 * Contract chung cho runtime configuration
 * của tất cả Flink preprocessing jobs.
 *
 * <p>
 * Bronze, Silver và Gold đều phải expose
 * cùng các cấu hình runtime cơ bản:
 * </p>
 *
 * <ul>
 *     <li>job name;</li>
 *     <li>parallelism;</li>
 *     <li>checkpoint interval;</li>
 *     <li>checkpoint timeout;</li>
 *     <li>max concurrent checkpoints;</li>
 *     <li>minimum pause between checkpoints.</li>
 * </ul>
 *
 * <p>
 * Interface này KHÔNG chứa:
 * </p>
 *
 * <ul>
 *     <li>topic;</li>
 *     <li>schema;</li>
 *     <li>watermark;</li>
 *     <li>state TTL;</li>
 *     <li>feature contract.</li>
 * </ul>
 *
 * <p>
 * Những cấu hình đó vẫn thuộc từng layer riêng.
 * </p>
 */
public interface FlinkRuntimeConfig {

    /**
     * Tên job hiển thị trên Flink Web UI.
     */
    String jobName();

    /**
     * Default parallelism của job.
     */
    int parallelism();

    /**
     * Chu kỳ checkpoint.
     */
    long checkpointIntervalMs();

    /**
     * Timeout của một checkpoint.
     */
    long checkpointTimeoutMs();

    /**
     * Số checkpoint tối đa được chạy đồng thời.
     */
    int maxConcurrentCheckpoints();

    /**
     * Khoảng nghỉ tối thiểu giữa hai checkpoint.
     */
    long minPauseBetweenCheckpointsMs();
}