package com.network.preprocess.gold;

import com.network.preprocess.config.GoldJobConfig;
import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceSample;
import com.network.preprocess.model.GoldSequenceWindow;
import com.network.preprocess.model.InvalidGoldFeatureRecord;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.sink.GoldKafkaSinks;
import com.network.preprocess.source.SilverEventKafkaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream
        .SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment
        .StreamExecutionEnvironment;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Entry point của Gold Flink Job.
 *
 * <p>Pipeline hoàn chỉnh:</p>
 *
 * <pre>
 * silver.ue.event
 *      ↓
 * timestamp + watermark
 *      ↓
 * SilverEvent → GoldSequenceEvent
 *      ↓
 * keyBy ueKey
 *      ↓
 * sliding sequence: length 32, stride 8
 *      ↓
 * encode x_cat và x_num
 *      ↓
 * GoldSequenceSample
 *      ↓
 * gold.ue.sequence
 * </pre>
 *
 * <p>Hai side output:</p>
 *
 * <ul>
 *     <li>gold-too-late-event.</li>
 *     <li>invalid-gold-feature.</li>
 * </ul>
 */
public final class GoldJob {

    private GoldJob() {
    }

    /**
     * Entry point khi submit Gold Job lên Flink.
     */
    public static void main(
            String[] args
    ) throws Exception {

        /*
         * BƯỚC 1: Load cấu hình.
         */
        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        /*
         * BƯỚC 2: Lấy Flink execution environment.
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        /*
         * BƯỚC 3: Xây dựng job graph.
         */
        buildPipeline(
                env,
                config
        );

        /*
         * BƯỚC 4: Submit job.
         *
         * Trước execute(), chưa có dữ liệu Kafka nào được đọc.
         */
        env.execute(
                config.jobName()
        );
    }

    /**
     * Lắp ráp toàn bộ Gold pipeline.
     *
     * <p>Method được tách khỏi main để topology test có thể
     * xây job graph mà không kết nối Kafka.</p>
     */
    public static void buildPipeline(
            StreamExecutionEnvironment env,
            GoldJobConfig config
    ) {
        Objects.requireNonNull(
                env,
                "env must not be null"
        );

        Objects.requireNonNull(
                config,
                "config must not be null"
        );

        configureEnvironment(
                env,
                config
        );

        /*
         * =========================================================
         * BƯỚC 1: ĐỌC SILVER EVENT VÀ GẮN WATERMARK
         * =========================================================
         *
         * Kafka không lưu watermark của Silver Job.
         * Vì vậy Gold phải đọc event_time và tạo watermark mới.
         */
        DataStream<SilverEvent> silverEvents =
                env.fromSource(
                                SilverEventKafkaSource.create(
                                        config
                                ),
                                createWatermarkStrategy(
                                        config
                                ),
                                "gold-silver-event-source"
                        )
                        .uid(
                                "gold-silver-event-source-v1"
                        );

        /*
         * =========================================================
         * BƯỚC 2: CHUYỂN SILVER EVENT THÀNH GOLD SEQUENCE EVENT
         * =========================================================
         *
         * Mapper giữ:
         * - identity;
         * - category dạng chuỗi;
         * - numeric source;
         * - event time;
         * - evidence;
         * - sourceOrderKey.
         *
         * Mapper chưa encode vocabulary ID.
         */
        SingleOutputStreamOperator<GoldSequenceEvent>
                goldSequenceEvents =
                silverEvents
                        .map(
                                new GoldSequenceEventMapper()
                        )
                        .name(
                                "gold-map-sequence-event"
                        )
                        .uid(
                                "gold-map-sequence-event-v1"
                        );

        /*
         * =========================================================
         * BƯỚC 3: GOM EVENT THEO UE VÀ TẠO SLIDING WINDOW
         * =========================================================
         *
         * keyBy là bắt buộc vì GoldSequenceProcessFunction
         * giữ state riêng cho từng ueKey.
         */
        SingleOutputStreamOperator<GoldSequenceWindow>
                sequenceWindows =
                goldSequenceEvents
                        .keyBy(
                                GoldSequenceEvent::ueKey
                        )
                        .process(
                                new GoldSequenceProcessFunction(
                                        config.sequenceLength(),
                                        config.sequenceStride(),
                                        config.stateTtlMs(),
                                        config.outputSchemaVersion(),
                                        config.featureVersion()
                                )
                        )
                        .name(
                                "gold-build-sequence-window"
                        )
                        .uid(
                                "gold-build-sequence-window-v1"
                        );

        /*
         * Event có eventTime nhỏ hơn hoặc bằng watermark hiện tại
         * không được chèn ngược vào window đã phát.
         */
        DataStream<GoldSequenceEvent> tooLateEvents =
                sequenceWindows.getSideOutput(
                        GoldSequenceProcessFunction
                                .TOO_LATE_EVENT_TAG
                );

        /*
         * =========================================================
         * BƯỚC 4: ENCODE WINDOW THÀNH SAMPLE MODEL-READY
         * =========================================================
         *
         * Main output:
         *     GoldSequenceSample
         *
         * Side output:
         *     InvalidGoldFeatureRecord
         */
        SingleOutputStreamOperator<GoldSequenceSample>
                modelReadySamples =
                sequenceWindows
                        .process(
                                new GoldFeatureProcessFunction(
                                        config.invalidFeatureSchemaVersion(),
                                        config.featureContract()
                                )
                        )
                        .name(
                                "gold-encode-model-feature"
                        )
                        .uid(
                                "gold-encode-model-feature-v1"
                        );

        DataStream<InvalidGoldFeatureRecord>
                invalidFeatureRecords =
                modelReadySamples.getSideOutput(
                        GoldFeatureProcessFunction
                                .INVALID_FEATURE_TAG
                );

        /*
         * =========================================================
         * BƯỚC 5: GHI MAIN OUTPUT
         * =========================================================
         */
        modelReadySamples
                .sinkTo(
                        GoldKafkaSinks.sequenceSink(
                                config
                        )
                )
                .name(
                        "gold-main-kafka-sink"
                )
                .uid(
                        "gold-main-kafka-sink-v1"
                );

        /*
         * =========================================================
         * BƯỚC 6: GHI TOO-LATE SIDE OUTPUT
         * =========================================================
         */
        tooLateEvents
                .sinkTo(
                        GoldKafkaSinks.tooLateEventSink(
                                config
                        )
                )
                .name(
                        "gold-too-late-kafka-sink"
                )
                .uid(
                        "gold-too-late-kafka-sink-v1"
                );

        /*
         * =========================================================
         * BƯỚC 7: GHI INVALID-FEATURE SIDE OUTPUT
         * =========================================================
         */
        invalidFeatureRecords
                .sinkTo(
                        GoldKafkaSinks.invalidFeatureSink(
                                config
                        )
                )
                .name(
                        "gold-invalid-feature-kafka-sink"
                )
                .uid(
                        "gold-invalid-feature-kafka-sink-v1"
                );
    }

    /**
     * Tạo watermark strategy cho SilverEvent.
     *
     * <p>Ví dụ với max out-of-orderness 30 giây:</p>
     *
     * <pre>
     * Event time lớn nhất đã thấy: 10:01:00
     * Watermark xấp xỉ:           10:00:30
     * </pre>
     *
     * <p>Event có thời gian nhỏ hơn hoặc bằng watermark có thể
     * bị xem là quá trễ.</p>
     */
    private static WatermarkStrategy<SilverEvent>
    createWatermarkStrategy(
            GoldJobConfig config
    ) {
        return WatermarkStrategy
                .<SilverEvent>forBoundedOutOfOrderness(
                        Duration.ofMillis(
                                config
                                        .watermarkMaxOutOfOrdernessMs()
                        )
                )

                /*
                 * eventTime trong SilverEvent là ISO-8601 UTC String.
                 *
                 * Ví dụ:
                 * 2026-07-08T10:00:00Z
                 */
                .withTimestampAssigner(
                        (event, previousTimestamp) ->
                                Instant.parse(
                                        event.eventTime()
                                ).toEpochMilli()
                )

                /*
                 * Một partition không có dữ liệu không được giữ
                 * watermark của toàn bộ source.
                 */
                .withIdleness(
                        Duration.ofMillis(
                                config.watermarkIdlenessMs()
                        )
                );
    }

    /**
     * Cấu hình runtime và checkpoint cho Gold Job.
     */
    private static void configureEnvironment(
            StreamExecutionEnvironment env,
            GoldJobConfig config
    ) {
        /*
         * Hiện Kafka topic có ba partition nên parallelism bằng 3.
         */
        env.setParallelism(
                config.parallelism()
        );

        /*
         * Kafka sink EXACTLY_ONCE chỉ commit transaction
         * khi Flink checkpoint hoàn thành.
         */
        env.enableCheckpointing(
                config.checkpointIntervalMs(),
                CheckpointingMode.EXACTLY_ONCE
        );

        /*
         * Hủy checkpoint nếu thời gian chạy vượt quá timeout.
         */
        env.getCheckpointConfig()
                .setCheckpointTimeout(
                        config.checkpointTimeoutMs()
                );

        /*
         * Giới hạn số checkpoint chạy đồng thời.
         */
        env.getCheckpointConfig()
                .setMaxConcurrentCheckpoints(
                        config.maxConcurrentCheckpoints()
                );

        /*
         * Tạo khoảng nghỉ giữa hai checkpoint liên tiếp.
         */
        env.getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(
                        config.minPauseBetweenCheckpointsMs()
                );
    }
}