package com.network.preprocess.gold;

import com.network.preprocess.config.GoldJobConfig;
import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceSample;
import com.network.preprocess.model.GoldSequenceWindow;
import com.network.preprocess.model.InvalidGoldFeatureRecord;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.runtime.FlinkEnvironmentConfigurator;
import com.network.preprocess.sink.GoldKafkaSinks;
import com.network.preprocess.source.SilverEventKafkaSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * <p>
 * Pipeline:
 * </p>
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
 * sliding sequence
 * length = 32
 * stride = 8
 *      ↓
 * encode x_cat / x_num
 *      ↓
 * GoldSequenceSample
 *      ↓
 * gold.ue.sequence
 * </pre>
 *
 * <p>
 * runtime configuration của Gold được cấu hình thông qua
 * FlinkEnvironmentConfigurator dùng chung với Bronze và Silver.
 * </p>
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
         * =========================================================
         * BƯỚC 1
         * LOAD CONFIG
         * =========================================================
         */

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );


        /*
         * =========================================================
         * BƯỚC 2
         * CREATE FLINK ENVIRONMENT
         * =========================================================
         */

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();


        /*
         * =========================================================
         * BƯỚC 3
         * BUILD PIPELINE
         * =========================================================
         */

        buildPipeline(
                env,
                config
        );


        /*
         * =========================================================
         * BƯỚC 4
         * EXECUTE
         * =========================================================
         */

        env.execute(
                config.jobName()
        );
    }


    /**
     * Xây dựng toàn bộ Gold pipeline.
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


        /*
         * =========================================================
         * FLINK RUNTIME CONFIGURATION
         * =========================================================
         *
         * Không cấu hình checkpoint trực tiếp tại GoldJob nữa.
         *
         * Bronze, Silver và Gold đều dùng cùng một configurator.
         */

        FlinkEnvironmentConfigurator.configure(
                env,
                config
        );


        /*
         * =========================================================
         * BƯỚC 1
         * SILVER SOURCE + WATERMARK
         * =========================================================
         *
         * Kafka không lưu watermark của upstream Flink Job.
         *
         * Gold phải tạo watermark mới từ eventTime
         * của SilverEvent.
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
         * BƯỚC 2
         * SILVER EVENT -> GOLD SEQUENCE EVENT
         * =========================================================
         *
         * Mapper giữ các source feature dạng raw:
         *
         * eventId
         * eventResult
         * cause code
         * sub cause code
         * duration
         * request retries
         *
         * Đồng thời giữ evidence/display fields cấu hình ở YAML.
         */

        SingleOutputStreamOperator<GoldSequenceEvent>
                goldSequenceEvents =
                silverEvents
                        .map(
                                new GoldSequenceEventMapper(
                                        config.evidenceFields()
                                )
                        )
                        .name(
                                "gold-map-sequence-event"
                        )
                        .uid(
                                "gold-map-sequence-event-v1"
                        );


        /*
         * =========================================================
         * BƯỚC 3
         * BUILD SLIDING SEQUENCE
         * =========================================================
         *
         * State được partition theo ueKey.
         *
         * Model v1:
         *
         * length = 32
         * stride = 8
         *
         * Các giá trị này đi từ GoldFeatureContract,
         * không hard-code ở đây.
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
         * =========================================================
         * TOO-LATE EVENT SIDE OUTPUT
         * =========================================================
         *
         * Event đến sau watermark không được chèn ngược
         * vào sequence/window đã emit.
         */

        DataStream<GoldSequenceEvent> tooLateEvents =
                sequenceWindows.getSideOutput(
                        GoldSequenceProcessFunction
                                .TOO_LATE_EVENT_TAG
                );


        /*
         * =========================================================
         * BƯỚC 4
         * FEATURE ENCODING
         * =========================================================
         *
         * Input:
         *
         * GoldSequenceWindow
         *
         * Output:
         *
         * GoldSequenceSample
         *
         * model_input:
         *
         * x_cat[32][4]
         * x_num[32][2]
         *
         * Encoder nhận GoldFeatureContract từ configuration.
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


        /*
         * Window không encode được theo feature contract
         * được đưa sang side output riêng.
         */

        DataStream<InvalidGoldFeatureRecord>
                invalidFeatureRecords =
                modelReadySamples.getSideOutput(
                        GoldFeatureProcessFunction
                                .INVALID_FEATURE_TAG
                );


        /*
         * =========================================================
         * BƯỚC 5
         * MAIN GOLD OUTPUT
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
         * BƯỚC 6
         * TOO-LATE OUTPUT
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
         * BƯỚC 7
         * INVALID FEATURE OUTPUT
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
     * <p>
     * Ví dụ:
     * </p>
     *
     * <pre>
     * max event time = 10:01:00
     * out-of-order   = 30 seconds
     *
     * watermark ≈ 10:00:30
     * </pre>
     *
     * <p>
     * Đây là event-time configuration riêng của Gold,
     * không thuộc FlinkRuntimeConfig chung.
     * </p>
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
                 * Silver eventTime là ISO-8601 UTC.
                 */
                .withTimestampAssigner(
                        (event, previousTimestamp) ->
                                Instant.parse(
                                        event.eventTime()
                                ).toEpochMilli()
                )

                /*
                 * Partition im lặng không được giữ watermark
                 * của toàn bộ source.
                 */
                .withIdleness(
                        Duration.ofMillis(
                                config.watermarkIdlenessMs()
                        )
                );
    }
}