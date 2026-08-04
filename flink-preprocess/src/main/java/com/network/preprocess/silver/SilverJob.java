package com.network.preprocess.silver;

import com.network.preprocess.config.SilverJobConfig;
import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.InvalidIdentityRecord;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.UnsupportedEventRecord;
import com.network.preprocess.silver.event.MapBackedEventCatalog;
import com.network.preprocess.silver.event.SilverEventTransformer;
import com.network.preprocess.silver.identity
        .MapBackedUeIdentityMappingLookup;
import com.network.preprocess.silver.identity.UeIdentityResolver;
import com.network.preprocess.sink.SilverKafkaSinks;
import com.network.preprocess.source.BronzeEventKafkaSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream
        .SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment
        .StreamExecutionEnvironment;

import java.util.Objects;

/**
 * Entry point của Silver Flink Job.
 *
 * <p>SilverJob chịu trách nhiệm lắp ráp các component đã xây
 * ở Checkpoint 7, 8 và 9 thành một streaming pipeline hoàn chỉnh.</p>
 *
 * <pre>
 * bronze.ue.event
 *      ↓
 * resolve identity
 *      ↓
 * normalize event
 *      ↓
 * deduplicate
 *      ↓
 * timestamp + watermark
 *      ↓
 * late-event routing
 *      ↓
 * silver.ue.event
 * </pre>
 */
public final class SilverJob {

    private SilverJob() {
    }

    /**
     * Entry point dùng khi submit Silver Job lên Flink.
     */
    public static void main(String[] args)
            throws Exception {

        /*
         * BƯỚC 1: Load toàn bộ cấu hình Silver.
         */
        SilverJobConfig config =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        /*
         * BƯỚC 2: Tạo Flink execution environment.
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        /*
         * BƯỚC 3: Gắn toàn bộ source, operator và sink vào job graph.
         */
        buildPipeline(
                env,
                config
        );

        /*
         * BƯỚC 4: Submit job cho Flink runtime.
         *
         * Trước execute(), code mới chỉ xây dựng job graph.
         * Sau execute(), source mới bắt đầu đọc Kafka.
         */
        env.execute(
                config.jobName()
        );
    }

    /**
     * Xây dựng Silver pipeline.
     *
     * <p>Tách method này khỏi main để unit test có thể kiểm tra
     * job topology mà không thực sự kết nối Kafka.</p>
     */
    public static void buildPipeline(
            StreamExecutionEnvironment env,
            SilverJobConfig config
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
         * ============================================================
         * BƯỚC 1: TẠO REFERENCE DATA PROVIDER
         * ============================================================
         */

        MapBackedUeIdentityMappingLookup identityLookup =
                new MapBackedUeIdentityMappingLookup(
                        config.msisdnToImsi(),
                        config.mtmsiToImsi()
                );

        UeIdentityResolver identityResolver =
                new UeIdentityResolver(
                        identityLookup
                );

        MapBackedEventCatalog eventCatalog =
                new MapBackedEventCatalog(
                        config.eventDefinitionsByAlias()
                );

        SilverEventTransformer eventTransformer =
                new SilverEventTransformer(
                        eventCatalog
                );

        /*
         * ============================================================
         * BƯỚC 2: ĐỌC BRONZE EVENT TỪ KAFKA
         * ============================================================
         *
         * Bronze chưa cần watermark tại source Silver.
         * Watermark chỉ được gắn sau khi:
         * - identity hợp lệ;
         * - event được hỗ trợ;
         * - SilverEvent được tạo hoàn chỉnh.
         */
        DataStream<BronzeEvent> bronzeEvents =
                env.fromSource(
                                BronzeEventKafkaSource.create(
                                        config
                                ),
                                WatermarkStrategy.noWatermarks(),
                                "silver-bronze-event-source"
                        )
                        .uid(
                                "silver-bronze-event-source-v1"
                        );

        /*
         * ============================================================
         * BƯỚC 3: RESOLVE IDENTITY
         * ============================================================
         *
         * Main output:
         *     IdentityResolvedEvent
         *
         * Side output:
         *     InvalidIdentityRecord
         */
        SingleOutputStreamOperator<IdentityResolvedEvent>
                resolvedEvents =
                bronzeEvents
                        .process(
                                new SilverIdentityProcessFunction(
                                        identityResolver
                                )
                        )
                        .name(
                                "silver-resolve-identity"
                        )
                        .uid(
                                "silver-resolve-identity-v1"
                        );

        DataStream<InvalidIdentityRecord>
                invalidIdentityEvents =
                resolvedEvents.getSideOutput(
                        SilverIdentityProcessFunction
                                .INVALID_IDENTITY_TAG
                );

        /*
         * ============================================================
         * BƯỚC 4: CHUẨN HÓA EVENT
         * ============================================================
         *
         * Main output:
         *     SilverEvent
         *
         * Side output:
         *     UnsupportedEventRecord
         */
        SingleOutputStreamOperator<SilverEvent>
                normalizedEvents =
                resolvedEvents
                        .process(
                                new SilverEventProcessFunction(
                                        eventTransformer
                                )
                        )
                        .name(
                                "silver-normalize-event"
                        )
                        .uid(
                                "silver-normalize-event-v1"
                        );

        DataStream<UnsupportedEventRecord>
                unsupportedEvents =
                normalizedEvents.getSideOutput(
                        SilverEventProcessFunction
                                .UNSUPPORTED_EVENT_TAG
                );

        /*
         * ============================================================
         * BƯỚC 5: DEDUP + WATERMARK + LATE ROUTING
         * ============================================================
         *
         * SilverStreamBuilder được tạo tại Checkpoint 9.
         */
        SilverStreamBuilder.Result streamResult =
                SilverStreamBuilder.build(
                        normalizedEvents,
                        config.stateTtlMs(),
                        config.watermarkMaxOutOfOrdernessMs(),
                        config.watermarkIdlenessMs()
                );

        /*
         * ============================================================
         * BƯỚC 6: GHI MAIN OUTPUT
         * ============================================================
         */
        streamResult
                .onTimeEvents()
                .sinkTo(
                        SilverKafkaSinks.eventSink(
                                config
                        )
                )
                .name(
                        "silver-main-kafka-sink"
                )
                .uid(
                        "silver-main-kafka-sink-v1"
                );

        /*
         * ============================================================
         * BƯỚC 7: GHI INVALID IDENTITY SIDE OUTPUT
         * ============================================================
         */
        invalidIdentityEvents
                .sinkTo(
                        SilverKafkaSinks
                                .invalidIdentitySink(
                                        config
                                )
                )
                .name(
                        "silver-invalid-identity-kafka-sink"
                )
                .uid(
                        "silver-invalid-identity-kafka-sink-v1"
                );

        /*
         * ============================================================
         * BƯỚC 8: GHI UNSUPPORTED EVENT SIDE OUTPUT
         * ============================================================
         */
        unsupportedEvents
                .sinkTo(
                        SilverKafkaSinks
                                .unsupportedEventSink(
                                        config
                                )
                )
                .name(
                        "silver-unsupported-event-kafka-sink"
                )
                .uid(
                        "silver-unsupported-event-kafka-sink-v1"
                );

        /*
         * ============================================================
         * BƯỚC 9: GHI LATE EVENT SIDE OUTPUT
         * ============================================================
         */
        streamResult
                .lateEvents()
                .sinkTo(
                        SilverKafkaSinks.lateEventSink(
                                config
                        )
                )
                .name(
                        "silver-late-event-kafka-sink"
                )
                .uid(
                        "silver-late-event-kafka-sink-v1"
                );
    }

    /**
     * Cấu hình runtime và checkpoint cho Silver Job.
     */
    private static void configureEnvironment(
            StreamExecutionEnvironment env,
            SilverJobConfig config
    ) {
        /*
         * Với ba Kafka partition, parallelism 3 cho phép
         * ba source subtask đọc đồng thời.
         */
        env.setParallelism(
                config.parallelism()
        );

        /*
         * EXACTLY_ONCE Kafka sink cần checkpointing.
         */
        env.enableCheckpointing(
                config.checkpointIntervalMs(),
                CheckpointingMode.EXACTLY_ONCE
        );

        /*
         * Hủy checkpoint nếu vượt quá timeout.
         */
        env.getCheckpointConfig()
                .setCheckpointTimeout(
                        config.checkpointTimeoutMs()
                );

        /*
         * Không chạy quá nhiều checkpoint đồng thời.
         */
        env.getCheckpointConfig()
                .setMaxConcurrentCheckpoints(
                        config.maxConcurrentCheckpoints()
                );

        /*
         * Tạo khoảng nghỉ tối thiểu giữa hai checkpoint.
         */
        env.getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(
                        config.minPauseBetweenCheckpointsMs()
                );
    }
}