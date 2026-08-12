package com.network.preprocess.silver;

import com.network.preprocess.config.SilverJobConfig;
import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.InvalidIdentityRecord;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.UnsupportedEventRecord;
import com.network.preprocess.runtime.FlinkEnvironmentConfigurator;
import com.network.preprocess.silver.event.MapBackedEventCatalog;
import com.network.preprocess.silver.event.SilverEventTransformer;
import com.network.preprocess.silver.identity
        .MapBackedUeIdentityMappingLookup;
import com.network.preprocess.silver.identity.UeIdentityResolver;
import com.network.preprocess.sink.SilverKafkaSinks;
import com.network.preprocess.source.BronzeEventKafkaSource;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream
        .SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment
        .StreamExecutionEnvironment;

import java.util.Objects;

/**
 * Entry point của Silver Flink Job.
 *
 * <p>
 * Pipeline:
 * </p>
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
 *
 * <p>
 * runtime configuration của Silver không còn được cấu hình
 * trực tiếp trong class này.
 * </p>
 *
 * <p>
 * Parallelism và checkpoint configuration đi theo:
 * </p>
 *
 * <pre>
 * application.yaml
 *        ↓
 * SilverJobConfig
 *        ↓
 * FlinkRuntimeConfig
 *        ↓
 * FlinkEnvironmentConfigurator
 * </pre>
 */
public final class SilverJob {

    private SilverJob() {
    }


    /**
     * Entry point dùng khi submit Silver Job lên Flink.
     */
    public static void main(
            String[] args
    ) throws Exception {

        /*
         * =========================================================
         * BƯỚC 1
         * LOAD CONFIGURATION
         * =========================================================
         */

        SilverJobConfig config =
                SilverJobConfig.loadFromClasspath(
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
     * Xây dựng toàn bộ Silver pipeline.
     *
     * <p>
     * Method được tách khỏi main để topology test
     * có thể dựng execution graph mà không execute job thật.
     * </p>
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


        /*
         * =========================================================
         * FLINK RUNTIME CONFIGURATION
         * =========================================================
         *
         * Trước Checkpoint 3, SilverJob có private method
         * configureEnvironment() riêng.
         *
         * Từ Checkpoint 3, Bronze/Silver/Gold dùng chung
         * một implementation.
         */

        FlinkEnvironmentConfigurator.configure(
                env,
                config
        );

        /*
        * Silver có keyed state cho deduplication và TTL.
        *
        * Dùng RocksDB để keyed state không chiếm toàn bộ Java heap
        * khi Silver chạy đồng thời với Bronze và Gold.
        *
        * true = incremental checkpoint.
        */
        env.setStateBackend(
                new EmbeddedRocksDBStateBackend(true)
        );


        /*
         * =========================================================
         * BƯỚC 1
         * REFERENCE DATA
         * =========================================================
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
         * =========================================================
         * BƯỚC 2
         * BRONZE KAFKA SOURCE
         * =========================================================
         *
         * Source chưa gắn watermark ở đây.
         *
         * Watermark được gắn sau khi event đã:
         *
         * - resolve identity;
         * - normalize;
         * - trở thành SilverEvent hợp lệ.
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
         * =========================================================
         * BƯỚC 3
         * RESOLVE UE IDENTITY
         * =========================================================
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


        /*
         * Identity không resolve được được đưa vào
         * side output riêng thay vì làm job fail.
         */

        DataStream<InvalidIdentityRecord>
                invalidIdentityEvents =
                resolvedEvents.getSideOutput(
                        SilverIdentityProcessFunction
                                .INVALID_IDENTITY_TAG
                );


        /*
         * =========================================================
         * BƯỚC 4
         * NORMALIZE EVENT
         * =========================================================
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


        /*
         * Event không nằm trong supported event catalog
         * được route sang side output.
         */

        DataStream<UnsupportedEventRecord>
                unsupportedEvents =
                normalizedEvents.getSideOutput(
                        SilverEventProcessFunction
                                .UNSUPPORTED_EVENT_TAG
                );


        /*
         * =========================================================
         * BƯỚC 5
         * DEDUPLICATE + WATERMARK + LATE ROUTING
         * =========================================================
         */

        SilverStreamBuilder.Result streamResult =
                SilverStreamBuilder.build(
                        normalizedEvents,
                        config.stateTtlMs(),
                        config.watermarkMaxOutOfOrdernessMs(),
                        config.watermarkIdlenessMs()
                );


        /*
         * =========================================================
         * BƯỚC 6
         * MAIN SILVER OUTPUT
         * =========================================================
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
         * =========================================================
         * BƯỚC 7
         * INVALID IDENTITY OUTPUT
         * =========================================================
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
         * =========================================================
         * BƯỚC 8
         * UNSUPPORTED EVENT OUTPUT
         * =========================================================
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
         * =========================================================
         * BƯỚC 9
         * LATE EVENT OUTPUT
         * =========================================================
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
}