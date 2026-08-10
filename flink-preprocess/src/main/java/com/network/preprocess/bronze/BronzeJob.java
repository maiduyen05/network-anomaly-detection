package com.network.preprocess.bronze;

import com.network.preprocess.config.BronzeJobConfig;
import com.network.preprocess.model.BronzeDlqRecord;
import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.KafkaRawRecord;
import com.network.preprocess.operator.TimestampNormalizer;
import com.network.preprocess.operator.TypeCastOperator;
import com.network.preprocess.parser.JsonEventParser;
import com.network.preprocess.parser.RawLogLineParser;
import com.network.preprocess.runtime.FlinkEnvironmentConfigurator;
import com.network.preprocess.sink.BronzeKafkaSinks;
import com.network.preprocess.source.RawEventKafkaSource;
import com.network.preprocess.validation.SchemaValidator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Entry point của Bronze Flink Job.
 *
 * <p>
 * Pipeline:
 * </p>
 *
 * <pre>
 * raw.ue.log.line
 *      ↓
 * KafkaRawRecord
 *      ↓
 * BronzeTransformer
 *      ├── hợp lệ → BronzeEvent
 *      │                ↓
 *      │         bronze.ue.event
 *      │
 *      └── lỗi → BronzeDlqRecord
 *                       ↓
 *                dlq.ue.log.line
 * </pre>
 *
 * <p>
 * Bronze không hard-code runtime configuration.
 * Parallelism/checkpoint được lấy từ application.yaml
 * thông qua BronzeJobConfig và FlinkEnvironmentConfigurator.
 * </p>
 */
public final class BronzeJob {

    private BronzeJob() {
    }


    /**
     * Entry point khi submit Bronze Job lên Flink.
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

        BronzeJobConfig config =
                BronzeJobConfig.loadFromClasspath(
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
         * CONFIGURE FLINK RUNTIME
         * =========================================================
         *
         * Không còn:
         *
         * JOB_PARALLELISM = 3
         * CHECKPOINT_INTERVAL_MS = 60000
         *
         * Tất cả lấy từ:
         *
         * application.yaml
         *       ↓
         * BronzeJobConfig
         *       ↓
         * FlinkEnvironmentConfigurator
         */

        FlinkEnvironmentConfigurator.configure(
                env,
                config
        );


        /*
         * =========================================================
         * BƯỚC 4
         * KAFKA RAW SOURCE
         * =========================================================
         *
         * Bronze chưa cần event-time watermark.
         *
         * Timestamp nghiệp vụ vẫn được parse và lưu trong
         * BronzeEvent, nhưng Bronze không window theo event time.
         */

        DataStream<KafkaRawRecord> rawStream =
                env.fromSource(
                                RawEventKafkaSource.create(
                                        config
                                ),
                                WatermarkStrategy.noWatermarks(),
                                "bronze-kafka-source"
                        )
                        .uid(
                                "bronze-kafka-source-v1"
                        );


        /*
         * =========================================================
         * BƯỚC 5
         * BUILD BRONZE TRANSFORMER
         * =========================================================
         */

        BronzeTransformer transformer =
                new BronzeTransformer(

                        /*
                         * Raw envelope schema.
                         */
                        config.envelopeSchemaVersion(),

                        /*
                         * Bronze output schema.
                         */
                        config.outputSchemaVersion(),

                        /*
                         * Parse raw JSON envelope.
                         */
                        new JsonEventParser(),

                        /*
                         * Validate envelope/schema.
                         */
                        new SchemaValidator(),

                        /*
                         * Parse raw UE log:
                         *
                         * delimiter = ;
                         * field count = 52
                         *
                         * Các giá trị này lấy từ YAML.
                         */
                        new RawLogLineParser(
                                config.delimiter(),
                                config.fieldCount()
                        ),

                        /*
                         * Chuẩn hóa timestamp theo timezone cấu hình.
                         */
                        new TimestampNormalizer(
                                config.localTimezone()
                        ),

                        /*
                         * String -> typed values.
                         */
                        new TypeCastOperator()
                );


        /*
         * =========================================================
         * BƯỚC 6
         * TRANSFORM RAW RECORD
         * =========================================================
         *
         * Main output:
         *
         * BronzeEvent
         *
         * Side output:
         *
         * BronzeDlqRecord
         */

        SingleOutputStreamOperator<BronzeEvent>
                bronzeStream =
                rawStream
                        .process(
                                new BronzeProcessFunction(
                                        transformer
                                )
                        )
                        .name(
                                "bronze-transform"
                        )
                        .uid(
                                "bronze-transform-v1"
                        );


        /*
         * =========================================================
         * BƯỚC 7
         * DLQ SIDE OUTPUT
         * =========================================================
         */

        DataStream<BronzeDlqRecord> dlqStream =
                bronzeStream.getSideOutput(
                        BronzeProcessFunction.DLQ_TAG
                );


        /*
         * =========================================================
         * BƯỚC 8
         * VALID BRONZE OUTPUT
         * =========================================================
         */

        bronzeStream
                .sinkTo(
                        BronzeKafkaSinks.eventSink(
                                config
                        )
                )
                .name(
                        "bronze-output-sink"
                )
                .uid(
                        "bronze-output-sink-v1"
                );


        /*
         * =========================================================
         * BƯỚC 9
         * BRONZE DLQ
         * =========================================================
         */

        dlqStream
                .sinkTo(
                        BronzeKafkaSinks.dlqSink(
                                config
                        )
                )
                .name(
                        "bronze-dlq-sink"
                )
                .uid(
                        "bronze-dlq-sink-v1"
                );


        /*
         * =========================================================
         * BƯỚC 10
         * EXECUTE
         * =========================================================
         *
         * Tên job không còn hard-code trong Java.
         *
         * application.yaml:
         *
         * bronze:
         *   job-name: flink-bronze-v1
         */

        env.execute(
                config.jobName()
        );
    }
}