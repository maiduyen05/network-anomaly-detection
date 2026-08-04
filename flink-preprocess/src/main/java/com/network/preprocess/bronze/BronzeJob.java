package com.network.preprocess.bronze;

import com.network.preprocess.config.BronzeJobConfig;
import com.network.preprocess.model.BronzeDlqRecord;
import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.KafkaRawRecord;
import com.network.preprocess.operator.TimestampNormalizer;
import com.network.preprocess.operator.TypeCastOperator;
import com.network.preprocess.parser.JsonEventParser;
import com.network.preprocess.parser.RawLogLineParser;
import com.network.preprocess.sink.BronzeKafkaSinks;
import com.network.preprocess.source.RawEventKafkaSource;
import com.network.preprocess.validation.SchemaValidator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Điểm bắt đầu chạy độc lập của Flink job tầng Bronze.
 *
 * <p>
 * Class này không trực tiếp parse hoặc validate từng record.
 * Nhiệm vụ chính của nó là lắp ráp toàn bộ các thành phần của pipeline Bronze:
 * </p>
 *
 * <pre>
 * Kafka raw topic
 *      ↓
 * RawEventKafkaSource
 *      ↓
 * KafkaRawRecord
 *      ↓
 * BronzeProcessFunction
 *      ↓
 * BronzeTransformer
 *      ├── hợp lệ → BronzeEvent
 *      └── lỗi    → BronzeDlqRecord
 *                       ↓
 * Kafka Sink
 *      ├── bronze output topic
 *      └── Bronze DLQ topic
 * </pre>
 */
public final class BronzeJob {

    /**
     * Parallelism mặc định của Bronze job.
     *
     * <p>Giá trị này được khai báo tại đây vì {@link BronzeJobConfig}
     * hiện chỉ chứa cấu hình Kafka và contract dữ liệu Bronze, chưa có
     * thuộc tính {@code parallelism} trong {@code application.yaml}.</p>
     */
    private static final int JOB_PARALLELISM = 3;

    /**
     * Chu kỳ tạo checkpoint: 60 giây.
     *
     * <p>Kafka sink dùng chế độ EXACTLY_ONCE cần checkpointing để commit
     * transaction. Khi bổ sung nhóm cấu hình {@code flink} vào YAML ở
     * checkpoint sau, hằng số này có thể được chuyển vào BronzeJobConfig.</p>
     */
    private static final long CHECKPOINT_INTERVAL_MS = 60_000L;

    /**
     * Constructor private để không cho tạo object BronzeJob.
     *
     * <p>
     * Class này chỉ chứa method {@link #main(String[])} dùng để khởi động job,
     * không có trạng thái riêng và không cần được khởi tạo bằng {@code new}.
     * </p>
     */
    private BronzeJob() {
    }

    /**
     * Entry point của Flink Bronze job.
     *
     * <p>
     * Khi submit job, Flink sẽ bắt đầu chạy từ method này.
     * Method thực hiện lần lượt các bước:
     * </p>
     *
     * <ol>
     *     <li>Đọc cấu hình Bronze từ file YAML.</li>
     *     <li>Tạo Flink execution environment.</li>
     *     <li>Cấu hình parallelism và checkpoint.</li>
     *     <li>Tạo Kafka source để đọc raw record.</li>
     *     <li>Tạo BronzeTransformer và các dependency của nó.</li>
     *     <li>Chạy transform và tách record hợp lệ/không hợp lệ.</li>
     *     <li>Ghi record hợp lệ vào Bronze output topic.</li>
     *     <li>Ghi record lỗi vào Bronze DLQ topic.</li>
     *     <li>Submit job cho Flink thực thi.</li>
     * </ol>
     *
     * @param args tham số dòng lệnh khi chạy job;
     *             hiện tại chưa được sử dụng
     * @throws Exception nếu quá trình load config, tạo job
     *                   hoặc submit job thất bại
     */
    public static void main(String[] args)
            throws Exception {

        /*
         * ============================================================
         * BƯỚC 1: ĐỌC CẤU HÌNH CỦA BRONZE JOB
         * ============================================================
         *
         * Toàn bộ cấu hình được đọc từ application.yaml.
         * Method loadFromClasspath() tìm file trong src/main/resources.
         *
         */
        BronzeJobConfig config =
                BronzeJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        /*
         * ============================================================
         * BƯỚC 2: TẠO FLINK EXECUTION ENVIRONMENT
         * ============================================================
         *
         * StreamExecutionEnvironment là object trung tâm dùng để xây dựng
         * toàn bộ Flink DataStream job.
         *
         * Mọi source, operator, transform và sink đều được đăng ký
         * vào environment này.
         *
         * Ở bước này job mới chỉ đang được xây dựng.
         * Flink chưa bắt đầu xử lý dữ liệu cho đến khi gọi env.execute().
         */
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment
                        .getExecutionEnvironment();

        /*
         * ============================================================
         * BƯỚC 3: CẤU HÌNH PARALLELISM
         * ============================================================
         * parallelism = 3
         *
         * thì Kafka source, transform và sink có thể chạy với 3 subtask.
         *
         * Parallelism cần được lựa chọn phù hợp với:
         * - số partition của Kafka topic;
         * - tài nguyên TaskManager;
         * - lưu lượng dữ liệu;
         * - yêu cầu throughput.
         */
        env.setParallelism(JOB_PARALLELISM);

        /*
         * ============================================================
         * BƯỚC 4: BẬT CHECKPOINTING
         * ============================================================
         *
         * Checkpoint dùng để Flink định kỳ lưu lại trạng thái xử lý,
         * ví dụ:
         * - Kafka offset đã đọc;
         * - state của operator;
         * - trạng thái transaction của Kafka sink.
         *
         * Nếu job gặp lỗi và restart, Flink có thể khôi phục từ checkpoint
         * gần nhất thay vì đọc lại toàn bộ dữ liệu từ đầu.
         *
         * CheckpointingMode.EXACTLY_ONCE có nghĩa là Flink cố gắng đảm bảo
         * mỗi record chỉ ảnh hưởng đúng một lần đến kết quả cuối cùng,
         * kể cả khi job bị restart.
         *
         * Kafka sink chạy ở chế độ EXACTLY_ONCE yêu cầu checkpointing
         * vì việc commit Kafka transaction được liên kết với checkpoint.
         */
        env.enableCheckpointing(
                CHECKPOINT_INTERVAL_MS,
                CheckpointingMode.EXACTLY_ONCE
        );

        /*
         * Chỉ cho phép tối đa một checkpoint chạy tại cùng một thời điểm.
         *
         * Ví dụ:
         * nếu checkpoint trước chưa hoàn thành,
         * Flink sẽ không khởi động thêm checkpoint mới.
         *
         * Cấu hình này giúp:
         * - giảm áp lực lên storage;
         * - tránh nhiều checkpoint chồng chéo;
         * - đơn giản hóa hoạt động của Kafka transactional sink.
         */
        env.getCheckpointConfig()
                .setMaxConcurrentCheckpoints(1);

        /*
         * ============================================================
         * BƯỚC 5: TẠO RAW KAFKA SOURCE
         * ============================================================
         *
         * RawEventKafkaSource.create(config) tạo KafkaSource đã được cấu hình:
         * - Kafka bootstrap servers;
         * - raw input topic;
         * - consumer group;
         * - starting offsets;
         * - isolation level;
         * - KafkaRawRecordDeserializationSchema.
         *
         * Source đọc từng Kafka message và chuyển thành KafkaRawRecord.
         *
         * KafkaRawRecord giữ:
         * - topic;
         * - partition;
         * - offset;
         * - Kafka timestamp;
         * - raw value.
         *
         * Ở đây dùng WatermarkStrategy.noWatermarks() vì Bronze chưa thực hiện
         * tính toán theo event time như window, join theo thời gian
         * hoặc xử lý late event.
         *
         * Bronze chỉ parse timestamp từ dữ liệu và lưu vào BronzeEvent.
         * Watermark thường được áp dụng ở tầng Silver hoặc Gold,
         * nơi thực sự có event-time computation.
         */
        DataStream<KafkaRawRecord> rawStream =
                env.fromSource(
                                RawEventKafkaSource.create(config),
                                WatermarkStrategy.noWatermarks(),
                                "bronze-kafka-source"
                        )

                        /*
                         * UID là định danh ổn định của operator trong Flink.
                         *
                         * UID giúp Flink ánh xạ state cũ với operator tương ứng
                         * khi restore từ checkpoint hoặc savepoint.
                         *
                         * Không nên tùy tiện thay đổi UID sau khi job đã chạy
                         * trong production, nếu operator có state cần khôi phục.
                         */
                        .uid("bronze-kafka-source-v1");

        /*
         * ============================================================
         * BƯỚC 6: TẠO BRONZE TRANSFORMER
         * ============================================================
         *
         * BronzeTransformer chứa logic xử lý nghiệp vụ của tầng Bronze.
         *
         * Nó nhận KafkaRawRecord và thực hiện các bước như:
         *
         * 1. Parse JSON envelope.
         * 2. Kiểm tra envelope schema version.
         * 3. Lấy raw log line.
         * 4. Tách raw log line theo delimiter.
         * 5. Kiểm tra số lượng field.
         * 6. Chuẩn hóa timestamp.
         * 7. Ép kiểu dữ liệu.
         * 8. Tạo BronzeEvent nếu hợp lệ.
         * 9. Tạo BronzeDlqRecord nếu dữ liệu không hợp lệ.
         *
         * Các dependency được truyền vào constructor thay vì tự tạo bên trong,
         * giúp code:
         * - dễ test;
         * - dễ thay thế implementation;
         * - ít phụ thuộc cứng;
         * - rõ trách nhiệm từng thành phần.
         */
        BronzeTransformer transformer =
                new BronzeTransformer(

                        /*
                         * Schema version mà raw JSON envelope bắt buộc phải có.
                         *
                         * Ví dụ:
                         * raw-envelope-v1
                         */
                        config.envelopeSchemaVersion(),

                        /*
                         * Schema version được gắn vào BronzeEvent đầu ra.
                         *
                         * Ví dụ:
                         * bronze-v1
                         */
                        config.outputSchemaVersion(),

                        /*
                         * Dùng Jackson để parse raw JSON string
                         * thành object Java đại diện cho raw envelope.
                         */
                        new JsonEventParser(),

                        /*
                         * Kiểm tra schema version và các trường bắt buộc
                         * của raw envelope.
                         */
                        new SchemaValidator(),

                        /*
                         * Tách raw log line thành danh sách field.
                         *
                         * Parser sử dụng:
                         * - delimiter từ cấu hình;
                         * - fieldCount mong đợi từ cấu hình.
                         *
                         * Ví dụ:
                         * delimiter = ";"
                         * fieldCount = 52
                         */
                        new RawLogLineParser(
                                config.delimiter(),
                                config.fieldCount()
                        ),

                        /*
                         * Chuyển timestamp dạng chuỗi trong raw log
                         * thành timestamp chuẩn.
                         *
                         * localTimezone dùng để hiểu chính xác timestamp
                         * khi raw data không chứa timezone rõ ràng.
                         *
                         * Ví dụ:
                         * Asia/Ho_Chi_Minh
                         */
                        new TimestampNormalizer(
                                config.localTimezone()
                        ),

                        /*
                         * Ép các raw field dạng String thành kiểu Java phù hợp,
                         * ví dụ:
                         * - String → Integer;
                         * - String → Long;
                         * - String → Double;
                         * - String → Boolean.
                         */
                        new TypeCastOperator()
                );

        /*
         * ============================================================
         * BƯỚC 7: XỬ LÝ RAW STREAM
         * ============================================================
         *
         * rawStream.process(...) áp dụng BronzeProcessFunction
         * cho từng KafkaRawRecord.
         *
         * BronzeProcessFunction sẽ gọi BronzeTransformer.
         *
         * Kết quả được phân thành hai luồng:
         *
         * 1. Main output:
         *    chứa BronzeEvent hợp lệ.
         *
         * 2. Side output:
         *    chứa BronzeDlqRecord của các record lỗi dữ liệu.
         *
         * Kiểu chính của operator là BronzeEvent, nên biến bronzeStream
         * có kiểu SingleOutputStreamOperator<BronzeEvent>.
         */
        SingleOutputStreamOperator<BronzeEvent>
                bronzeStream =
                rawStream
                        .process(
                                new BronzeProcessFunction(
                                        transformer
                                )
                        )

                        /*
                         * name() là tên dễ đọc hiển thị trong:
                         * - Flink Web UI;
                         * - execution graph;
                         * - metrics;
                         * - logs.
                         *
                         * name không nhất thiết phải ổn định như UID,
                         * nhưng nên đặt rõ ý nghĩa của operator.
                         */
                        .name("bronze-transform")

                        /*
                         * UID ổn định của operator transform.
                         *
                         * Nếu sau này dùng savepoint hoặc restore state,
                         * Flink dựa vào UID này để xác định đúng operator.
                         */
                        .uid("bronze-transform-v1");

        /*
         * ============================================================
         * BƯỚC 8: LẤY SIDE OUTPUT CHỨA RECORD LỖI
         * ============================================================
         *
         * BronzeProcessFunction.DLQ_TAG là OutputTag dùng để đánh dấu
         * luồng dữ liệu lỗi.
         *
         * bronzeStream là main output chứa BronzeEvent.
         *
         * getSideOutput(...) lấy ra luồng phụ chứa BronzeDlqRecord.
         *
         * Sau bước này ta có hai stream độc lập:
         *
         * bronzeStream:
         *     record hợp lệ.
         *
         * dlqStream:
         *     record dữ liệu lỗi cần lưu để kiểm tra hoặc xử lý lại.
         */
        DataStream<BronzeDlqRecord> dlqStream =
                bronzeStream.getSideOutput(
                        BronzeProcessFunction.DLQ_TAG
                );

        /*
         * ============================================================
         * BƯỚC 9: GHI BRONZE EVENT HỢP LỆ VÀO KAFKA
         * ============================================================
         *
         * BronzeKafkaSinks.eventSink(config) tạo KafkaSink dành cho
         * BronzeEvent hợp lệ.
         *
         * Sink thường thực hiện:
         * - lấy Kafka key;
         * - serialize BronzeEvent thành JSON snake_case;
         * - chuyển key và value thành byte[];
         * - tạo ProducerRecord;
         * - ghi vào Bronze output topic.
         *
         * Ví dụ topic:
         * bronze.ue.event
         */
        bronzeStream
                .sinkTo(
                        BronzeKafkaSinks.eventSink(config)
                )
                .name("bronze-output-sink")
                .uid("bronze-output-sink-v1");

        /*
         * ============================================================
         * BƯỚC 10: GHI RECORD LỖI VÀO BRONZE DLQ
         * ============================================================
         *
         * dlqStream chứa các record có lỗi dữ liệu dự kiến, ví dụ:
         * - raw value null;
         * - JSON sai định dạng;
         * - schema version không hỗ trợ;
         * - thiếu trường bắt buộc;
         * - sai số lượng field;
         * - timestamp không hợp lệ;
         * - field không ép kiểu được.
         *
         * BronzeKafkaSinks.dlqSink(config) serialize BronzeDlqRecord
         * thành JSON và ghi vào DLQ topic.
         *
         * Ví dụ topic:
         * dlq.ue.log.line
         *
         * DLQ giúp:
         * - không làm dừng toàn bộ job vì một record xấu;
         * - lưu lại thông tin lỗi;
         * - truy vết theo topic, partition và offset;
         * - thống kê lỗi dữ liệu;
         * - xử lý lại record sau khi sửa dữ liệu hoặc parser.
         */
        dlqStream
                .sinkTo(
                        BronzeKafkaSinks.dlqSink(config)
                )
                .name("bronze-dlq-sink")
                .uid("bronze-dlq-sink-v1");

        /*
         * ============================================================
         * BƯỚC 11: SUBMIT VÀ CHẠY FLINK JOB
         * ============================================================
         *
         * env.execute(...) gửi execution graph cho Flink runtime
         * và bắt đầu xử lý dữ liệu liên tục.
         *
         * "flink-bronze-v1" là tên job được hiển thị trên Flink Web UI.
         */
        env.execute("flink-bronze-v1");
    }
}
