package com.network.preprocess.silver.time;

import com.network.preprocess.model.BronzeEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Tạo WatermarkStrategy cho BronzeEvent tại Kafka source của Silver.
 *
 * <p>
 * Watermark phải được tạo ngay tại source để KafkaSource
 * có thể theo dõi tiến độ event-time theo từng Kafka split/partition.
 * </p>
 *
 * <p>
 * Bronze đã chuẩn hóa EVENT_TIME thành ISO-8601 UTC.
 * Vì vậy Silver chỉ cần đọc BronzeEvent.eventTime().
 * </p>
 *
 * <p>
 * Ngoài bounded out-of-orderness, strategy này bật watermark alignment.
 * Mục tiêu là không cho một Kafka partition chạy nhanh hơn các partition
 * còn lại hàng phút trong lúc replay/backfill. Nếu không align, sau các
 * lần keyBy/shuffle ở Silver, cùng một UE có thể nhận event từ những
 * source partition đang ở các mốc event-time rất xa nhau và Silver Kafka
 * output sẽ bị đảo timeline mạnh.
 * </p>
 */
public final class BronzeWatermarkStrategyFactory {

    /**
     * Các split của cùng Bronze Kafka source tham gia cùng một nhóm
     * watermark alignment.
     */
    static final String WATERMARK_ALIGNMENT_GROUP =
            "silver-bronze-kafka-source";

    /**
     * Cho phép watermark nhanh nhất đi trước watermark chậm nhất tối đa 5s.
     *
     * <p>
     * Dataset replay thực tế cho thấy Bronze partition chỉ có disorder
     * dưới khoảng 19 giây, trong khi Silver trước đây sinh backward gap
     * gần 300 giây. Drift 5 giây đủ nhỏ để tránh partition chạy quá xa,
     * đồng thời không thay thế bounded-out-of-orderness 30 giây dùng để
     * xử lý disorder thật bên trong từng partition.
     * </p>
     */
    static final long WATERMARK_ALIGNMENT_MAX_DRIFT_MS =
            5_000L;

    /**
     * Coordinator kiểm tra/cập nhật trạng thái alignment mỗi 1 giây.
     */
    static final long WATERMARK_ALIGNMENT_UPDATE_INTERVAL_MS =
            1_000L;

    private BronzeWatermarkStrategyFactory() {
    }

    /**
     * Tạo watermark strategy cho BronzeEvent.
     *
     * @param maxOutOfOrdernessMs khoảng event được phép đến lệch thứ tự
     * @param idleTimeoutMs thời gian xác định source split đang idle
     */
    public static WatermarkStrategy<BronzeEvent> create(
            long maxOutOfOrdernessMs,
            long idleTimeoutMs
    ) {

        if (maxOutOfOrdernessMs < 0) {
            throw new IllegalArgumentException(
                    "maxOutOfOrdernessMs must not be negative"
            );
        }

        if (idleTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "idleTimeoutMs must be greater than zero"
            );
        }

        return WatermarkStrategy

                /*
                 * Watermark của từng Kafka split chậm hơn timestamp lớn
                 * nhất đã thấy theo bounded-out-of-orderness cấu hình.
                 */
                .<BronzeEvent>forBoundedOutOfOrderness(
                        Duration.ofMillis(
                                maxOutOfOrdernessMs
                        )
                )

                /*
                 * Event time lấy từ payload BronzeEvent,
                 * không lấy Kafka CreateTime.
                 */
                .withTimestampAssigner(
                        (event, previousTimestamp) ->
                                extractEventTimeMillis(event)
                )

                /*
                 * Kafka partition không có dữ liệu trong khoảng này
                 * sẽ được đánh dấu idle để không giữ watermark chung.
                 */
                .withIdleness(
                        Duration.ofMillis(
                                idleTimeoutMs
                        )
                )

                /*
                 * Không cho một source split/partition chạy quá xa
                 * so với các split còn lại.
                 *
                 * Với KafkaSource (FLIP-27/new Source API), Flink có thể
                 * pause split đang đi quá nhanh rồi resume khi các split
                 * còn lại bắt kịp. Đây là điểm rất quan trọng khi replay
                 * vì tốc độ consume giữa các Kafka partition có thể khác
                 * nhau đáng kể dù dữ liệu gốc không disorder lớn.
                 */
                .withWatermarkAlignment(
                        WATERMARK_ALIGNMENT_GROUP,
                        Duration.ofMillis(
                                WATERMARK_ALIGNMENT_MAX_DRIFT_MS
                        ),
                        Duration.ofMillis(
                                WATERMARK_ALIGNMENT_UPDATE_INTERVAL_MS
                        )
                );
    }

    /**
     * Chuyển BronzeEvent.eventTime ISO-8601 UTC
     * thành epoch millisecond.
     */
    static long extractEventTimeMillis(
            BronzeEvent event
    ) {

        Objects.requireNonNull(
                event,
                "BronzeEvent must not be null"
        );

        String eventTime =
                event.eventTime();

        if (eventTime == null
                || eventTime.isBlank()) {

            throw new IllegalArgumentException(
                    "BronzeEvent.eventTime must not be blank"
            );
        }

        try {

            return Instant
                    .parse(eventTime)
                    .toEpochMilli();

        } catch (DateTimeException exception) {

            /*
             * Đây là contract violation giữa Bronze và Silver.
             *
             * Bronze phải chịu trách nhiệm normalize EVENT_TIME.
             * Silver không được fallback về processing time.
             */
            throw new IllegalArgumentException(
                    "BronzeEvent.eventTime is not valid ISO-8601 UTC: "
                            + eventTime,
                    exception
            );
        }
    }
}
