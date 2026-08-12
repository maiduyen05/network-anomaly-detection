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
 */
public final class BronzeWatermarkStrategyFactory {

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
                 * Watermark chậm hơn timestamp lớn nhất
                 * theo khoảng out-of-orderness cấu hình.
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
                 * sẽ được đánh dấu idle để không giữ watermark.
                 */
                .withIdleness(
                        Duration.ofMillis(
                                idleTimeoutMs
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