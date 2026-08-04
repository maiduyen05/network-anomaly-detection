package com.network.preprocess.silver.time;

import com.network.preprocess.model.SilverEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * Factory tạo WatermarkStrategy cho SilverEvent.
 */
public final class SilverWatermarkStrategyFactory {

    /**
     * Không cho tạo object factory vì class chỉ chứa static method.
     */
    private SilverWatermarkStrategyFactory() {
    }

    /**
     * Tạo watermark strategy cho Silver stream.
     *
     * @param maxOutOfOrdernessMs thời gian cho phép event đến không đúng thứ tự
     * @param idleTimeoutMs thời gian xác định một Kafka partition đang idle
     * @return WatermarkStrategy cho SilverEvent
     */
    public static WatermarkStrategy<SilverEvent> create(
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

        SilverEventTimestampExtractor timestampExtractor =
                new SilverEventTimestampExtractor();

        return WatermarkStrategy
                /*
                 * Flink giữ watermark chậm hơn timestamp lớn nhất
                 * theo đúng khoảng maxOutOfOrdernessMs.
                 */
                .<SilverEvent>forBoundedOutOfOrderness(
                        Duration.ofMillis(maxOutOfOrdernessMs)
                )

                /*
                 * Timestamp của record được lấy từ event_time,
                 * không lấy Kafka timestamp và không lấy processing time.
                 */
                .withTimestampAssigner(
                        (event, previousTimestamp) ->
                                timestampExtractor.extractTimestamp(event)
                )

                /*
                 * Partition không có dữ liệu sẽ được đánh dấu idle,
                 * tránh làm watermark toàn stream đứng yên.
                 */
                .withIdleness(
                        Duration.ofMillis(idleTimeoutMs)
                );
    }
}