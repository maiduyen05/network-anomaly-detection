package com.network.preprocess.silver.time;

import com.network.preprocess.model.SilverEvent;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Objects;

/**
 * Chuyển event_time của SilverEvent thành epoch millisecond.
 *
 * <p>Bronze đã chịu trách nhiệm:</p>
 *
 * <ul>
 *     <li>Parse EVENT_TIME.</li>
 *     <li>Chuyển về UTC.</li>
 *     <li>Fallback khi source timestamp không hợp lệ.</li>
 * </ul>
 *
 * <p>Vì vậy đến Silver, eventTime phải là chuỗi ISO-8601 hợp lệ,
 * ví dụ:</p>
 *
 * <pre>
 * 2026-07-08T10:15:30.123Z
 * </pre>
 */
public final class SilverEventTimestampExtractor
        implements Serializable {

    /**
     * Lấy epoch millisecond từ SilverEvent.
     *
     * @param event SilverEvent cần lấy event time
     * @return event time dạng epoch millisecond
     */
    public long extractTimestamp(SilverEvent event) {
        Objects.requireNonNull(
                event,
                "SilverEvent must not be null"
        );

        String eventTime = event.eventTime();

        if (eventTime == null || eventTime.isBlank()) {
            throw new IllegalArgumentException(
                    "SilverEvent.eventTime must not be blank"
            );
        }

        try {
            return Instant.parse(eventTime).toEpochMilli();
        } catch (DateTimeException exception) {
            /*
             * Đây là lỗi contract giữa Bronze và Silver.
             *
             * Timestamp raw sai phải được Bronze fallback hoặc đưa DLQ;
             * Silver không tự dùng system time vì sẽ làm sai thứ tự event.
             */
            throw new IllegalArgumentException(
                    "SilverEvent.eventTime is not valid ISO-8601 UTC: "
                            + eventTime,
                    exception
            );
        }
    }
}