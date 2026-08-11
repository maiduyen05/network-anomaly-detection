package com.network.preprocess.operator;

import com.network.preprocess.model.BronzeErrorCode;
import com.network.preprocess.parser.BronzeDataException;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.zone.ZoneRules;
import java.util.List;
import java.util.Locale;

/**
 * Chuẩn hóa EVENT_TIME về UTC ISO-8601.
 *
 * <p>Class hỗ trợ ba dạng timestamp:</p>
 *
 * <ol>
 *     <li>
 *         Unix Epoch Milliseconds:
 *         {@code 1719385235407}
 *     </li>
 *
 *     <li>
 *         Timestamp đã có UTC offset:
 *         {@code 2026-08-03T13:15:30+07:00}
 *     </li>
 *
 *     <li>
 *         Timestamp local không có offset:
 *         {@code 2026-08-03 13:15:30}
 *         hoặc {@code 2026-08-03 13:15:30.123}
 *     </li>
 * </ol>
 *
 * <p>
 * Epoch milliseconds đã biểu diễn một thời điểm tuyệt đối nên
 * không áp dụng source timezone.
 * </p>
 *
 * <p>
 * Timestamp local không có offset được hiểu theo timezone cấu hình,
 * ví dụ Asia/Ho_Chi_Minh, sau đó chuyển sang UTC.
 * </p>
 */
public final class TimestampNormalizer implements Serializable {

    /**
     * Dataset hiện tại lưu EVENT_TIME dưới dạng Unix Epoch Milliseconds.
     *
     * Ví dụ:
     *
     * 1719385235407
     *
     * tương ứng:
     *
     * 2024-06-26T07:00:35.407Z
     */
    private static final int EPOCH_MILLIS_LENGTH = 13;

    /*
     * Formatter cho timestamp local:
     *
     * uuuu-MM-dd HH:mm:ss
     * uuuu-MM-dd HH:mm:ss.S
     * uuuu-MM-dd HH:mm:ss.SSS
     * ... tối đa 9 chữ số phần thập phân.
     *
     * Dùng uuuu thay cho yyyy để strict parsing hoạt động đúng với
     * LocalDateTime.
     */
    private static final DateTimeFormatter LOCAL_TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .appendPattern("uuuu-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(
                            ChronoField.NANO_OF_SECOND,
                            1,
                            9,
                            true
                    )
                    .optionalEnd()
                    .toFormatter(Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);

    /*
     * Chỉ lưu timezone dưới dạng String.
     *
     * Khi Flink serialize object này, String được serialize ổn định.
     * ZoneId sẽ được tạo lại khi cần.
     */
    private final String sourceTimezone;

    /*
     * transient khiến ZoneId không cần đi qua quá trình serialization
     * của Flink.
     */
    private transient ZoneId sourceZoneId;

    /**
     * Tạo timestamp normalizer.
     *
     * @param sourceTimezone timezone của timestamp local,
     *                       ví dụ Asia/Ho_Chi_Minh
     */
    public TimestampNormalizer(String sourceTimezone) {

        /*
         * Timezone đến từ application.yaml.
         *
         * Nếu thiếu timezone, đây là lỗi cấu hình nên job phải fail,
         * không được route từng record sang DLQ.
         */
        if (sourceTimezone == null || sourceTimezone.isBlank()) {
            throw new IllegalArgumentException(
                    "sourceTimezone must not be blank"
            );
        }

        try {
            /*
             * Kiểm tra timezone ngay khi khởi tạo.
             *
             * Ví dụ hợp lệ:
             *
             * Asia/Ho_Chi_Minh
             * UTC
             */
            ZoneId.of(sourceTimezone);

        } catch (DateTimeException exception) {

            throw new IllegalArgumentException(
                    "Invalid sourceTimezone: " + sourceTimezone,
                    exception
            );
        }

        this.sourceTimezone = sourceTimezone;
    }

    /**
     * Parse EVENT_TIME bắt buộc và chuyển về UTC ISO-8601.
     *
     * @param rawEventTime giá trị EVENT_TIME trong raw log
     * @return timestamp UTC, ví dụ 2024-06-26T07:00:35.407Z
     * @throws BronzeDataException nếu timestamp thiếu hoặc sai format
     */
    public String normalizeRequiredToUtc(
            String rawEventTime
    ) throws BronzeDataException {

        /*
         * EVENT_TIME là field bắt buộc.
         */
        if (rawEventTime == null || rawEventTime.isBlank()) {
            throw invalidTimestamp(
                    "EVENT_TIME is required"
            );
        }

        /*
         * Loại khoảng trắng ở đầu và cuối.
         */
        String normalizedInput = rawEventTime.trim();

        /*
         * ==========================================================
         * FORMAT 1 - UNIX EPOCH MILLISECONDS
         * ==========================================================
         *
         * Dataset thực tế hiện tại lưu EVENT_TIME dạng:
         *
         * 1719385235407
         *
         * Đây là số milliseconds kể từ:
         *
         * 1970-01-01T00:00:00Z
         *
         * Epoch timestamp đã chứa thời điểm tuyệt đối,
         * vì vậy không áp dụng sourceTimezone.
         */
        if (isEpochMilliseconds(normalizedInput)) {

            try {
                long epochMillis =
                        Long.parseLong(normalizedInput);

                return Instant
                        .ofEpochMilli(epochMillis)
                        .toString();

            } catch (NumberFormatException
                     | DateTimeException exception) {

                throw invalidTimestamp(
                        "EVENT_TIME has invalid epoch milliseconds"
                );
            }
        }

        /*
         * ==========================================================
         * FORMAT 2 - ISO TIMESTAMP CÓ OFFSET
         * ==========================================================
         *
         * Ví dụ:
         *
         * 2026-08-03T13:15:30+07:00
         */
        try {
            OffsetDateTime offsetDateTime =
                    OffsetDateTime.parse(
                            normalizedInput,
                            DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    );

            return offsetDateTime
                    .toInstant()
                    .toString();

        } catch (DateTimeParseException ignored) {
            /*
             * Input có thể là local datetime,
             * thử parser thứ ba bên dưới.
             */
        }

        /*
         * ==========================================================
         * FORMAT 3 - LOCAL DATETIME
         * ==========================================================
         *
         * Ví dụ:
         *
         * 2026-08-03 13:15:30
         * 2026-08-03 13:15:30.123
         */
        try {
            LocalDateTime localDateTime =
                    LocalDateTime.parse(
                            normalizedInput,
                            LOCAL_TIMESTAMP_FORMATTER
                    );

            ZoneRules zoneRules =
                    zoneId().getRules();

            /*
             * Một local timestamp có thể có:
             *
             * - 1 offset: hợp lệ.
             * - 0 offset: DST gap.
             * - 2 offset: DST overlap.
             */
            List<ZoneOffset> validOffsets =
                    zoneRules.getValidOffsets(
                            localDateTime
                    );

            /*
             * Chỉ chấp nhận timestamp ánh xạ tới đúng một offset.
             */
            if (validOffsets.size() != 1) {
                throw invalidTimestamp(
                        "EVENT_TIME is ambiguous or does not exist "
                                + "in the configured timezone"
                );
            }

            Instant instant =
                    localDateTime.toInstant(
                            validOffsets.get(0)
                    );

            return instant.toString();

        } catch (DateTimeParseException exception) {

            throw invalidTimestamp(
                    "EVENT_TIME has invalid format"
            );
        }
    }

    /**
     * Kiểm tra EVENT_TIME có phải Unix Epoch Milliseconds hay không.
     *
     * <p>
     * Contract hiện tại chỉ nhận đúng 13 chữ số để tránh nhầm
     * Unix Epoch Seconds 10 chữ số với Epoch Milliseconds.
     * </p>
     */
    private boolean isEpochMilliseconds(String value) {

        if (value.length() != EPOCH_MILLIS_LENGTH) {
            return false;
        }

        for (int index = 0; index < value.length(); index++) {

            if (!Character.isDigit(
                    value.charAt(index)
            )) {
                return false;
            }
        }

        return true;
    }

    /**
     * Tạo BronzeDataException thống nhất cho timestamp lỗi.
     */
    private BronzeDataException invalidTimestamp(
            String safeMessage
    ) {
        return new BronzeDataException(
                BronzeErrorCode.INVALID_EVENT_TIME,
                safeMessage
        );
    }

    /**
     * Lấy ZoneId hiện tại hoặc tạo lại sau khi Flink deserialize object.
     */
    private ZoneId zoneId() {

        if (sourceZoneId == null) {
            sourceZoneId =
                    ZoneId.of(sourceTimezone);
        }

        return sourceZoneId;
    }
}