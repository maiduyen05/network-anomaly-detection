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
 * <p>Class hỗ trợ hai dạng timestamp:</p>
 *
 * <ol>
 *     <li>
 *         Timestamp đã có UTC offset:
 *         {@code 2026-08-03T13:15:30+07:00}
 *     </li>
 *     <li>
 *         Timestamp local không có offset:
 *         {@code 2026-08-03 13:15:30}
 *         hoặc {@code 2026-08-03 13:15:30.123}
 *     </li>
 * </ol>
 *
 * <p>Timestamp local được hiểu theo timezone cấu hình, ví dụ
 * Asia/Ho_Chi_Minh, sau đó chuyển sang UTC.</p>
 */
public final class TimestampNormalizer implements Serializable {

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
             * Asia/Ho_Chi_Minh
             * UTC
             */
            ZoneId.of(sourceTimezone);

        } catch (DateTimeException exception) {
            /*
             * Timezone sai là lỗi cấu hình hệ thống.
             */
            throw new IllegalArgumentException(
                    "Invalid sourceTimezone: " + sourceTimezone,
                    exception
            );
        }

        /*
         * Giữ lại giá trị cấu hình đã kiểm tra.
         */
        this.sourceTimezone = sourceTimezone;
    }

    /**
     * Parse EVENT_TIME bắt buộc và chuyển về UTC ISO-8601.
     *
     * @param rawEventTime giá trị EVENT_TIME trong raw log
     * @return timestamp UTC, ví dụ 2026-08-03T06:15:30Z
     * @throws BronzeDataException nếu timestamp thiếu hoặc sai format
     */
    public String normalizeRequiredToUtc(
            String rawEventTime
    ) throws BronzeDataException {

        /*
         * EVENT_TIME là field bắt buộc.
         *
         * Khác với numeric field, timestamp rỗng không được giữ null.
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
         * Thử parse timestamp có offset trước.
         *
         * Ví dụ:
         * 2026-08-03T13:15:30+07:00
         */
        try {
            OffsetDateTime offsetDateTime =
                    OffsetDateTime.parse(
                            normalizedInput,
                            DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    );

            /*
             * toInstant() chuyển timestamp có offset về cùng một
             * thời điểm tuyệt đối trong UTC.
             *
             * Instant.toString() xuất ISO-8601 với ký hiệu Z.
             */
            return offsetDateTime
                    .toInstant()
                    .toString();

        } catch (DateTimeParseException ignored) {
            /*
             * Không kết luận lỗi ngay.
             *
             * Input có thể là timestamp local không có offset,
             * nên ta thử parser thứ hai ở bên dưới.
             */
        }

        /*
         * Thử parse timestamp local.
         *
         * Ví dụ:
         * 2026-08-03 13:15:30.123
         */
        try {
            LocalDateTime localDateTime =
                    LocalDateTime.parse(
                            normalizedInput,
                            LOCAL_TIMESTAMP_FORMATTER
                    );

            /*
             * Lấy quy tắc timezone từ application.yaml.
             */
            ZoneRules zoneRules =
                    zoneId().getRules();

            /*
             * Một local timestamp có thể có:
             *
             * - 1 offset: thời gian hợp lệ và không mơ hồ.
             * - 0 offset: rơi vào DST gap, thời gian không tồn tại.
             * - 2 offset: rơi vào DST overlap, thời gian bị mơ hồ.
             *
             * Asia/Ho_Chi_Minh hiện không có DST nhưng kiểm tra này làm
             * code đúng nếu sau này đổi sang timezone khác.
             */
            List<ZoneOffset> validOffsets =
                    zoneRules.getValidOffsets(
                            localDateTime
                    );

            /*
             * Chỉ chấp nhận timestamp ánh xạ được tới đúng một offset.
             */
            if (validOffsets.size() != 1) {
                throw invalidTimestamp(
                        "EVENT_TIME is ambiguous or does not exist "
                                + "in the configured timezone"
                );
            }

            /*
             * Chuyển LocalDateTime cùng offset vừa tìm được thành Instant.
             */
            Instant instant =
                    localDateTime.toInstant(
                            validOffsets.get(0)
                    );

            /*
             * Trả kết quả UTC ISO-8601.
             */
            return instant.toString();

        } catch (DateTimeParseException exception) {
            /*
             * Không đưa rawEventTime vào error message vì đó là dữ liệu
             * nguồn và không cần thiết cho việc phân loại lỗi.
             */
            throw invalidTimestamp(
                    "EVENT_TIME has invalid format"
            );
        }
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

        /*
         * sourceZoneId sẽ null khi:
         *
         * - Object vừa được khởi tạo.
         * - Object vừa được Flink deserialize trên TaskManager.
         */
        if (sourceZoneId == null) {
            sourceZoneId = ZoneId.of(
                    sourceTimezone
            );
        }

        return sourceZoneId;
    }
}