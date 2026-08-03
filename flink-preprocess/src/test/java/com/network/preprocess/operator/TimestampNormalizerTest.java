package com.network.preprocess.operator;

import com.network.preprocess.model.BronzeErrorCode;
import com.network.preprocess.parser.BronzeDataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm tra EVENT_TIME được chuẩn hóa về UTC.
 */
class TimestampNormalizerTest {

    private final TimestampNormalizer normalizer =
            new TimestampNormalizer(
                    "Asia/Ho_Chi_Minh"
            );

    @Test
    void shouldConvertVietnamLocalTimeToUtc() throws Exception {

        String result = normalizer.normalizeRequiredToUtc(
                "2026-08-03 13:15:30"
        );

        assertEquals(
                "2026-08-03T06:15:30Z",
                result
        );
    }

    @Test
    void shouldPreserveFractionalSeconds() throws Exception {

        String result = normalizer.normalizeRequiredToUtc(
                "2026-08-03 13:15:30.123"
        );

        assertEquals(
                "2026-08-03T06:15:30.123Z",
                result
        );
    }

    @Test
    void shouldConvertTimestampThatAlreadyContainsOffset()
            throws Exception {

        String result = normalizer.normalizeRequiredToUtc(
                "2026-08-03T13:15:30+07:00"
        );

        assertEquals(
                "2026-08-03T06:15:30Z",
                result
        );
    }

    @Test
    void shouldTrimSurroundingSpaces() throws Exception {

        String result = normalizer.normalizeRequiredToUtc(
                "  2026-08-03 13:15:30  "
        );

        assertEquals(
                "2026-08-03T06:15:30Z",
                result
        );
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {
            "",
            "   ",
            "2026-99-99 10:00:00",
            "2026-02-30 10:00:00",
            "03/08/2026 10:00",
            "not-a-timestamp"
    })
    void shouldRejectMissingOrInvalidTimestamp(
            String rawTimestamp
    ) {
        BronzeDataException exception = assertThrows(
                BronzeDataException.class,
                () -> normalizer.normalizeRequiredToUtc(
                        rawTimestamp
                )
        );

        assertEquals(
                BronzeErrorCode.INVALID_EVENT_TIME,
                exception.getErrorCode()
        );
    }

    @Test
    void shouldRejectDstGap() {

        /*
         * 02:30 ngày DST bắt đầu không tồn tại tại New York.
         */
        TimestampNormalizer newYorkNormalizer =
                new TimestampNormalizer(
                        "America/New_York"
                );

        BronzeDataException exception = assertThrows(
                BronzeDataException.class,
                () -> newYorkNormalizer.normalizeRequiredToUtc(
                        "2026-03-08 02:30:00"
                )
        );

        assertEquals(
                BronzeErrorCode.INVALID_EVENT_TIME,
                exception.getErrorCode()
        );
    }

    @Test
    void shouldRejectDstOverlap() {

        /*
         * 01:30 ngày DST kết thúc xảy ra hai lần tại New York,
         * vì vậy timestamp local này bị mơ hồ.
         */
        TimestampNormalizer newYorkNormalizer =
                new TimestampNormalizer(
                        "America/New_York"
                );

        BronzeDataException exception = assertThrows(
                BronzeDataException.class,
                () -> newYorkNormalizer.normalizeRequiredToUtc(
                        "2026-11-01 01:30:00"
                )
        );

        assertEquals(
                BronzeErrorCode.INVALID_EVENT_TIME,
                exception.getErrorCode()
        );
    }

    @Test
    void shouldFailFastForInvalidConfiguredTimezone() {

        /*
         * Timezone sai là lỗi application.yaml.
         */
        assertThrows(
                IllegalArgumentException.class,
                () -> new TimestampNormalizer(
                        "Invalid/Timezone"
                )
        );
    }
}