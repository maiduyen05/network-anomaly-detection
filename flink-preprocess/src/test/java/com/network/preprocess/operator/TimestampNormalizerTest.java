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

    /**
     * Format thực tế của dataset:
     *
     * 1719385235407
     *
     * = 2024-06-26 14:00:35.407 +07:00
     * = 2024-06-26T07:00:35.407Z
     */
    @Test
    void shouldConvertEpochMillisecondsToUtc()
            throws Exception {

        String result =
                normalizer.normalizeRequiredToUtc(
                        "1719385235407"
                );

        assertEquals(
                "2024-06-26T07:00:35.407Z",
                result
        );
    }

    @Test
    void shouldTrimEpochMilliseconds()
            throws Exception {

        String result =
                normalizer.normalizeRequiredToUtc(
                        "  1719385235407  "
                );

        assertEquals(
                "2024-06-26T07:00:35.407Z",
                result
        );
    }

    /**
     * Không chấp nhận epoch seconds 10 chữ số.
     *
     * Contract dataset hiện tại là epoch milliseconds 13 chữ số.
     */
    @Test
    void shouldRejectEpochSeconds() {

        BronzeDataException exception =
                assertThrows(
                        BronzeDataException.class,
                        () -> normalizer
                                .normalizeRequiredToUtc(
                                        "1719385235"
                                )
                );

        assertEquals(
                BronzeErrorCode.INVALID_EVENT_TIME,
                exception.getErrorCode()
        );
    }

    @Test
    void shouldConvertVietnamLocalTimeToUtc()
            throws Exception {

        String result =
                normalizer.normalizeRequiredToUtc(
                        "2026-08-03 13:15:30"
                );

        assertEquals(
                "2026-08-03T06:15:30Z",
                result
        );
    }

    @Test
    void shouldPreserveFractionalSeconds()
            throws Exception {

        String result =
                normalizer.normalizeRequiredToUtc(
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

        String result =
                normalizer.normalizeRequiredToUtc(
                        "2026-08-03T13:15:30+07:00"
                );

        assertEquals(
                "2026-08-03T06:15:30Z",
                result
        );
    }

    @Test
    void shouldTrimSurroundingSpaces()
            throws Exception {

        String result =
                normalizer.normalizeRequiredToUtc(
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
            "not-a-timestamp",
            "171938523540X"
    })
    void shouldRejectMissingOrInvalidTimestamp(
            String rawTimestamp
    ) {

        BronzeDataException exception =
                assertThrows(
                        BronzeDataException.class,
                        () -> normalizer
                                .normalizeRequiredToUtc(
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

        TimestampNormalizer newYorkNormalizer =
                new TimestampNormalizer(
                        "America/New_York"
                );

        BronzeDataException exception =
                assertThrows(
                        BronzeDataException.class,
                        () -> newYorkNormalizer
                                .normalizeRequiredToUtc(
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

        TimestampNormalizer newYorkNormalizer =
                new TimestampNormalizer(
                        "America/New_York"
                );

        BronzeDataException exception =
                assertThrows(
                        BronzeDataException.class,
                        () -> newYorkNormalizer
                                .normalizeRequiredToUtc(
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

        assertThrows(
                IllegalArgumentException.class,
                () -> new TimestampNormalizer(
                        "Invalid/Timezone"
                )
        );
    }
}