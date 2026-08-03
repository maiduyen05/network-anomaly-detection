package com.network.preprocess.parser;

import com.network.preprocess.model.BronzeErrorCode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm tra exception giữ nguyên error code và safe message.
 */
class BronzeDataExceptionTest {

    @Test
    void shouldPreserveErrorCodeAndSafeMessage() {

        BronzeDataException exception =
                new BronzeDataException(
                        BronzeErrorCode.INVALID_DURATION,
                        "Numeric value must not be negative"
                );

        assertEquals(
                BronzeErrorCode.INVALID_DURATION,
                exception.getErrorCode()
        );

        assertEquals(
                "Numeric value must not be negative",
                exception.getMessage()
        );
    }

    @Test
    void shouldRejectNullErrorCode() {

        assertThrows(
                NullPointerException.class,
                () -> new BronzeDataException(
                        null,
                        "Safe error message"
                )
        );
    }

    @Test
    void safeMessageShouldNotContainRawIdentity() {

        /*
         * Đây là test phòng ngừa việc vô tình đưa identity hoặc raw payload
         * vào message sẽ được ghi sang DLQ.
         */
        String rawIdentity = "452010123456789";

        BronzeDataException exception =
                new BronzeDataException(
                        BronzeErrorCode.INVALID_DURATION,
                        "Numeric value has invalid Long format"
                );

        assertFalse(
                exception.getMessage().contains(rawIdentity)
        );
    }
}