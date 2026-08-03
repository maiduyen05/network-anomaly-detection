package com.network.preprocess.operator;

import com.network.preprocess.model.BronzeErrorCode;
import com.network.preprocess.parser.BronzeDataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm tra chính sách ép kiểu numeric của Bronze.
 */
class TypeCastOperatorTest {

    private final TypeCastOperator operator =
            new TypeCastOperator();

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "\t"})
    void shouldReturnNullForBlankNullableLong(
            String rawValue
    ) throws Exception {

        Long result =
                operator.parseNullableNonNegativeLong(
                        rawValue,
                        BronzeErrorCode.INVALID_DURATION
                );

        assertNull(result);
    }

    @Test
    void shouldParseNonNegativeLong() throws Exception {

        assertEquals(
                0L,
                operator.parseNullableNonNegativeLong(
                        "0",
                        BronzeErrorCode.INVALID_DURATION
                )
        );

        assertEquals(
                1500L,
                operator.parseNullableNonNegativeLong(
                        " 1500 ",
                        BronzeErrorCode.INVALID_DURATION
                )
        );

        assertEquals(
                Long.MAX_VALUE,
                operator.parseNullableNonNegativeLong(
                        String.valueOf(Long.MAX_VALUE),
                        BronzeErrorCode.INVALID_DURATION
                )
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "-1",
            "1.5",
            "abc",
            "10ms",
            "9223372036854775808"
    })
    void shouldRejectInvalidLong(
            String rawValue
    ) {
        assertNumericError(
                () -> operator.parseNullableNonNegativeLong(
                        rawValue,
                        BronzeErrorCode.INVALID_DURATION
                ),
                BronzeErrorCode.INVALID_DURATION
        );
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "\t"})
    void shouldReturnNullForBlankNullableInteger(
            String rawValue
    ) throws Exception {

        Integer result =
                operator.parseNullableNonNegativeInteger(
                        rawValue,
                        BronzeErrorCode.INVALID_REQUEST_RETRIES
                );

        assertNull(result);
    }

    @Test
    void shouldParseNonNegativeInteger() throws Exception {

        assertEquals(
                0,
                operator.parseNullableNonNegativeInteger(
                        "0",
                        BronzeErrorCode.INVALID_REQUEST_RETRIES
                )
        );

        assertEquals(
                2,
                operator.parseNullableNonNegativeInteger(
                        " 2 ",
                        BronzeErrorCode.INVALID_REQUEST_RETRIES
                )
        );

        assertEquals(
                Integer.MAX_VALUE,
                operator.parseNullableNonNegativeInteger(
                        String.valueOf(Integer.MAX_VALUE),
                        BronzeErrorCode.INVALID_REQUEST_RETRIES
                )
        );
    }

    @Test
    void shouldKeepRequestRetriesErrorCode() {

        assertNumericError(
                () -> operator.parseNullableNonNegativeInteger(
                        "-1",
                        BronzeErrorCode.INVALID_REQUEST_RETRIES
                ),
                BronzeErrorCode.INVALID_REQUEST_RETRIES
        );
    }

    @Test
    void shouldKeepPagingAttemptsErrorCode() {

        assertNumericError(
                () -> operator.parseNullableNonNegativeInteger(
                        "abc",
                        BronzeErrorCode.INVALID_PAGING_ATTEMPTS
                ),
                BronzeErrorCode.INVALID_PAGING_ATTEMPTS
        );
    }

    @Test
    void shouldRejectIntegerOverflow() {

        assertNumericError(
                () -> operator.parseNullableNonNegativeInteger(
                        "2147483648",
                        BronzeErrorCode.INVALID_REQUEST_RETRIES
                ),
                BronzeErrorCode.INVALID_REQUEST_RETRIES
        );
    }

    @Test
    void shouldRejectNullErrorCode() {

        /*
         * errorCode null là lỗi lập trình, không phải dữ liệu DLQ.
         */
        assertThrows(
                NullPointerException.class,
                () -> operator.parseNullableNonNegativeLong(
                        "10",
                        null
                )
        );
    }

    private void assertNumericError(
            ThrowingOperation operation,
            BronzeErrorCode expectedErrorCode
    ) {
        BronzeDataException exception = assertThrows(
                BronzeDataException.class,
                operation::execute
        );

        assertEquals(
                expectedErrorCode,
                exception.getErrorCode()
        );
    }

    /**
     * Cho phép helper nhận lambda có thể ném checked exception.
     */
    @FunctionalInterface
    private interface ThrowingOperation {

        void execute() throws BronzeDataException;
    }
}