package com.network.preprocess.parser;

import com.network.preprocess.model.BronzeErrorCode;
import com.network.preprocess.support.RawPayloadTestData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm tra raw payload được tách đúng 52 field.
 */
class RawLogLineParserTest {

    private final RawLogLineParser parser =
            new RawLogLineParser(";", 52);

    @Test
    void shouldParseExactlyFiftyTwoFields() throws Exception {

        Map<String, String> fields = parser.parse(
                RawPayloadTestData.validPayload()
        );

        assertEquals(52, fields.size());

        assertEquals(
                "l_service_request",
                fields.get("EVENT_ID")
        );

        assertEquals(
                "success",
                fields.get("EVENT_RESULT")
        );

        assertEquals(
                "1500",
                fields.get("DURATION")
        );

        assertEquals(
                "2026-08-03 13:15:30",
                fields.get("EVENT_TIME")
        );

        assertEquals(
                "1",
                fields.get("PAGING_ATTEMPTS")
        );
    }

    @Test
    void shouldMapAllFieldsUsingCorrectOrder() throws Exception {

        Map<String, String> fields = parser.parse(
                RawPayloadTestData.validPayload()
        );

        /*
         * Parser nên trả LinkedHashMap để thứ tự field được giữ ổn định.
         */
        assertEquals(
                RawPayloadTestData.RAW_FIELD_NAMES,
                new ArrayList<>(fields.keySet())
        );
    }

    @Test
    void shouldPreserveTrailingEmptyField() throws Exception {

        /*
         * Đặt DATE_HOUR, field cuối cùng, thành rỗng.
         *
         * Parser phải split bằng giới hạn -1 để không làm mất field cuối.
         */
        String rawPayload =
                RawPayloadTestData.validPayloadWith(
                        "DATE_HOUR",
                        ""
                );

        assertTrue(rawPayload.endsWith(";"));

        Map<String, String> fields =
                parser.parse(rawPayload);

        assertEquals(52, fields.size());
        assertEquals("", fields.get("DATE_HOUR"));
    }

    @Test
    void shouldRejectNullPayload() {

        assertErrorCode(
                null,
                BronzeErrorCode.EMPTY_RAW_PAYLOAD
        );
    }

    @Test
    void shouldRejectBlankPayload() {

        assertErrorCode(
                "   ",
                BronzeErrorCode.EMPTY_RAW_PAYLOAD
        );
    }

    @Test
    void shouldRejectPayloadWhereAllFieldsAreEmpty() {

        /*
         * Dòng này có đủ delimiter nhưng toàn bộ 52 field đều rỗng.
         */
        String rawPayload = String.join(
                ";",
                Collections.nCopies(52, "")
        );

        assertErrorCode(
                rawPayload,
                BronzeErrorCode.EMPTY_RAW_PAYLOAD
        );
    }

    @Test
    void shouldRejectPayloadWithOnlyFiftyOneFields() {

        List<String> values = new ArrayList<>(
                Collections.nCopies(51, "")
        );

        /*
         * Ít nhất một field không rỗng để lỗi được phân loại là sai
         * số field, không phải toàn bộ payload rỗng.
         */
        values.set(0, "l_service_request");

        String rawPayload = String.join(";", values);

        assertErrorCode(
                rawPayload,
                BronzeErrorCode.WRONG_RAW_FIELD_COUNT
        );
    }

    @Test
    void shouldRejectPayloadWithFiftyThreeFields() {

        List<String> values = new ArrayList<>(
                Collections.nCopies(53, "")
        );

        values.set(0, "l_service_request");

        String rawPayload = String.join(";", values);

        assertErrorCode(
                rawPayload,
                BronzeErrorCode.WRONG_RAW_FIELD_COUNT
        );
    }

    private void assertErrorCode(
            String rawPayload,
            BronzeErrorCode expectedErrorCode
    ) {
        BronzeDataException exception = assertThrows(
                BronzeDataException.class,
                () -> parser.parse(rawPayload)
        );

        assertEquals(
                expectedErrorCode,
                exception.getErrorCode()
        );
    }
}