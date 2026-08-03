package com.network.preprocess.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.network.preprocess.model.RawNetworkEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm tra việc chuyển JSON Kafka value thành RawNetworkEvent.
 */
class JsonEventParserTest {

    private final JsonEventParser parser =
            new JsonEventParser();

    @Test
    void shouldParseValidRawEnvelopeJson() throws Exception {

        String json = """
                {
                  "schema_version": "raw-envelope-v1",
                  "raw_record_id": "record-0001",
                  "source_file": "ue-log.csv",
                  "source_line": 125,
                  "raw_payload": "event;result;payload"
                }
                """;

        RawNetworkEvent event = parser.parse(json);

        assertNotNull(event);

        /*
         * Kiểm tra @JsonProperty đã ánh xạ đúng từ snake_case
         * sang accessor camelCase của Java record.
         */
        assertEquals(
                "raw-envelope-v1",
                event.schemaVersion()
        );

        assertEquals(
                "record-0001",
                event.rawRecordId()
        );

        assertEquals(
                "ue-log.csv",
                event.sourceFile()
        );

        assertEquals(
                125L,
                event.sourceLine()
        );

        assertEquals(
                "event;result;payload",
                event.rawPayload()
        );
    }

    @Test
    void shouldIgnoreUnknownEnvelopeFields() throws Exception {

        String json = """
                {
                  "schema_version": "raw-envelope-v1",
                  "raw_record_id": "record-0001",
                  "source_file": "ue-log.csv",
                  "source_line": 125,
                  "raw_payload": "payload",
                  "future_field": "producer-added-this-field"
                }
                """;

        /*
         * RawNetworkEvent có @JsonIgnoreProperties(ignoreUnknown = true),
         * vì vậy field mới không được làm parser thất bại.
         */
        RawNetworkEvent event = parser.parse(json);

        assertNotNull(event);
        assertEquals("record-0001", event.rawRecordId());
    }

    @Test
    void shouldThrowJsonProcessingExceptionForMalformedJson() {

        /*
         * JSON bị thiếu dấu đóng }.
         */
        String malformedJson = """
                {
                  "schema_version": "raw-envelope-v1"
                """;

        assertThrows(
                JsonProcessingException.class,
                () -> parser.parse(malformedJson)
        );
    }

    @Test
    void shouldReturnNullForJsonNullLiteral() throws Exception {

        /*
         * Chuỗi "null" là JSON đúng cú pháp.
         *
         * Parser trả null. SchemaValidator sẽ chịu trách nhiệm
         * chuyển trường hợp này thành INVALID_ENVELOPE_SCHEMA.
         */
        RawNetworkEvent event = parser.parse("null");

        assertNull(event);
    }

    @Test
    void shouldRejectJavaNullArgument() {

        /*
         * Java null biểu thị gọi sai API, không phải Kafka JSON "null".
         */
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> parser.parse(null)
        );

        assertEquals(
                "json must not be null",
                exception.getMessage()
        );
    }
}