package com.network.preprocess.validation;

import com.network.preprocess.model.BronzeErrorCode;
import com.network.preprocess.model.RawNetworkEvent;
import com.network.preprocess.parser.BronzeDataException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kiểm tra contract của JSON raw envelope.
 */
class SchemaValidatorTest {

    private static final String EXPECTED_SCHEMA_VERSION =
            "raw-envelope-v1";

    private final SchemaValidator validator =
            new SchemaValidator();

    @Test
    void shouldAcceptValidEnvelope() {

        RawNetworkEvent event = validEvent();

        assertDoesNotThrow(
                () -> validator.validateOrThrow(
                        event,
                        EXPECTED_SCHEMA_VERSION
                )
        );
    }

    @Test
    void shouldRejectJsonNullLiteralResult() {

        BronzeDataException exception = assertThrows(
                BronzeDataException.class,
                () -> validator.validateOrThrow(
                        null,
                        EXPECTED_SCHEMA_VERSION
                )
        );

        assertEquals(
                BronzeErrorCode.INVALID_ENVELOPE_SCHEMA,
                exception.getErrorCode()
        );
    }

    @Test
    void shouldRejectMissingSchemaVersion() {

        RawNetworkEvent event = new RawNetworkEvent(
                null,
                "record-0001",
                "ue-log.csv",
                1L,
                "payload"
        );

        assertErrorCode(
                event,
                BronzeErrorCode.INVALID_ENVELOPE_SCHEMA
        );
    }

    @Test
    void shouldRejectBlankSchemaVersion() {

        RawNetworkEvent event = new RawNetworkEvent(
                "   ",
                "record-0001",
                "ue-log.csv",
                1L,
                "payload"
        );

        assertErrorCode(
                event,
                BronzeErrorCode.INVALID_ENVELOPE_SCHEMA
        );
    }

    @Test
    void shouldRejectUnsupportedSchemaVersion() {

        RawNetworkEvent event = new RawNetworkEvent(
                "raw-envelope-v2",
                "record-0001",
                "ue-log.csv",
                1L,
                "payload"
        );

        assertErrorCode(
                event,
                BronzeErrorCode
                        .UNSUPPORTED_ENVELOPE_SCHEMA_VERSION
        );
    }

    @Test
    void shouldRejectMissingRawRecordId() {

        RawNetworkEvent event = new RawNetworkEvent(
                EXPECTED_SCHEMA_VERSION,
                null,
                "ue-log.csv",
                1L,
                "payload"
        );

        assertErrorCode(
                event,
                BronzeErrorCode.INVALID_ENVELOPE_SCHEMA
        );
    }

    @Test
    void shouldRejectMissingSourceFile() {

        RawNetworkEvent event = new RawNetworkEvent(
                EXPECTED_SCHEMA_VERSION,
                "record-0001",
                null,
                1L,
                "payload"
        );

        assertErrorCode(
                event,
                BronzeErrorCode.INVALID_ENVELOPE_SCHEMA
        );
    }

    @Test
    void shouldRejectMissingSourceLine() {

        RawNetworkEvent event = new RawNetworkEvent(
                EXPECTED_SCHEMA_VERSION,
                "record-0001",
                "ue-log.csv",
                null,
                "payload"
        );

        assertErrorCode(
                event,
                BronzeErrorCode.INVALID_ENVELOPE_SCHEMA
        );
    }

    @Test
    void shouldRejectZeroSourceLine() {

        RawNetworkEvent event = new RawNetworkEvent(
                EXPECTED_SCHEMA_VERSION,
                "record-0001",
                "ue-log.csv",
                0L,
                "payload"
        );

        assertErrorCode(
                event,
                BronzeErrorCode.INVALID_ENVELOPE_SCHEMA
        );
    }

    @Test
    void shouldAllowNullRawPayloadAtSchemaStage() {

        /*
         * SchemaValidator không phân loại raw_payload rỗng.
         *
         * RawLogLineParser sẽ phân loại nó thành EMPTY_RAW_PAYLOAD.
         */
        RawNetworkEvent event = new RawNetworkEvent(
                EXPECTED_SCHEMA_VERSION,
                "record-0001",
                "ue-log.csv",
                1L,
                null
        );

        assertDoesNotThrow(
                () -> validator.validateOrThrow(
                        event,
                        EXPECTED_SCHEMA_VERSION
                )
        );
    }

    @Test
    void shouldFailFastWhenExpectedSchemaConfigurationIsBlank() {

        /*
         * expectedSchemaVersion rỗng là lỗi application.yaml,
         * không phải lỗi của Kafka record.
         */
        assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateOrThrow(
                        validEvent(),
                        " "
                )
        );
    }

    private void assertErrorCode(
            RawNetworkEvent event,
            BronzeErrorCode expectedErrorCode
    ) {
        BronzeDataException exception = assertThrows(
                BronzeDataException.class,
                () -> validator.validateOrThrow(
                        event,
                        EXPECTED_SCHEMA_VERSION
                )
        );

        assertEquals(
                expectedErrorCode,
                exception.getErrorCode()
        );
    }

    private RawNetworkEvent validEvent() {
        return new RawNetworkEvent(
                EXPECTED_SCHEMA_VERSION,
                "record-0001",
                "ue-log.csv",
                1L,
                "payload"
        );
    }
}