package com.network.preprocess.source;

import com.network.preprocess.model.BronzeEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonKafkaValueDeserializationSchemaTest {

    private final JsonKafkaValueDeserializationSchema<BronzeEvent>
            deserializer =
            new JsonKafkaValueDeserializationSchema<>(
                    BronzeEvent.class
            );

    @Test
    void shouldDeserializeSnakeCaseJsonIntoBronzeEvent()
            throws Exception {

        String json = """
                {
                  "schema_version": "bronze-v1",
                  "raw_record_id": "raw-record-100",
                  "event_id": "l_service_request",
                  "event_result": "success",
                  "duration_ms": 120,
                  "request_retries": 0,
                  "paging_attempts": 1,
                  "event_time": "2026-07-08T10:15:30Z",
                  "event_time_quality": "SOURCE",
                  "msisdn": "84900000001",
                  "imsi": "452040000000001",
                  "mtmsi": "0x1234",
                  "imeisv": "3567890123456701",
                  "mmegi": "10",
                  "mmec": "01",
                  "sub_cause_code": null,
                  "msc": null,
                  "tac": "1001",
                  "eci": "20001",
                  "sgw": "SGW01",
                  "pdn_pgw": null,
                  "raw_fields": {
                    "SUB_TYPE": "0",
                    "REPORT_SIDE": "left",
                    "SGSN": "SGSN01"
                  },
                  "source": {
                    "source_file": "test-log-file",
                    "source_line": 101,
                    "topic": "raw.ue.log.line",
                    "partition": 0,
                    "offset": 100,
                    "kafka_timestamp": "2026-07-08T10:15:30Z",
                    "ingest_time": "2026-07-08T10:15:31Z"
                  }
                }
                """;

        BronzeEvent event =
                deserializer.deserialize(
                        json.getBytes(
                                StandardCharsets.UTF_8
                        )
                );

        assertEquals(
                "raw-record-100",
                event.rawRecordId()
        );

        assertEquals(
                "452040000000001",
                event.imsi()
        );

        assertEquals(
                120L,
                event.durationMs()
        );

        assertEquals(
                100L,
                event.source().offset()
        );

        assertEquals(
                "0",
                event.rawFields().get("SUB_TYPE")
        );

        /*
         * BronzeEvent compact constructor phải tạo mutable map.
         * Đây là regression check cho lỗi Kryo trước đây.
         */
        event.rawFields().put(
                "TEST_FIELD",
                "test-value"
        );

        assertEquals(
                "test-value",
                event.rawFields().get("TEST_FIELD")
        );
    }

    @Test
    void shouldRejectUnknownJsonField()
            throws Exception {

        String json = """
                {
                  "schema_version": "bronze-v1",
                  "raw_record_id": "raw-record-100",
                  "unknown_contract_field": "unexpected"
                }
                """;

        assertThrows(
                Exception.class,
                () -> deserializer.deserialize(
                        json.getBytes(
                                StandardCharsets.UTF_8
                        )
                )
        );
    }

    @Test
    void shouldRejectEmptyKafkaValue() {
        assertThrows(
                Exception.class,
                () -> deserializer.deserialize(
                        new byte[0]
                )
        );
    }
}