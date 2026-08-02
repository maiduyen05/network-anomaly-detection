package com.network.producer.kafka;

import com.network.producer.model.RawNetworkEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test cho KafkaMessageKeyResolver.
 */
class KafkaMessageKeyResolverTest {

    /**
     * Kiểm tra hai event có cùng IMSI tạo cùng Kafka key.
     */
    @Test
    void shouldUseStableHashedImsiAsKey() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent firstEvent =
                createEvent(
                        "id-1",
                        "event;success;10;0;normal;"
                                + "84900000001;"
                                + "452040000000001"
                );

        RawNetworkEvent secondEvent =
                createEvent(
                        "id-2",
                        "event;success;20;0;normal;"
                                + "84900000002;"
                                + "452040000000001"
                );

        String firstKey =
                resolver.resolve(firstEvent);

        String secondKey =
                resolver.resolve(secondEvent);

        // Cùng IMSI nên cùng key.
        assertEquals(
                firstKey,
                secondKey
        );

        // SHA-256 phải có 64 ký tự hexadecimal.
        assertTrue(
                firstKey.matches("^[a-f0-9]{64}$")
        );
    }

    /**
     * Kiểm tra IMSI khác nhau tạo key khác nhau.
     */
    @Test
    void shouldCreateDifferentKeysForDifferentImsi() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent firstEvent =
                createEvent(
                        "id-1",
                        "event;success;10;0;normal;"
                                + "84900000001;"
                                + "452040000000001"
                );

        RawNetworkEvent secondEvent =
                createEvent(
                        "id-2",
                        "event;success;10;0;normal;"
                                + "84900000001;"
                                + "452040000000002"
                );

        assertNotEquals(
                resolver.resolve(firstEvent),
                resolver.resolve(secondEvent)
        );
    }

    /**
     * Kiểm tra payload không đủ field sử dụng rawRecordId.
     */
    @Test
    void shouldFallbackToRawRecordId() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent event =
                createEvent(
                        "fallback-record-id",
                        "malformed"
                );

        assertEquals(
                "fallback-record-id",
                resolver.resolve(event)
        );
    }

    /**
     * Tạo RawNetworkEvent ngắn gọn cho test.
     */
    private static RawNetworkEvent createEvent(
            String rawRecordId,
            String rawPayload
    ) {
        return new RawNetworkEvent(
                rawRecordId,
                "raw-envelope-v1",
                "sample.log",
                1L,
                rawPayload
        );
    }
}