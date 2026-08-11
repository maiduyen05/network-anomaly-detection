package com.network.producer.kafka;

import com.network.producer.model.RawNetworkEvent;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test cho KafkaMessageKeyResolver.
 *
 * <p>
 * Contract hiện tại của log-producer:
 * </p>
 *
 * <pre>
 * RawNetworkEvent
 *      ↓
 * Kafka key = rawRecordId
 * </pre>
 *
 * <p>
 * Producer KHÔNG parse IMSI, MSISDN hoặc các field nghiệp vụ
 * trong rawPayload để tạo Kafka key.
 * </p>
 *
 * <p>
 * Việc parse raw log và resolve UE identity thuộc:
 * </p>
 *
 * <pre>
 * Bronze
 *   ↓
 * Silver
 * </pre>
 */
class KafkaMessageKeyResolverTest {

    /**
     * rawRecordId phải được dùng trực tiếp làm Kafka key.
     */
    @Test
    void shouldUseRawRecordIdAsKafkaKey() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent event =
                createEvent(
                        "record-id-001",
                        "event;success;10;0"
                );

        assertEquals(
                "record-id-001",
                resolver.resolve(event)
        );
    }


    /**
     * Hai raw record khác nhau phải giữ key khác nhau,
     * ngay cả khi raw payload chứa cùng IMSI.
     *
     * <p>
     * Đây là contract quan trọng:
     * producer không đọc IMSI từ payload.
     * </p>
     */
    @Test
    void shouldKeepDifferentRawRecordIdsAsDifferentKeys() {

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

        assertNotEquals(
                resolver.resolve(firstEvent),
                resolver.resolve(secondEvent)
        );

        assertEquals(
                "id-1",
                resolver.resolve(firstEvent)
        );

        assertEquals(
                "id-2",
                resolver.resolve(secondEvent)
        );
    }


    /**
     * Nội dung raw payload không được ảnh hưởng tới Kafka key.
     *
     * <p>
     * Cùng rawRecordId thì resolver phải trả cùng key,
     * dù payload khác nhau.
     * </p>
     */
    @Test
    void shouldNotDeriveKafkaKeyFromRawPayload() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent firstEvent =
                createEvent(
                        "stable-record-id",
                        "payload-a"
                );

        RawNetworkEvent secondEvent =
                createEvent(
                        "stable-record-id",
                        "completely-different-payload"
                );

        assertEquals(
                resolver.resolve(firstEvent),
                resolver.resolve(secondEvent)
        );

        assertEquals(
                "stable-record-id",
                resolver.resolve(firstEvent)
        );
    }


    /**
     * Khoảng trắng ngoài rawRecordId được loại bỏ.
     */
    @Test
    void shouldTrimRawRecordId() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent event =
                createEvent(
                        "   record-id-001   ",
                        "payload"
                );

        assertEquals(
                "record-id-001",
                resolver.resolve(event)
        );
    }


    /**
     * rawRecordId rỗng không thể dùng làm Kafka key.
     */
    @Test
    void shouldRejectBlankRawRecordId() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent event =
                createEvent(
                        "   ",
                        "payload"
                );

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> resolver.resolve(event)
                );

        assertEquals(
                "rawRecordId must not be blank",
                exception.getMessage()
        );
    }


    /**
     * Event null là lỗi lập trình.
     */
    @Test
    void shouldRejectNullEvent() {

        KafkaMessageKeyResolver resolver =
                new KafkaMessageKeyResolver();

        assertThrows(
                NullPointerException.class,
                () -> resolver.resolve(null)
        );
    }


    /**
     * Helper tạo RawNetworkEvent ngắn gọn cho unit test.
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