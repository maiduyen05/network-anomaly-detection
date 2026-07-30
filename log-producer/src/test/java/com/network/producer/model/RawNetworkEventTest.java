package com.network.producer.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test cơ bản cho RawNetworkEvent.
 * Mục tiêu: xác nhận model Java hoạt động đúng (chưa kết nối kafka)
 */
class RawNetworkEventTest {

    /**
     * Kiểm tra các giá trị truyền vào record
     * được lưu và đọc lại chính xác.
     */
    @Test
    void shouldStoreRawEventFields() {

        // Tạo một dòng log mẫu đã được rút gọn.
         String rawPayload =
                "l_service_request;success;125;0";

        String rawRecordId =
                "abctest1234567890";

        // Tạo object RawNetworkEvent dùng cho test.
        RawNetworkEvent event = new RawNetworkEvent(
                rawRecordId,
                "raw-envelope-v1",      // Phiên bản schema.
                "A20240626.1400+0700-"
                        + "20240626.1401+0700_840_ebs",
                1L,                     // Dòng đầu tiên trong file.
                rawPayload              // Nội dung raw.
        );

        // Kiểm tra eventId.
        assertEquals(
                "abctest1234567890",
                event.rawRecordId()
        );

        // Kiểm tra schema version.
        assertEquals(
                "raw-envelope-v1",
                event.schemaVersion()
        );

        // Kiểm tra tên file nguồn.
        assertEquals(
                "A20240626.1400+0700-"
                        + "20240626.1401+0700_840_ebs",
                event.sourceFile()
        );

        // Kiểm tra số dòng.
        assertEquals(
                1L,
                event.sourceLine()
        );

        // Kiểm tra rawData được giữ nguyên.
        assertEquals(
                rawPayload,
                event.rawPayload()
        );
    }
}