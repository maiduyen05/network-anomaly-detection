package com.network.producer.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test cho EventIdGenerator.
 */
class RawRecordIdGeneratorTest {

    /**
     * Kiểm tra cùng một input luôn tạo cùng một event ID.
     */
    @Test
    void shouldGenerateSameIdForSameInput() {

        // Chuẩn bị dữ liệu nguồn.
        String sourceFile =
                "A20240626.1400+0700-"
                        + "20240626.1401+0700_840_ebs";

        long lineNumber = 1L;

        String rawData =
                "l_service_request;success;125;0";

        // Tạo event ID lần thứ nhất.
        String firstId = RawRecordIdGenerator.generate(
                sourceFile,
                lineNumber,
                rawData
        );

        // Tạo event ID lần thứ hai với đúng cùng input.
        String secondId = RawRecordIdGenerator.generate(
                sourceFile,
                lineNumber,
                rawData
        );

        // Hai ID phải hoàn toàn giống nhau.
        assertEquals(firstId, secondId);
    }

    /**
     * Kiểm tra event ID có đúng định dạng SHA-256.
     */
    @Test
    void shouldGenerateLowercaseSha256HexString() {

        // Tạo một event ID mẫu.
        String eventId = RawRecordIdGenerator.generate(
                "sample.log",
                1L,
                "event;success"
        );

        // SHA-256 dạng hexadecimal phải có đúng 64 ký tự.
        assertEquals(64, eventId.length());

        /*
         * Chỉ chấp nhận:
         * - chữ cái a đến f
         * - chữ số 0 đến 9
         */
        assertTrue(
                eventId.matches("^[a-f0-9]{64}$")
        );
    }

    /**
     * Kiểm tra đổi số dòng sẽ tạo event ID khác.
     */
    @Test
    void shouldGenerateDifferentIdWhenLineNumberChanges() {

        String firstId = RawRecordIdGenerator.generate(
                "sample.log",
                1L,
                "event;success"
        );

        String secondId = RawRecordIdGenerator.generate(
                "sample.log",
                2L,
                "event;success"
        );

        assertNotEquals(firstId, secondId);
    }

    /**
     * Kiểm tra đổi rawData sẽ tạo event ID khác.
     */
    @Test
    void shouldGenerateDifferentIdWhenRawDataChanges() {

        String firstId = RawRecordIdGenerator.generate(
                "sample.log",
                1L,
                "event;success"
        );

        String secondId = RawRecordIdGenerator.generate(
                "sample.log",
                1L,
                "event;failure"
        );

        assertNotEquals(firstId, secondId);
    }

    /**
     * Kiểm tra lineNumber bằng 0 bị từ chối.
     */
    @Test
    void shouldRejectInvalidLineNumber() {

        assertThrows(
                IllegalArgumentException.class,
                () -> RawRecordIdGenerator.generate(
                        "sample.log",
                        0L,
                        "event;success"
                )
        );
    }
}