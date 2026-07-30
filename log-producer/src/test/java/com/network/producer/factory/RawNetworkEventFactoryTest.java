package com.network.producer.factory;

import com.network.producer.model.RawNetworkEvent;
import com.network.producer.model.SourceLine;
import com.network.producer.util.RawRecordIdGenerator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test cho RawNetworkEventFactory.
 *
 * <p>Mỗi test kiểm tra một quy tắc nhỏ của factory.
 * Các test này không đọc file và không kết nối Kafka.</p>
 */
class RawNetworkEventFactoryTest {

    /**
     * Kiểm tra một SourceLine hợp lệ được chuyển thành
     * RawNetworkEvent với đầy đủ field chính xác.
     */
    @Test
    void shouldCreateRawNetworkEventFromSourceLine() {

        // Chuẩn bị dữ liệu đầu vào giống dữ liệu từ FileLogReader.
        SourceLine sourceLine = new SourceLine(
                "A20240626.1400+0700-"
                        + "20240626.1401+0700_840_ebs",
                1L,
                "l_service_request;success;125;0"
        );

        // Tạo factory cần kiểm tra.
        RawNetworkEventFactory factory =
                new RawNetworkEventFactory();

        // Thực hiện chuyển đổi.
        RawNetworkEvent event =
                factory.create(sourceLine);

        /*
         * Tính ID mong đợi bằng chính công thức chuẩn.
         *
         * Mục tiêu là xác nhận factory đã truyền đúng
         * sourceFile, lineNumber và rawData vào generator.
         */
        String expectedRawRecordId =
                RawRecordIdGenerator.generate(
                        sourceLine.sourceFile(),
                        sourceLine.lineNumber(),
                        sourceLine.rawData()
                );

        // Kiểm tra ID.
        assertEquals(
                expectedRawRecordId,
                event.rawRecordId()
        );

        // Kiểm tra schema version.
        assertEquals(
                "raw-envelope-v1",
                event.schemaVersion()
        );

        // Kiểm tra tên file được giữ nguyên.
        assertEquals(
                sourceLine.sourceFile(),
                event.sourceFile()
        );

        // Kiểm tra số dòng được giữ nguyên.
        assertEquals(
                sourceLine.lineNumber(),
                event.sourceLine()
        );

        // Kiểm tra raw payload không bị chỉnh sửa.
        assertEquals(
                sourceLine.rawData(),
                event.rawPayload()
        );
    }

    /**
     * Kiểm tra cùng một SourceLine luôn tạo cùng rawRecordId.
     *
     * <p>Tính ổn định này rất quan trọng khi chạy lại
     * cùng một file hoặc replay dữ liệu.</p>
     */
    @Test
    void shouldCreateStableRawRecordId() {

        SourceLine sourceLine = new SourceLine(
                "sample.log",
                10L,
                "l_tau;success;661;0"
        );

        RawNetworkEventFactory factory =
                new RawNetworkEventFactory();

        RawNetworkEvent firstEvent =
                factory.create(sourceLine);

        RawNetworkEvent secondEvent =
                factory.create(sourceLine);

        assertEquals(
                firstEvent.rawRecordId(),
                secondEvent.rawRecordId()
        );
    }

    /**
     * Kiểm tra một dòng trống vẫn được tạo thành raw event.
     *
     * <p>Producer không quyết định dòng trống có hợp lệ
     * về nghiệp vụ hay không. Bronze Job sẽ thực hiện việc đó
     * và route message lỗi sang DLQ.</p>
     */
    @Test
    void shouldAllowEmptyRawData() {

        SourceLine sourceLine = new SourceLine(
                "sample.log",
                1L,
                ""
        );

        RawNetworkEventFactory factory =
                new RawNetworkEventFactory();

        RawNetworkEvent event =
                factory.create(sourceLine);

        assertEquals(
                "",
                event.rawPayload()
        );
    }

    /**
     * Kiểm tra sourceFile chỉ chứa khoảng trắng bị từ chối.
     */
    @Test
    void shouldRejectBlankSourceFile() {

        SourceLine sourceLine = new SourceLine(
                "   ",
                1L,
                "event;success"
        );

        RawNetworkEventFactory factory =
                new RawNetworkEventFactory();

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(sourceLine)
        );
    }

    /**
     * Kiểm tra số dòng bằng 0 bị từ chối.
     */
    @Test
    void shouldRejectInvalidLineNumber() {

        SourceLine sourceLine = new SourceLine(
                "sample.log",
                0L,
                "event;success"
        );

        RawNetworkEventFactory factory =
                new RawNetworkEventFactory();

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(sourceLine)
        );
    }

    /**
     * Kiểm tra rawData bằng null bị từ chối.
     *
     * <p>Chuỗi rỗng được phép, nhưng null không được phép.</p>
     */
    @Test
    void shouldRejectNullRawData() {

        SourceLine sourceLine = new SourceLine(
                "sample.log",
                1L,
                null
        );

        RawNetworkEventFactory factory =
                new RawNetworkEventFactory();

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.create(sourceLine)
        );
    }
}
