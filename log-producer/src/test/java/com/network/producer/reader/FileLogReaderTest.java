package com.network.producer.reader;

import com.network.producer.model.SourceLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit test cho FileLogReader.
 */
class FileLogReaderTest {

    /**
     * JUnit tự tạo một thư mục tạm cho mỗi lần chạy test (được dọn sau khi test kết thúc)
     */
    @TempDir
    Path tempDirectory;

    /**
     * Kiểm tra reader đọc đúng ba dòng theo đúng thứ tự.
     */
    @Test
    void shouldReadFileLineByLine() throws IOException {

        // Tạo đường dẫn file test trong thư mục tạm.
        Path inputFile = tempDirectory.resolve(
                "A20240626.1400+0700-"
                        + "20240626.1401+0700_840_ebs"
        );

        // Chuẩn bị ba dòng dữ liệu giả.
        List<String> inputLines = List.of(
                "l_service_request;success;125;0",
                "l_bearer_modify;success;24;0",
                "l_tau;success;661;0"
        );

        // Ghi ba dòng vào file tạm bằng UTF-8.
        Files.write(
                inputFile,
                inputLines,
                StandardCharsets.UTF_8
        );

        // Danh sách dùng để thu lại các dòng reader phát ra.
        List<SourceLine> actualLines =
                new ArrayList<>();

        // Tạo FileLogReader cần kiểm tra.
        FileLogReader reader = new FileLogReader();

        /*
         * actualLines::add tương đương:
         *
         * sourceLine -> actualLines.add(sourceLine)
         */
        long lineCount = reader.read(
                inputFile,
                actualLines::add
        );

        // Reader phải báo đã đọc ba dòng.
        assertEquals(3L, lineCount);

        // Danh sách kết quả phải có ba object.
        assertEquals(3, actualLines.size());

        // Kiểm tra dòng đầu tiên.
        assertEquals(
                inputFile.getFileName().toString(),
                actualLines.get(0).sourceFile()
        );

        assertEquals(
                1L,
                actualLines.get(0).lineNumber()
        );

        assertEquals(
                inputLines.get(0),
                actualLines.get(0).rawData()
        );

        // Kiểm tra dòng thứ hai.
        assertEquals(
                2L,
                actualLines.get(1).lineNumber()
        );

        assertEquals(
                inputLines.get(1),
                actualLines.get(1).rawData()
        );

        // Kiểm tra dòng thứ ba.
        assertEquals(
                3L,
                actualLines.get(2).lineNumber()
        );

        assertEquals(
                inputLines.get(2),
                actualLines.get(2).rawData()
        );
    }

    /**
     * Kiểm tra file rỗng trả về 0 dòng.
     */
    @Test
    void shouldReturnZeroForEmptyFile() throws IOException {

        // Tạo một file rỗng.
        Path emptyFile = tempDirectory.resolve(
                "empty.log"
        );

        Files.createFile(emptyFile);

        // Tạo reader.
        FileLogReader reader = new FileLogReader();

        // Danh sách nhận dữ liệu.
        List<SourceLine> actualLines =
                new ArrayList<>();

        // Đọc file rỗng.
        long lineCount = reader.read(
                emptyFile,
                actualLines::add
        );

        // Không có dòng nào được đọc.
        assertEquals(0L, lineCount);

        // Consumer không nhận object nào.
        assertEquals(0, actualLines.size());
    }

    /**
     * Kiểm tra đường dẫn không tồn tại bị từ chối.
     */
    @Test
    void shouldRejectMissingFile() {

        // Tạo đường dẫn nhưng không tạo file thật.
        Path missingFile = tempDirectory.resolve(
                "missing.log"
        );

        // Tạo reader.
        FileLogReader reader = new FileLogReader();

        // Phải phát sinh IllegalArgumentException.
        assertThrows(
                IllegalArgumentException.class,
                () -> reader.read(
                        missingFile,
                        sourceLine -> {
                            // Không cần xử lý trong test này.
                        }
                )
        );
    }
}