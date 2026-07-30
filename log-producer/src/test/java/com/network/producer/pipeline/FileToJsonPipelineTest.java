package com.network.producer.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.network.producer.factory.RawNetworkEventFactory;
import com.network.producer.model.RawNetworkEvent;
import com.network.producer.reader.FileLogReader;
import com.network.producer.serialization.RawNetworkEventJsonSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
Kiểm tra toàn bộ pipeline nội bộ từ file nguồn đến JSON
File -> FileLogReader -> SourceLine -> RawNetworkEventFactory -> RawNetworkEvent -> RawNetworkEventJsonSerializer -> JSON
Chú ý: Test này chưa khởi động Kafka và chưa gửi network request.</p>
 */
class FileToJsonPipelineTest {

    /**
    JUnit tạo thư mục tạm riêng cho test.
    File test không được ghi vào data/raw/incoming và sẽ được JUnit dọn sau khi test kết thúc.</p>
     */
    @TempDir
    Path tempDirectory;

    /**
     * Kiểm tra một file ba dòng tạo đúng ba JSON message.
     */
    @Test
    void shouldConvertEachFileLineToOneJsonMessage()
            throws Exception {

        /*
         * Tạo file tạm với tên giống định dạng file thực tế.
         *
         * Producer chưa parse metadata thời gian từ tên file.
         * Nó chỉ giữ nguyên tên trong source_file.
         */
        Path inputFile = tempDirectory.resolve(
                "A20240626.1400+0700-"
                        + "20240626.1401+0700_840_ebs"
        );

        /*
         * Dùng dữ liệu ngắn và đã ẩn danh.
         *
         * Test này không kiểm tra đủ 52 field vì producer
         * không chịu trách nhiệm parse schema nghiệp vụ.
         * Việc kiểm tra 52 field thuộc Bronze Job.
         */
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

        // Thành phần đọc file.
        FileLogReader reader =
                new FileLogReader();

        // Thành phần chuyển SourceLine thành RawNetworkEvent.
        RawNetworkEventFactory factory =
                new RawNetworkEventFactory();

        // Thành phần chuyển RawNetworkEvent thành JSON.
        RawNetworkEventJsonSerializer serializer =
                new RawNetworkEventJsonSerializer();

        // Dùng để đọc lại JSON trong phần assert.
        ObjectMapper objectMapper =
                new ObjectMapper();

        /*
         * Danh sách này chỉ dùng trong test.
         *
         * Trong chương trình thật, JSON sẽ được gửi ngay
         * cho Kafka producer thay vì giữ toàn bộ trong List.
         */
        List<String> jsonMessages =
                new ArrayList<>();

        /*
         * Đọc file theo từng dòng.
         *
         * Với mỗi SourceLine:
         * 1. Factory tạo RawNetworkEvent.
         * 2. Serializer tạo JSON.
         * 3. JSON được thêm vào danh sách kiểm tra.
         */
        long lineCount = reader.read(
                inputFile,
                sourceLine -> {
                    RawNetworkEvent event =
                            factory.create(sourceLine);

                    String json =
                            serializer.serialize(event);

                    jsonMessages.add(json);
                }
        );

        // Reader phải đọc đúng ba dòng.
        assertEquals(
                3L,
                lineCount
        );

        // Mỗi dòng phải tạo đúng một JSON message.
        assertEquals(
                3,
                jsonMessages.size()
        );

        // Parse message đầu tiên.
        JsonNode firstMessage =
                objectMapper.readTree(
                        jsonMessages.get(0)
                );

        // Message thứ nhất phải có source_line bằng 1.
        assertEquals(
                1L,
                firstMessage.get("source_line").asLong()
        );

        // raw_payload phải đúng với dòng đầu tiên.
        assertEquals(
                inputLines.get(0),
                firstMessage.get("raw_payload").asText()
        );

        // source_file phải chỉ là tên file, không phải full path.
        assertEquals(
                inputFile.getFileName().toString(),
                firstMessage.get("source_file").asText()
        );

        // Parse message thứ hai.
        JsonNode secondMessage =
                objectMapper.readTree(
                        jsonMessages.get(1)
                );

        // Message thứ hai phải có source_line bằng 2.
        assertEquals(
                2L,
                secondMessage.get("source_line").asLong()
        );

        // Parse message thứ ba.
        JsonNode thirdMessage =
                objectMapper.readTree(
                        jsonMessages.get(2)
                );

        // Message thứ ba phải có source_line bằng 3.
        assertEquals(
                3L,
                thirdMessage.get("source_line").asLong()
        );

        // raw_payload thứ ba phải được giữ nguyên.
        assertEquals(
                inputLines.get(2),
                thirdMessage.get("raw_payload").asText()
        );

        /*
         * Ba dòng khác nhau phải có raw_record_id khác nhau.
         *
         * Ngay cả khi raw payload giống nhau, lineNumber khác nhau
         * cũng sẽ tạo ID khác.
         */
        assertNotEquals(
                firstMessage.get("raw_record_id").asText(),
                secondMessage.get("raw_record_id").asText()
        );

        assertNotEquals(
                secondMessage.get("raw_record_id").asText(),
                thirdMessage.get("raw_record_id").asText()
        );
    }
}