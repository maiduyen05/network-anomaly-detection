package com.network.producer.reader;

import com.network.producer.model.SourceLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Đọc file log tuần tự theo từng dòng (không nạp cả file vào RAM cùng 1 lúc)
 */
public final class FileLogReader {

    /**
     * Đọc một file và chuyển từng dòng cho lineConsumer (sau này dễ tái lập)
     * Trong 1 lineConsumer có thể là: tạo RawNetworkEvenr, chuyển sang định dạng json, đưa vào kafka,...
     *
     * @param filePath đường dẫn file cần đọc
     * @param lineConsumer hàm nhận và xử lý từng SourceLine
     * @return tổng số dòng đã đọc
     * @throws IOException nếu không thể đọc file
     */
    public long read(
            Path filePath,
            Consumer<SourceLine> lineConsumer
    ) throws IOException {

        // Không cho phép filePath bằng null.
        Objects.requireNonNull(
                filePath,
                "filePath must not be null"
        );

        // Không cho phép lineConsumer bằng null.
        Objects.requireNonNull(
                lineConsumer,
                "lineConsumer must not be null"
        );

        // Đường dẫn phải tồn tại.
        if (!Files.exists(filePath)) {
            throw new IllegalArgumentException(
                    "Input file does not exist: " + filePath
            );
        }

        // Đường dẫn phải là file thường, không phải thư mục.
        if (!Files.isRegularFile(filePath)) {
            throw new IllegalArgumentException(
                    "Input path is not a regular file: " + filePath
            );
        }

        /*
         * Chỉ lấy tên file, không lấy toàn bộ đường dẫn, nếu không SHA-256 làm eventID thay đổi
         * Ví dụ:
         * /home/user/data/sample.log
         * → sample.log
         */
        String sourceFile =
                filePath.getFileName().toString();

        // Số dòng đã đọc.
        long lineNumber = 0L;

        /*
         * BufferedReader đọc dữ liệu tuần tự.
         * try-with-resources bảo đảm file được đóng ngay cả khi xảy ra lỗi.
         */
        try (
                BufferedReader reader =
                        Files.newBufferedReader(
                                filePath,
                                StandardCharsets.UTF_8
                        )
        ) {
            String rawData;

            /*
             * readLine() chỉ đọc một dòng mỗi lần, khi hết file trả về null
             */
            while ((rawData = reader.readLine()) != null) {

                // Dòng đầu tiên có số thứ tự 1.
                lineNumber++;

                // Tạo object mô tả dòng vừa đọc.
                SourceLine sourceLine = new SourceLine(
                        sourceFile,
                        lineNumber,
                        rawData
                );

                /*
                 * Gửi dòng hiện tại cho lineConsumer xử lý các bước tiếp theo
                 */
                lineConsumer.accept(sourceLine);
            }
        }

        // Trả về tổng số dòng đọc thành công.
        return lineNumber;
    }
}