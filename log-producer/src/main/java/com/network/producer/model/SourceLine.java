package com.network.producer.model;

/**
 * Đại diện cho một dòng vừa được đọc từ file nguồn, chứa thông tin lấy trực tiếp từ file
 * Chưa phải message Kafka hoàn chỉnh do chưa có eventId và ingestedAt.
 *
 * @param sourceFile tên file nguồn
 * @param lineNumber số thứ tự dòng trong file, bắt đầu từ 1
 * @param rawData nội dung nguyên bản của dòng
 */
public record SourceLine(
        String sourceFile,
        long lineNumber,
        String rawData
) {
}