package com.network.producer.model;

/**
 * JSON envelope được gửi vào Kafka raw topic.
 *
 * <p>Envelope chỉ giữ raw payload và metadata tối thiểu.
 * Producer không parse 52 trường nghiệp vụ.</p>
 *
 * @param rawRecordId
 *        ID SHA-256 định danh dòng nguồn.
 *
 * @param schemaVersion
 *        Phiên bản của JSON envelope.
 *
 * @param sourceFile
 *        Tên file nguồn.
 *
 * @param sourceLine
 *        Số dòng trong file, bắt đầu từ 1.
 *
 * @param rawPayload
 *        Nội dung nguyên bản của dòng log.
 */

// record: kiểu lớp đặc biệt trong java dùng để biểu diễn DL đơn giản, không cần tự viết nhiều mã lặp như constructor, getter,...
// Dùng record vì phù hợp với object immutable (sau khi tạo không thể thay đổi), chỉ có getter, không có setter.
// Tự sinh equals(), hashCode(), toString() và constructor.

public record RawNetworkEvent(
        String rawRecordId,
        String schemaVersion,
        String sourceFile,
        long sourceLine,
        String rawPayload
) {
}


