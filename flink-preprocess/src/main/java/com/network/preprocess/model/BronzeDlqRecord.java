package com.network.preprocess.model;

import java.io.Serializable;

/**
 * Record lỗi được ghi vào dlq.ue.log.line.
 *
 * @param schemaVersion schema của DLQ record
 * @param dlqId ID deterministic của lỗi
 * @param rawRecordId ID raw envelope nếu đọc được
 * @param errorCode mã lỗi cố định
 * @param errorMessage mô tả an toàn, không chứa raw payload/identity
 * @param failedAt thời điểm lỗi dạng UTC ISO-8601
 * @param originalMessage message gốc phục vụ replay
 * @param source metadata nguồn đọc được
 */
public record BronzeDlqRecord(
        String schemaVersion,
        String dlqId,
        String rawRecordId,
        BronzeErrorCode errorCode,
        String errorMessage,
        String failedAt,
        String originalMessage,
        BronzeSourceMetadata source
) implements Serializable {

    /**
     * Không đưa originalMessage vào log vì nó có thể chứa IMSI/MSISDN.
     */
    @Override
    public String toString() {
        return "BronzeDlqRecord{" +
                "schemaVersion='" + schemaVersion + '\'' +
                ", dlqId='" + dlqId + '\'' +
                ", rawRecordId='" + rawRecordId + '\'' +
                ", errorCode=" + errorCode +
                ", originalMessage=<redacted>" +
                '}';
    }
}