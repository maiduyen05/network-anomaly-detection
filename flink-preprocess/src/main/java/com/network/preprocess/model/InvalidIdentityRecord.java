package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Record được route vào topic invalid-identity (phải được giưới hạn ACL và retention phù hợp )
 *
 * <p>Record giữ BronzeEvent gốc để có thể điều tra hoặc replay.
 * Vì BronzeEvent chứa IMSI/MSISDN, không được log toàn bộ record.</p>
 *
 * @param schemaVersion schema của invalid identity record
 * @param invalidIdentityId ID deterministic của lỗi
 * @param rawRecordId raw_record_id từ Bronze
 * @param reason nguyên nhân không resolve được identity
 * @param errorMessage thông báo an toàn, không chứa giá trị identity
 * @param failedAt thời điểm Silver xử lý record dạng UTC ISO-8601
 * @param originalEvent BronzeEvent gốc phục vụ đối soát/replay
 */
public record InvalidIdentityRecord(
        String schemaVersion,
        String invalidIdentityId,
        String rawRecordId,
        InvalidIdentityReason reason,
        String errorMessage,
        String failedAt,
        BronzeEvent originalEvent
) implements Serializable {

    public InvalidIdentityRecord {
        Objects.requireNonNull(
                schemaVersion,
                "schemaVersion must not be null"
        );

        Objects.requireNonNull(
                invalidIdentityId,
                "invalidIdentityId must not be null"
        );

        Objects.requireNonNull(
                rawRecordId,
                "rawRecordId must not be null"
        );

        Objects.requireNonNull(
                reason,
                "reason must not be null"
        );

        Objects.requireNonNull(
                errorMessage,
                "errorMessage must not be null"
        );

        Objects.requireNonNull(
                failedAt,
                "failedAt must not be null"
        );

        Objects.requireNonNull(
                originalEvent,
                "originalEvent must not be null"
        );
    }

    /**
     * Không gọi originalEvent.toString().
     *
     * <p>BronzeEvent có thể chứa IMSI, MSISDN, MTMSI và IMEISV.
     * Những giá trị này không nên xuất hiện trong application log.</p>
     */
    @Override
    public String toString() {
        return "InvalidIdentityRecord{" +
                "schemaVersion='" + schemaVersion + '\'' +
                ", invalidIdentityId='" + invalidIdentityId + '\'' +
                ", rawRecordId='" + rawRecordId + '\'' +
                ", reason=" + reason +
                ", originalEvent=<redacted>" +
                '}';
    }
}