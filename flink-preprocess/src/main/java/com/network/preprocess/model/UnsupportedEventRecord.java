package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Record ghi vào topic unsupported-event.
 *
 * <p>originalEvent được giữ để điều tra và replay nhưng không được
 * in ra log qua toString vì nó chứa IMSI, MSISDN và các dữ liệu mạng.</p>
 */
public record UnsupportedEventRecord(
        String schemaVersion,
        String unsupportedEventId,
        String rawRecordId,
        UnsupportedEventReason reason,
        String message,
        String failedAt,
        IdentityResolvedEvent originalEvent
) implements Serializable {

    public UnsupportedEventRecord {
        Objects.requireNonNull(
                schemaVersion,
                "schemaVersion must not be null"
        );

        Objects.requireNonNull(
                unsupportedEventId,
                "unsupportedEventId must not be null"
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
                message,
                "message must not be null"
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
     */
    @Override
    public String toString() {
        return "UnsupportedEventRecord[" +
                "schemaVersion=" + schemaVersion +
                ", unsupportedEventId=" + unsupportedEventId +
                ", rawRecordId=" + rawRecordId +
                ", reason=" + reason +
                ", message=" + message +
                ", failedAt=" + failedAt +
                ", originalEvent=<redacted>" +
                ']';
    }
}