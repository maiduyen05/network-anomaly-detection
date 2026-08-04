package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Event hợp lệ ở đầu ra tầng Silver.
 *
 * <p>SilverEvent đã:</p>
 *
 * <ul>
 *     <li>Resolve subscriber identity về IMSI.</li>
 *     <li>Tạo ueKey ổn định.</li>
 *     <li>Chuẩn hóa EVENT_ID theo model catalog.</li>
 *     <li>Chuẩn hóa EVENT_RESULT.</li>
 *     <li>Tạo display metadata.</li>
 *     <li>Tạo quality metadata.</li>
 * </ul>
 *
 * <p>SilverEvent chưa:</p>
 *
 * <ul>
 *     <li>Deduplicate.</li>
 *     <li>Gắn watermark.</li>
 *     <li>Phân loại late event.</li>
 *     <li>Tạo chuỗi sự kiện Gold.</li>
 * </ul>
 */
public record SilverEvent(
        String schemaVersion,
        String silverEventId,
        String rawRecordId,

        String ueKey,
        String imsi,

        String eventId,
        EventResult eventResult,

        Long durationMs,
        Integer requestRetries,
        Integer subType,

        String eventTime,
        String reportSide,

        String msisdn,
        String mtmsi,
        String imeisv,

        String mmegi,
        String mmec,
        String tac,
        String eci,
        String sgw,
        String sgsn,

        SilverDisplay display,
        SilverQuality quality,

        Map<String, String> rawFields,
        BronzeSourceMetadata source
) implements Serializable {

    public SilverEvent {
        Objects.requireNonNull(
                schemaVersion,
                "schemaVersion must not be null"
        );

        Objects.requireNonNull(
                silverEventId,
                "silverEventId must not be null"
        );

        Objects.requireNonNull(
                rawRecordId,
                "rawRecordId must not be null"
        );

        Objects.requireNonNull(
                ueKey,
                "ueKey must not be null"
        );

        Objects.requireNonNull(
                imsi,
                "imsi must not be null"
        );

        Objects.requireNonNull(
                eventId,
                "eventId must not be null"
        );

        Objects.requireNonNull(
                eventResult,
                "eventResult must not be null"
        );

        Objects.requireNonNull(
                eventTime,
                "eventTime must not be null"
        );

        Objects.requireNonNull(
                display,
                "display must not be null"
        );

        Objects.requireNonNull(
                quality,
                "quality must not be null"
        );

        Objects.requireNonNull(
                source,
                "source must not be null"
        );


        /*
         * Lưu một LinkedHashMap mutable ở bên trong để tương thích
         * với cơ chế copy/serialization của Flink và Kryo.
         */
        rawFields = rawFields == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(rawFields);
    }

        /**
     * Không cho code bên ngoài thay đổi rawFields.
     *
     * Map bên trong vẫn là LinkedHashMap mutable để Flink có thể
     * serialize và copy khi shuffle, checkpoint hoặc chạy test harness.
     */
        @Override
        public Map<String, String> rawFields() {
                return Collections.unmodifiableMap(rawFields);
        }
    }
