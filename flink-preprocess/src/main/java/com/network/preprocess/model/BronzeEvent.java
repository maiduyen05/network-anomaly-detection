package com.network.preprocess.model;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Event hợp lệ ở cuối tầng Bronze.
 *
 * <p>BronzeEvent đã:</p>
 *
 * <ul>
 *     <li>Parse JSON envelope.</li>
 *     <li>Parse đúng 52 trường raw log.</li>
 *     <li>Chuẩn hóa EVENT_TIME về UTC.</li>
 *     <li>Chuyển các numeric field sang kiểu số.</li>
 *     <li>Giữ Kafka metadata để deduplicate tại Silver.</li>
 * </ul>
 *
 * <p>BronzeEvent chưa:</p>
 *
 * <ul>
 *     <li>Resolve subscriber identity về IMSI.</li>
 *     <li>Chuẩn hóa EVENT_ID theo danh mục model.</li>
 *     <li>Deduplicate.</li>
 *     <li>Gắn watermark.</li>
 *     <li>Tạo model feature.</li>
 * </ul>
 */
/**
 * Event hợp lệ ở cuối tầng Bronze.
 */
public record BronzeEvent(
        String schemaVersion,
        String rawRecordId,

        String eventId,
        String eventResult,
        Long durationMs,
        Integer requestRetries,
        Integer pagingAttempts,

        String eventTime,
        String eventTimeQuality,

        String msisdn,
        String imsi,
        String mtmsi,
        String imeisv,
        String mmegi,
        String mmec,
        String subCauseCode,
        String msc,
        String tac,
        String eci,
        String sgw,
        String pdnPgw,

        Map<String, String> rawFields,
        BronzeSourceMetadata source
) implements Serializable {

    /**
     * Tạo bản sao độc lập của rawFields.
     *
     * Không sử dụng Map.copyOf(), Collections.emptyMap() hoặc
     * Collections.unmodifiableMap() vì Flink/Kryo cần Map mutable
     * trong quá trình sao chép record giữa các operator.
     *
     * Việc tạo LinkedHashMap mới vẫn ngăn Map truyền từ bên ngoài
     * thay đổi trực tiếp dữ liệu đang nằm trong BronzeEvent.
     */
    public BronzeEvent {
        rawFields = rawFields == null
                ? new LinkedHashMap<>()
                : new LinkedHashMap<>(rawFields);
    }
}