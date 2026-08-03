package com.network.preprocess.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Đại diện cho JSON envelope được producer gửi vào raw Kafka topic.
 *
 * <p>Ví dụ message Kafka:</p>
 *
 * <pre>
 * {
 *   "schema_version": "raw-envelope-v1",
 *   "raw_record_id": "record-0001",
 *   "source_file": "ue-log-20260803.csv",
 *   "source_line": 125,
 *   "raw_payload": "field-1;field-2;...;field-52"
 * }
 * </pre>
 *
 * <p>Class này chỉ biểu diễn cấu trúc JSON envelope. Nó chưa:</p>
 *
 * <ul>
 *     <li>Kiểm tra các field bắt buộc.</li>
 *     <li>Kiểm tra schema version.</li>
 *     <li>Parse 52 trường trong raw_payload.</li>
 *     <li>Chuẩn hóa timestamp.</li>
 *     <li>Ép kiểu numeric.</li>
 * </ul>
 *
 * <p>Các công việc trên được giao cho những class riêng biệt để mỗi
 * class chỉ có một trách nhiệm.</p>
 *
 * @param schemaVersion phiên bản schema của JSON envelope
 * @param rawRecordId ID ổn định của raw record do producer tạo
 * @param sourceFile tên file log nguồn
 * @param sourceLine số dòng trong file nguồn, bắt đầu từ 1
 * @param rawPayload dòng log thô chứa 52 field phân cách bằng delimiter
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RawNetworkEvent(

        /*
         * @JsonProperty ánh xạ tên JSON dạng snake_case
         * sang tên Java dạng camelCase.
         *
         * JSON: schema_version
         * Java: schemaVersion
         */
        @JsonProperty("schema_version")
        String schemaVersion,

        /*
         * JSON: raw_record_id
         * Java: rawRecordId
         */
        @JsonProperty("raw_record_id")
        String rawRecordId,

        /*
         * JSON: source_file
         * Java: sourceFile
         */
        @JsonProperty("source_file")
        String sourceFile,

        /*
         * Dùng Long thay vì long.
         *
         * Nếu source_line bị thiếu trong JSON, Jackson sẽ gán null.
         * Nếu dùng long, giá trị thiếu có thể trở thành 0 và làm mất
         * khả năng phân biệt "không có field" với "field có giá trị 0".
         */
        @JsonProperty("source_line")
        Long sourceLine,

        /*
         * rawPayload vẫn là một String tại thời điểm này.
         *
         * RawLogLineParser ở Bước 6.8 mới chịu trách nhiệm tách
         * rawPayload thành đúng 52 field.
         */
        @JsonProperty("raw_payload")
        String rawPayload

) implements Serializable {
}