package com.network.preprocess.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.network.preprocess.model.RawNetworkEvent;

import java.io.Serializable;

/**
 * Chuyển Kafka value dạng JSON String thành RawNetworkEvent.
 *
 * <p>Class này chỉ parse cú pháp JSON. Nó không kiểm tra:</p>
 *
 * <ul>
 *     <li>schema_version có đúng hay không.</li>
 *     <li>raw_record_id có bị thiếu hay không.</li>
 *     <li>source_line có hợp lệ hay không.</li>
 *     <li>raw_payload có đủ 52 trường hay không.</li>
 * </ul>
 *
 * <p>Việc tách parse và validate giúp phân biệt hai loại lỗi:</p>
 *
 * <ul>
 *     <li>JSON sai cú pháp → INVALID_ENVELOPE_JSON.</li>
 *     <li>JSON đúng cú pháp nhưng thiếu field → INVALID_ENVELOPE_SCHEMA.</li>
 * </ul>
 */
public final class JsonEventParser implements Serializable {

    /*
     * ObjectMapper không nên được Flink serialize cùng operator.
     *
     * transient khiến Flink bỏ qua field này khi serialize
     * JsonEventParser từ JobManager sang TaskManager.
     *
     * Sau khi đến TaskManager, mapper() sẽ tạo ObjectMapper mới.
     */
    private transient ObjectMapper objectMapper;

    /**
     * Parse JSON thành RawNetworkEvent.
     *
     * @param json Kafka value dạng JSON
     * @return RawNetworkEvent được Jackson tạo ra
     * @throws JsonProcessingException nếu JSON sai cú pháp hoặc không thể
     *                                 chuyển thành RawNetworkEvent
     */
    public RawNetworkEvent parse(String json)
            throws JsonProcessingException {

        /*
         * kafkaRecord.value() == null đã được BronzeTransformer
         * kiểm tra trước khi gọi method này.
         *
         * Nếu code khác gọi parse(null), đây là lỗi sử dụng API,
         * không phải dữ liệu cần route sang Bronze DLQ.
         */
        if (json == null) {
            throw new IllegalArgumentException(
                    "json must not be null"
            );
        }

        /*
         * readValue thực hiện hai việc:
         *
         * 1. Parse cú pháp JSON.
         * 2. Ánh xạ JSON vào RawNetworkEvent.
         *
         * Ví dụ ánh xạ:
         *
         * schema_version -> schemaVersion
         * raw_record_id  -> rawRecordId
         * source_file    -> sourceFile
         * source_line    -> sourceLine
         * raw_payload    -> rawPayload
         *
         * Không catch JsonProcessingException tại đây.
         * BronzeTransformer cần exception này để tạo DLQ record với
         * error code INVALID_ENVELOPE_JSON.
         */
        return mapper().readValue(
                json,
                RawNetworkEvent.class
        );
    }

    /**
     * Lấy ObjectMapper hiện tại hoặc tạo mới nếu parser vừa được
     * deserialize trên Flink TaskManager.
     */
    private ObjectMapper mapper() {

        /*
         * Lần đầu gọi parse(), objectMapper chưa tồn tại.
         */
        if (objectMapper == null) {

            /*
             * RawNetworkEvent chỉ chứa String và Long nên chưa cần
             * đăng ký JavaTimeModule.
             */
            objectMapper = new ObjectMapper();
        }

        /*
         * Những lần gọi sau sử dụng lại cùng ObjectMapper.
         */
        return objectMapper;
    }
}