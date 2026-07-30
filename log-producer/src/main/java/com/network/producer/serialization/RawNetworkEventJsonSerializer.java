package com.network.producer.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.network.producer.model.RawNetworkEvent;

import java.util.Objects;

/**
 * Chuyển RawNetworkEvent Java object thành chuỗi JSON (chuyển sang dạng string)
 */
public final class RawNetworkEventJsonSerializer {

    /**
    ObjectMapper là thành phần của Jackson dùng để:
    1. Đọc các component trong Java record
    2. Chuyển chúng thành JSON
    3. Escape các ký tự đặc biệt đúng chuẩn JSON
     *
    Ví dụ nếu raw payload chứa dấu ngoặc kép ==> Jackson sẽ escape nó đúng cách thay vì làm JSON lỗi.</p>
     */
    private final ObjectMapper objectMapper;

    /**
     * Khởi tạo serializer với cấu hình chuẩn của project.
     */
    public RawNetworkEventJsonSerializer() {

        // Tạo Jackson ObjectMapper.
        this.objectMapper = new ObjectMapper();

        /*
        Chuyển tên field Java từ camelCase sang snake_case (do JSON dùng snake_case)
        Ví dụ:
         * rawRecordId   → raw_record_id
         * schemaVersion → schema_version
         */
        this.objectMapper.setPropertyNamingStrategy(
                PropertyNamingStrategies.SNAKE_CASE
        );
    }

    /**
     * Chuyển RawNetworkEvent thành JSON.
     *
     * @param event raw event cần serialize
     * @return chuỗi JSON hoàn chỉnh
     * @throws NullPointerException nếu event bằng null
     * @throws IllegalStateException nếu Jackson không thể tạo JSON
     */
    public String serialize(RawNetworkEvent event) {

        /*
        Không cho phép serialize một object null.
        Chú ý: Nếu không kiểm tra ở đây, lỗi có thể xuất hiện muộn hơn khi publisher chuẩn bị gửi Kafka.
         */
        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        try {
            /*
            Jackson đọc các component của RawNetworkEvent record và tạo chuỗi JSON.
             * Thứ tự field trong JSON không nên được dùng làm logic xử lý. Consumer phải đọc theo tên field.
             */
            return objectMapper.writeValueAsString(event);

        } catch (JsonProcessingException exception) {
            /*
             * RawNetworkEvent chỉ chứa String và long nên lỗi
             * serialize thường là lỗi code hoặc cấu hình Jackson.
             *
             * Chuyển checked exception thành IllegalStateException
             * để caller không phải xử lý một lỗi không thể phục hồi
             * cho từng message.
             */
            throw new IllegalStateException(
                    "Could not serialize RawNetworkEvent to JSON",
                    exception
            );
        }
    }
}