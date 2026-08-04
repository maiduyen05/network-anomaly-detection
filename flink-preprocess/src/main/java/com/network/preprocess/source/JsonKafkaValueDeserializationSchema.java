package com.network.preprocess.source;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.util.Objects;

/**
 * Chuyển phần value của Kafka message từ JSON thành Java object.
 *
 * <p>Ví dụ:</p>
 *
 * <pre>
 * Kafka value JSON
 *      ↓
 * JsonKafkaValueDeserializationSchema&lt;BronzeEvent&gt;
 *      ↓
 * BronzeEvent
 * </pre>
 *
 * @param <T> kiểu Java cần deserialize
 */
public final class JsonKafkaValueDeserializationSchema<T>
        implements DeserializationSchema<T> {

    /**
     * Class cụ thể mà Jackson cần tạo.
     */
    private final Class<T> targetType;

    /**
     * ObjectMapper được tạo riêng trên từng TaskManager.
     *
     * <p>Đặt transient để Flink không serialize trực tiếp mapper
     * từ JobManager sang TaskManager.</p>
     */
    private transient ObjectMapper objectMapper;

    public JsonKafkaValueDeserializationSchema(
            Class<T> targetType
    ) {
        this.targetType = Objects.requireNonNull(
                targetType,
                "targetType must not be null"
        );
    }

    /**
     * Deserialize một Kafka value.
     */
    @Override
    public T deserialize(
            byte[] message
    ) throws IOException {
        if (message == null || message.length == 0) {
            throw new IOException(
                    "Kafka message value must not be empty"
            );
        }

        return mapper().readValue(
                message,
                targetType
        );
    }

    /**
     * Kafka stream là unbounded nên không có end-of-stream marker.
     */
    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    /**
     * Cung cấp type information cho Flink.
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetType);
    }

    /**
     * Khởi tạo ObjectMapper khi TaskManager sử dụng lần đầu.
     */
    private ObjectMapper mapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();

            /*
             * schema_version -> schemaVersion
             * raw_record_id  -> rawRecordId
             */
            objectMapper.setPropertyNamingStrategy(
                    PropertyNamingStrategies.SNAKE_CASE
            );

            /*
             * Bronze và Silver là contract nội bộ.
             *
             * Nếu JSON có field lạ mà model Java chưa biết,
             * fail rõ ràng thay vì âm thầm bỏ qua.
             */
            objectMapper.configure(
                    DeserializationFeature
                            .FAIL_ON_UNKNOWN_PROPERTIES,
                    true
            );
        }

        return objectMapper;
    }
}