package com.network.producer.kafka;

import com.network.producer.model.RawNetworkEvent;

import java.util.Objects;

/**
 * Xác định Kafka key cho RawNetworkEvent.
 *
 * Producer chỉ sử dụng rawRecordId làm key và không đọc các field
 * nghiệp vụ trong raw payload. Việc parse IMSI/MSISDN và keyBy
 * thuộc trách nhiệm của Flink.
 */
public final class KafkaMessageKeyResolver {

    public String resolve(RawNetworkEvent event) {

        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        String rawRecordId = event.rawRecordId();

        if (rawRecordId == null || rawRecordId.isBlank()) {
            throw new IllegalArgumentException(
                    "rawRecordId must not be blank"
            );
        }

        return rawRecordId.trim();
    }
}