package com.network.preprocess.model;

import java.io.Serializable;

/**
 * Metadata dùng để truy vết Bronze event về nguồn ban đầu.
 *
 * @param sourceFile file log do producer cung cấp
 * @param sourceLine dòng trong file, bắt đầu từ 1
 * @param topic Kafka topic nguồn
 * @param partition Kafka partition nguồn
 * @param offset Kafka offset nguồn
 * @param kafkaTimestamp Kafka record timestamp dạng UTC ISO-8601
 * @param ingestTime thời điểm Bronze xử lý record dạng UTC ISO-8601
 */
public record BronzeSourceMetadata(
        String sourceFile,
        Long sourceLine,
        String topic,
        int partition,
        long offset,
        String kafkaTimestamp,
        String ingestTime
) implements Serializable {
}