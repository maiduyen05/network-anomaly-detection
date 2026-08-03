package com.network.preprocess.model;

import java.io.Serializable;

/**
 * Đại diện 1 record vừa được đọc từ Kafka raw topic (gồm DL gốc và địa chỉ của nó).
 *
 * <p> 
 * Ở đây, DL vẫn chỉ là chuỗi văn bản chưa được chuyển thành đối tượng Java
 * ==> Chương trình xử lý an toàn hơn 
 * Nếu JSON bị sai định dạng, thiếu trường hoặc có DL không hợp lệ, CT có thể tự kiểm tra 
 * và gửi record sang DLQ thay vì làm Kafka Sorce phát sinh exception 
 * khiến Flink job khởi động lại liên tục Class này chưa parse JSON. Nếu JSON sai mà source
 </p>
 *
 * @param topic Kafka topic chứa record
 * @param partition partition chứa record
 * @param offset offset của record trong partition
 * @param kafkaTimestamp timestamp do Kafka gắn cho record
 * @param value nội dung UTF-8; có thể null
 * Các param như topic, partition và offset hữu ích khi cần truy vết DL
 */
public record KafkaRawRecord(
        String topic,
        int partition,
        long offset,
        long kafkaTimestamp,
        String value
) implements Serializable {
}