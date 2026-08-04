package com.network.preprocess.silver.dedup;

import com.network.preprocess.model.BronzeSourceMetadata;
import com.network.preprocess.model.SilverEvent;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Objects;

/**
 * Tạo khóa dùng để deduplicate SilverEvent.
 *
 * <p>Khóa được tạo từ metadata Kafka của record gốc:</p>
 *
 * <pre>
 * source.topic + source.partition + source.offset
 * </pre>
 *
 * <p>Ví dụ:</p>
 *
 * <pre>
 * raw.ue.log.line | partition 1 | offset 200
 * </pre>
 *
 * <p>Nếu Flink đọc lại đúng record trên sau restart, record vẫn có
 * cùng topic, partition và offset. Vì vậy record sẽ được đưa về cùng
 * keyed state và bị nhận diện là duplicate.</p>
 *
 * <p>Không dùng ueKey làm khóa dedup. Một UE có nhiều event hợp lệ,
 * nên dùng ueKey sẽ làm mất các event sau event đầu tiên.</p>
 */
public final class SilverDedupKeySelector
        implements KeySelector<SilverEvent, String> {

    /**
     * Ký tự phân cách nội bộ.
     *
     * <p>Ký tự unit separator gần như không xuất hiện trong tên
     * Kafka topic, giúp tránh nhầm lẫn khi ghép khóa.</p>
     */
    private static final String SEPARATOR = "\u001F";

    /**
     * Lấy khóa dedup của một SilverEvent.
     *
     * @param event event cần lấy khóa
     * @return khóa duy nhất theo Kafka source metadata
     */
    @Override
    public String getKey(SilverEvent event) {
        Objects.requireNonNull(
                event,
                "SilverEvent must not be null"
        );

        /*
         * SilverEvent hợp lệ bắt buộc phải giữ source metadata
         * được truyền từ Bronze.
         */
        BronzeSourceMetadata source = Objects.requireNonNull(
                event.source(),
                "SilverEvent.source must not be null"
        );

        /*
         * topic + partition + offset đủ để xác định duy nhất
         * một record Kafka trong phạm vi một Kafka cluster.
         */
        return source.topic()
                + SEPARATOR
                + source.partition()
                + SEPARATOR
                + source.offset();
    }
}