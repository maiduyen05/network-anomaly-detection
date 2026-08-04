package com.network.preprocess.silver.event;

import com.network.preprocess.model.EventDefinition;

import java.io.Serializable;
import java.util.Optional;

/**
 * Cổng tra cứu event theo lookup key đã chuẩn hóa.
 *
 * <p>Catalog thật có thể đến từ:</p>
 *
 * <ul>
 *     <li>File cấu hình.</li>
 *     <li>Database.</li>
 *     <li>Kafka compacted topic.</li>
 *     <li>Broadcast state.</li>
 * </ul>
 */
public interface EventCatalog extends Serializable {

    /**
     * @param normalizedEventId EVENT_ID đã qua EventIdNormalizer
     * @return định nghĩa model nếu event được hỗ trợ
     */
    Optional<EventDefinition> findByEventId(
            String normalizedEventId
    );
}