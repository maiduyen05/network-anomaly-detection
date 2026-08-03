package com.network.preprocess.bronze;

import com.network.preprocess.model.BronzeDlqRecord;
import com.network.preprocess.model.BronzeEvent;

import java.io.Serializable;
import java.util.Objects;

/**
 * Kết quả của một lần Bronze transform. 
 * (Phiếu kết quả BronzeTransform đưa cho BronzeProcessFunction để quyết định gửi sang DLQ hay tiếp tục xử lý)
 *
 * <p>Mỗi result chứa chính xác một trong hai:</p>
 *
 * <ul>
 *     <li>BronzeEvent hợp lệ.</li>
 *     <li>BronzeDlqRecord lỗi.</li>
 * </ul>
 */
public final class BronzeTransformResult implements Serializable {

    private final BronzeEvent event;
    private final BronzeDlqRecord dlqRecord;

    private BronzeTransformResult(
            BronzeEvent event,
            BronzeDlqRecord dlqRecord
    ) {
        this.event = event;
        this.dlqRecord = dlqRecord;
    }

    public static BronzeTransformResult valid(
            BronzeEvent event
    ) {
        return new BronzeTransformResult(
                Objects.requireNonNull(event),
                null
        );
    }

    public static BronzeTransformResult invalid(
            BronzeDlqRecord dlqRecord
    ) {
        return new BronzeTransformResult(
                null,
                Objects.requireNonNull(dlqRecord)
        );
    }

    public boolean isValid() {
        return event != null;
    }

    public BronzeEvent getEvent() {
        return event;
    }

    public BronzeDlqRecord getDlqRecord() {
        return dlqRecord;
    }
}