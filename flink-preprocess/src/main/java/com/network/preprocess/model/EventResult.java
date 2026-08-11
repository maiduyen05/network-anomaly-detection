package com.network.preprocess.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

/**
 * Kết quả sự kiện được Silver/Gold feature contract hỗ trợ.
 *
 * <p>
 * Contract v2 giữ nguyên bốn giá trị thực tế quan sát được
 * trong raw dataset:
 * </p>
 *
 * <ul>
 *     <li>empty string</li>
 *     <li>abort</li>
 *     <li>reject</li>
 *     <li>success</li>
 * </ul>
 *
 * <p>
 * Empty string là một category explicit của contract v2.
 * Null vẫn được xem là missing.
 * </p>
 */
public enum EventResult implements Serializable {

    EMPTY(
            "",
            "Empty"
    ),

    ABORT(
            "abort",
            "Abort"
    ),

    REJECT(
            "reject",
            "Reject"
    ),

    SUCCESS(
            "success",
            "Success"
    );

    /**
     * Giá trị canonical được ghi ra JSON/Kafka
     * và được GoldFeatureEncoder sử dụng.
     */
    private final String wireValue;

    /**
     * Nhãn dùng cho UI/evidence.
     */
    private final String displayLabel;

    EventResult(
            String wireValue,
            String displayLabel
    ) {
        this.wireValue = wireValue;
        this.displayLabel = displayLabel;
    }

    @JsonValue
    public String wireValue() {
        return wireValue;
    }

    public String displayLabel() {
        return displayLabel;
    }
}