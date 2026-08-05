package com.network.preprocess.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

/**
 * Kết quả sự kiện được feature contract hỗ trợ.
 */
public enum EventResult implements Serializable {

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
     * Nhãn thân thiện dùng cho dashboard.
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