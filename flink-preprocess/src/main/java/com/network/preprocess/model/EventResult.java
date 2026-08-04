package com.network.preprocess.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;

/**
 * Kết quả sự kiện đã được chuẩn hóa tại Silver.
 *
 * <p>Không giữ tùy ý mọi giá trị raw trong field eventResult.
 * Những giá trị raw không nhận diện được sẽ được chuyển thành
 * {@link #UNKNOWN} và đánh dấu trong SilverQuality.</p>
 */
public enum EventResult implements Serializable {

    SUCCESS(
            "success",
            "Success"
    ),

    FAILURE(
            "failure",
            "Failure"
    ),

    TIMEOUT(
            "timeout",
            "Timeout"
    ),

    UNKNOWN(
            "unknown",
            "Unknown"
    );

    /**
     * Giá trị ổn định được ghi ra JSON/Kafka.
     */
    private final String wireValue;

    /**
     * Nhãn thân thiện cho dashboard hoặc UI.
     */
    private final String displayLabel;

    EventResult(
            String wireValue,
            String displayLabel
    ) {
        this.wireValue = wireValue;
        this.displayLabel = displayLabel;
    }

    /**
     * @JsonValue giúp Jackson ghi "success" thay vì "SUCCESS".
     */
    @JsonValue
    public String wireValue() {
        return wireValue;
    }

    public String displayLabel() {
        return displayLabel;
    }
}