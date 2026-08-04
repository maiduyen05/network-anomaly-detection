package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata phục vụ hiển thị.
 *
 * <p>Display không được sử dụng làm key hoặc điều kiện nghiệp vụ.
 * Các nhãn trong đây có thể thay đổi mà không làm thay đổi
 * canonical event ID.</p>
 */
public record SilverDisplay(
        String eventName,
        String eventResultLabel
) implements Serializable {

    public SilverDisplay {
        Objects.requireNonNull(
                eventName,
                "eventName must not be null"
        );

        Objects.requireNonNull(
                eventResultLabel,
                "eventResultLabel must not be null"
        );
    }
}