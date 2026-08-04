package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Định nghĩa một EVENT_ID mà model chấp nhận.
 *
 * @param canonicalEventId EVENT_ID chuẩn dùng xuyên suốt Silver và Gold
 * @param displayName tên thân thiện phục vụ hiển thị
 */
public record EventDefinition(
        String canonicalEventId,
        String displayName
) implements Serializable {

    public EventDefinition {
        Objects.requireNonNull(
                canonicalEventId,
                "canonicalEventId must not be null"
        );

        Objects.requireNonNull(
                displayName,
                "displayName must not be null"
        );

        canonicalEventId = canonicalEventId.trim();
        displayName = displayName.trim();

        if (canonicalEventId.isEmpty()) {
            throw new IllegalArgumentException(
                    "canonicalEventId must not be blank"
            );
        }

        if (displayName.isEmpty()) {
            throw new IllegalArgumentException(
                    "displayName must not be blank"
            );
        }
    }
}