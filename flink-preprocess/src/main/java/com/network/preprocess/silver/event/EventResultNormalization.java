package com.network.preprocess.silver.event;

import com.network.preprocess.model.EventResult;

import java.io.Serializable;
import java.util.Objects;

/**
 * Kết quả chuẩn hóa EVENT_RESULT.
 *
 * @param eventResult giá trị canonical
 * @param recognized raw result có được nhận diện hay không
 * @param changed raw result có bị thay đổi cách biểu diễn hay không
 */
public record EventResultNormalization(
        EventResult eventResult,
        boolean recognized,
        boolean changed
) implements Serializable {

    public EventResultNormalization {
        Objects.requireNonNull(
                eventResult,
                "eventResult must not be null"
        );
    }
}