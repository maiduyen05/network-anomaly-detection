package com.network.preprocess.silver;

import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.UnsupportedEventRecord;

import java.io.Serializable;
import java.util.Objects;

/**
 * Kết quả chuyển IdentityResolvedEvent thành SilverEvent.
 *
 * <p>Mỗi result chứa chính xác một outcome:</p>
 *
 * <ul>
 *     <li>SilverEvent hợp lệ.</li>
 *     <li>UnsupportedEventRecord.</li>
 * </ul>
 */
public final class SilverTransformationResult
        implements Serializable {

    private final SilverEvent silverEvent;
    private final UnsupportedEventRecord unsupportedEvent;

    private SilverTransformationResult(
            SilverEvent silverEvent,
            UnsupportedEventRecord unsupportedEvent
    ) {
        if ((silverEvent == null)
                == (unsupportedEvent == null)) {

            throw new IllegalArgumentException(
                    "Result must contain exactly one outcome"
            );
        }

        this.silverEvent = silverEvent;
        this.unsupportedEvent = unsupportedEvent;
    }

    public static SilverTransformationResult supported(
            SilverEvent event
    ) {
        return new SilverTransformationResult(
                Objects.requireNonNull(event),
                null
        );
    }

    public static SilverTransformationResult unsupported(
            UnsupportedEventRecord record
    ) {
        return new SilverTransformationResult(
                null,
                Objects.requireNonNull(record)
        );
    }

    public boolean isSupported() {
        return silverEvent != null;
    }

    public SilverEvent getSilverEvent() {
        return silverEvent;
    }

    public UnsupportedEventRecord getUnsupportedEvent() {
        return unsupportedEvent;
    }
}