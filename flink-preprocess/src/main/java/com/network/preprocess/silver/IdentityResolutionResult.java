package com.network.preprocess.silver;

import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.InvalidIdentityRecord;

import java.io.Serializable;
import java.util.Objects;

/**
 * Kết quả của một lần resolve identity.
 *
 * <p>Mỗi result chứa chính xác một trong hai:</p>
 *
 * <ul>
 *     <li>IdentityResolvedEvent hợp lệ.</li>
 *     <li>InvalidIdentityRecord.</li>
 * </ul>
 */
public final class IdentityResolutionResult
        implements Serializable {

    private final IdentityResolvedEvent resolvedEvent;
    private final InvalidIdentityRecord invalidRecord;

    private IdentityResolutionResult(
            IdentityResolvedEvent resolvedEvent,
            InvalidIdentityRecord invalidRecord
    ) {
        /*
         * Hai field cùng null hoặc cùng khác null đều là lỗi code.
         */
        if ((resolvedEvent == null) == (invalidRecord == null)) {
            throw new IllegalArgumentException(
                    "Result must contain exactly one outcome"
            );
        }

        this.resolvedEvent = resolvedEvent;
        this.invalidRecord = invalidRecord;
    }

    public static IdentityResolutionResult resolved(
            IdentityResolvedEvent event
    ) {
        return new IdentityResolutionResult(
                Objects.requireNonNull(event),
                null
        );
    }

    public static IdentityResolutionResult invalid(
            InvalidIdentityRecord record
    ) {
        return new IdentityResolutionResult(
                null,
                Objects.requireNonNull(record)
        );
    }

    public boolean isResolved() {
        return resolvedEvent != null;
    }

    public IdentityResolvedEvent getResolvedEvent() {
        return resolvedEvent;
    }

    public InvalidIdentityRecord getInvalidRecord() {
        return invalidRecord;
    }
}