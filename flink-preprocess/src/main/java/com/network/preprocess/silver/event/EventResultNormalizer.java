package com.network.preprocess.silver.event;

import com.network.preprocess.model.EventResult;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Chuẩn hóa EVENT_RESULT theo feature contract v2.
 *
 * <p>
 * Bốn category hợp lệ:
 * </p>
 *
 * <ul>
 *     <li>""</li>
 *     <li>abort</li>
 *     <li>reject</li>
 *     <li>success</li>
 * </ul>
 *
 * <p>
 * Null vẫn là missing.
 * Empty string là category hợp lệ.
 * </p>
 */
public final class EventResultNormalizer {

    private EventResultNormalizer() {
        // Utility class không cần instance.
    }

    /**
     * Chuẩn hóa EVENT_RESULT.
     *
     * @return Optional.empty chỉ khi null hoặc unknown category
     */
    public static Optional<EventResult> normalize(
            String rawEventResult
    ) {

        /*
         * Null nghĩa là field thực sự không tồn tại.
         */
        if (rawEventResult == null) {
            return Optional.empty();
        }

        String lookupKey =
                rawEventResult
                        .trim()
                        .toLowerCase(Locale.ROOT);

        return switch (lookupKey) {

            /*
             * Empty string là category hợp lệ của contract v2.
             */
            case "" ->
                    Optional.of(
                            EventResult.EMPTY
                    );

            case "abort" ->
                    Optional.of(
                            EventResult.ABORT
                    );

            case "reject" ->
                    Optional.of(
                            EventResult.REJECT
                    );

            case "success" ->
                    Optional.of(
                            EventResult.SUCCESS
                    );

            default ->
                    Optional.empty();
        };
    }

    /**
     * Kiểm tra raw representation có được normalize hay không.
     */
    public static boolean wasChanged(
            String rawEventResult,
            EventResult normalizedEventResult
    ) {

        Objects.requireNonNull(
                rawEventResult,
                "rawEventResult must not be null"
        );

        Objects.requireNonNull(
                normalizedEventResult,
                "normalizedEventResult must not be null"
        );

        return !normalizedEventResult
                .wireValue()
                .equals(
                        rawEventResult.trim()
                );
    }
}