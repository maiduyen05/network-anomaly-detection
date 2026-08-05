package com.network.preprocess.silver.event;

import com.network.preprocess.model.EventResult;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Chuẩn hóa EVENT_RESULT theo đúng feature contract.
 *
 * <p>Chỉ hai category được công nhận:</p>
 *
 * <ul>
 *     <li>reject</li>
 *     <li>success</li>
 * </ul>
 *
 * <p>Không tự chuyển failure hoặc timeout thành reject.
 * Việc làm đó sẽ thay đổi ý nghĩa dữ liệu mà chưa có
 * tài liệu training xác nhận.</p>
 */
public final class EventResultNormalizer {

    private EventResultNormalizer() {
        // Utility class không cần tạo object.
    }

    /**
     * Chuẩn hóa EVENT_RESULT.
     *
     * @return Optional.empty nếu thiếu hoặc không thuộc contract
     */
    public static Optional<EventResult> normalize(
            String rawEventResult
    ) {
        if (rawEventResult == null
                || rawEventResult.isBlank()) {

            return Optional.empty();
        }

        String lookupKey =
                rawEventResult
                        .trim()
                        .toLowerCase(Locale.ROOT);

        return switch (lookupKey) {
            case "reject" ->
                    Optional.of(EventResult.REJECT);

            case "success" ->
                    Optional.of(EventResult.SUCCESS);

            default ->
                    Optional.empty();
        };
    }

    /**
     * Kiểm tra raw value có bị thay đổi cách biểu diễn không.
     *
     * <p>Ví dụ:</p>
     *
     * <pre>
     * "success"   → false
     * " SUCCESS " → true
     * </pre>
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
                .equals(rawEventResult.trim());
    }
}