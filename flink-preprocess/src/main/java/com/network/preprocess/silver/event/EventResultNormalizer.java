package com.network.preprocess.silver.event;

import com.network.preprocess.model.EventResult;

import java.util.Locale;

/**
 * Chuẩn hóa EVENT_RESULT.
 *
 * <p>Checkpoint này chỉ nhận diện những giá trị đã có contract rõ ràng:</p>
 *
 * <ul>
 *     <li>success</li>
 *     <li>failure</li>
 *     <li>timeout</li>
 *     <li>unknown</li>
 * </ul>
 *
 * <p>Không tự suy đoán rằng "0" là success hoặc "1" là failure
 * nếu chưa có tài liệu chính thức của nguồn dữ liệu.</p>
 */
public final class EventResultNormalizer {

    private EventResultNormalizer() {
    }

    public static EventResultNormalization normalize(
            String rawEventResult
    ) {
        if (rawEventResult == null
                || rawEventResult.isBlank()) {

            return new EventResultNormalization(
                    EventResult.UNKNOWN,
                    false,
                    true
            );
        }

        String trimmed = rawEventResult.trim();

        String lookupKey = trimmed
                .toLowerCase(Locale.ROOT)
                .replace('-', '_')
                .replace(' ', '_');

        EventResult normalized;
        boolean recognized;

        switch (lookupKey) {
            case "success" -> {
                normalized = EventResult.SUCCESS;
                recognized = true;
            }

            case "failure" -> {
                normalized = EventResult.FAILURE;
                recognized = true;
            }

            case "timeout" -> {
                normalized = EventResult.TIMEOUT;
                recognized = true;
            }

            case "unknown" -> {
                normalized = EventResult.UNKNOWN;
                recognized = true;
            }

            default -> {
                normalized = EventResult.UNKNOWN;
                recognized = false;
            }
        }

        boolean changed =
                !normalized.wireValue().equals(trimmed);

        return new EventResultNormalization(
                normalized,
                recognized,
                changed
        );
    }
}