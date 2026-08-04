package com.network.preprocess.silver.event;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Chuẩn hóa EVENT_ID thành lookup key ổn định.
 *
 * <p>Ví dụ:</p>
 *
 * <pre>
 * " L_SERVICE_REQUEST "  -> "l_service_request"
 * "L-Service Request"    -> "l_service_request"
 * "l  service__request"  -> "l_service_request"
 * </pre>
 *
 * <p>Normalizer chỉ tạo lookup key. Nó không quyết định event
 * có được model hỗ trợ hay không. Việc đó thuộc EventCatalog.</p>
 */
public final class EventIdNormalizer {

    /**
     * Mọi chuỗi ký tự không phải chữ hoặc số được thay bằng "_".
     */
    private static final Pattern SEPARATOR_PATTERN =
            Pattern.compile("[^a-z0-9]+");

    /**
     * Loại "_" ở đầu hoặc cuối sau chuẩn hóa.
     */
    private static final Pattern EDGE_UNDERSCORE_PATTERN =
            Pattern.compile("^_+|_+$");

    private EventIdNormalizer() {
    }

    public static Optional<String> normalizeLookupKey(
            String rawEventId
    ) {
        if (rawEventId == null || rawEventId.isBlank()) {
            return Optional.empty();
        }

        String lowerCase = rawEventId
                .trim()
                .toLowerCase(Locale.ROOT);

        String separated = SEPARATOR_PATTERN
                .matcher(lowerCase)
                .replaceAll("_");

        String normalized = EDGE_UNDERSCORE_PATTERN
                .matcher(separated)
                .replaceAll("");

        if (normalized.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(normalized);
    }
}