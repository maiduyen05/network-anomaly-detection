package com.network.preprocess.gold.feature;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Vocabulary cố định của một categorical feature.
 *
 * <p>Class này chỉ thực hiện lookup:</p>
 *
 * <pre>
 * category đã biết → ID cố định
 * </pre>
 *
 * <p>Vocabulary tuyệt đối không được bổ sung category mới
 * trong lúc Flink đang xử lý Kafka.</p>
 */
public final class CategoricalVocabulary
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String featureName;

    /*
     * Dùng LinkedHashMap có thể thay đổi thay vì Collections.unmodifiableMap.
     *
     * Việc này tránh lỗi Kryo copy từng gặp với UnmodifiableMap
     * trong Flink test harness.
     *
     * Map vẫn được bảo vệ vì không trả trực tiếp ra ngoài.
     */
    private final LinkedHashMap<String, Long> categoryToId;

    CategoricalVocabulary(
            String featureName,
            Map<String, Long> configuredMapping
    ) {
        if (featureName == null || featureName.isBlank()) {
            throw new IllegalArgumentException(
                    "featureName must not be blank"
            );
        }

        Objects.requireNonNull(
                configuredMapping,
                "configuredMapping must not be null"
        );

        if (configuredMapping.isEmpty()) {
            throw invalidVocabulary(
                    featureName,
                    null,
                    "Vocabulary must not be empty"
            );
        }

        this.featureName = featureName.trim();
        this.categoryToId = new LinkedHashMap<>();

        /*
         * Kiểm tra để không có hai category dùng cùng một ID.
         */
        Set<Long> usedIds = new HashSet<>();

        for (Map.Entry<String, Long> entry
                : configuredMapping.entrySet()) {

            String rawCategory = entry.getKey();
            Long categoryId = entry.getValue();

            if (rawCategory == null) {
                throw invalidVocabulary(
                        featureName,
                        null,
                        "Vocabulary contains a null category"
                );
            }

            if (categoryId == null || categoryId < 0L) {
                throw invalidVocabulary(
                        featureName,
                        rawCategory,
                        "Category ID must be a non-negative integer"
                );
            }

            /*
             * Chuỗi rỗng vẫn được giữ lại.
             *
             * Đây là yêu cầu quan trọng cho CAUSE_CODE
             * và SUB_CAUSE_CODE.
             */
            String normalizedCategory = normalize(rawCategory);

            if (categoryToId.containsKey(normalizedCategory)) {
                throw invalidVocabulary(
                        featureName,
                        rawCategory,
                        "Duplicated category after normalization"
                );
            }

            if (!usedIds.add(categoryId)) {
                throw invalidVocabulary(
                        featureName,
                        rawCategory,
                        "Duplicated category ID: " + categoryId
                );
            }

            categoryToId.put(normalizedCategory, categoryId);
        }
    }

    /**
     * Chuyển category đầu vào thành ID cố định.
     *
     * <p>Quy tắc:</p>
     *
     * <ul>
     *     <li>null luôn được xem là missing.</li>
     *     <li>Chuỗi rỗng hợp lệ nếu vocabulary có key "".</li>
     *     <li>Chuỗi rỗng là missing nếu vocabulary không có key "".</li>
     *     <li>Category khác nhưng không có trong vocabulary là unknown.</li>
     * </ul>
     */
    public long encode(String rawValue) {
        if (rawValue == null) {
            throw missingValue(rawValue);
        }

        String normalizedValue = normalize(rawValue);

        /*
         * Ví dụ:
         *
         * eventId = "   ":
         *     event_code không có category ""
         *     → MISSING_VALUE
         *
         * CAUSE_CODE = "   ":
         *     normalized_cause_code có category ""
         *     → trả về ID 1
         */
        if (normalizedValue.isEmpty()
                && !categoryToId.containsKey("")) {

            throw missingValue(rawValue);
        }

        Long categoryId = categoryToId.get(normalizedValue);

        if (categoryId == null) {
            throw new GoldFeatureEncodingException(
                    featureName,
                    GoldFeatureEncodingException.Reason.UNKNOWN_CATEGORY,
                    rawValue,
                    "Unknown category '"
                            + rawValue
                            + "' for feature '"
                            + featureName
                            + "'"
            );
        }

        return categoryId;
    }

    /**
     * Chuẩn hóa chuỗi giống nhau giữa lúc tạo vocabulary
     * và lúc xử lý dữ liệu.
     */
    public static String normalize(String rawValue) {
        return rawValue
                .trim()
                .toLowerCase(Locale.ROOT);
    }

    public String getFeatureName() {
        return featureName;
    }

    /**
     * Trả về bản sao để code bên ngoài không sửa được
     * vocabulary mà encoder đang sử dụng.
     */
    public Map<String, Long> getCategoryToId() {
        return new LinkedHashMap<>(categoryToId);
    }

    public int size() {
        return categoryToId.size();
    }

    private GoldFeatureEncodingException missingValue(
            String rawValue
    ) {
        return new GoldFeatureEncodingException(
                featureName,
                GoldFeatureEncodingException.Reason.MISSING_VALUE,
                rawValue,
                "Feature '" + featureName + "' is missing"
        );
    }

    private static GoldFeatureEncodingException invalidVocabulary(
            String featureName,
            String rejectedValue,
            String message
    ) {
        return new GoldFeatureEncodingException(
                featureName,
                GoldFeatureEncodingException.Reason.INVALID_VOCABULARY,
                rejectedValue,
                message
        );
    }
}