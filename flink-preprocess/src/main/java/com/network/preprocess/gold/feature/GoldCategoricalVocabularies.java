package com.network.preprocess.gold.feature;

import java.util.LinkedHashMap;

/**
 * Nơi khai báo duy nhất cho toàn bộ categorical vocabulary
 * của feature contract gold-ue-sequence-feature-v1.
 *
 * <p>Không đặt mapping rải rác trong ProcessFunction,
 * mapper hoặc test vì rất dễ tạo ra ID không đồng nhất.</p>
 */
public final class GoldCategoricalVocabularies {

    private GoldCategoricalVocabularies() {
        // Utility class không cần tạo object.
    }

    /**
     * x_cat[:, 0] — event_code.
     */
    public static CategoricalVocabulary eventCode() {
        LinkedHashMap<String, Long> mapping =
                new LinkedHashMap<>();

        mapping.put("l_attach", 1L);
        mapping.put("l_bearer_modify", 2L);
        mapping.put("l_dedicated_bearer_activate", 3L);
        mapping.put("l_dedicated_bearer_deactivate", 4L);
        mapping.put("l_detach", 5L);
        mapping.put("l_handover", 6L);
        mapping.put("l_pdn_connect", 7L);
        mapping.put("l_service_request", 8L);
        mapping.put("l_tau", 9L);

        return new CategoricalVocabulary(
                "event_code",
                mapping
        );
    }

    /**
     * x_cat[:, 1] — event_result_code.
     *
     * <p>Có đúng hai category nên ID bắt đầu từ 0.</p>
     */
    public static CategoricalVocabulary eventResultCode() {
        LinkedHashMap<String, Long> mapping =
                new LinkedHashMap<>();

        mapping.put("reject", 0L);
        mapping.put("success", 1L);

        return new CategoricalVocabulary(
                "event_result_code",
                mapping
        );
    }

    /**
     * x_cat[:, 2] — normalized_cause_code.
     *
     * <p>Chuỗi rỗng là category thật và có ID 1.</p>
     */
    public static CategoricalVocabulary normalizedCauseCode() {
        LinkedHashMap<String, Long> mapping =
                new LinkedHashMap<>();

        mapping.put("", 1L);
        mapping.put("10", 2L);
        mapping.put("38", 3L);
        mapping.put("9", 4L);

        return new CategoricalVocabulary(
                "normalized_cause_code",
                mapping
        );
    }

    /**
     * x_cat[:, 3] — sub_cause_code.
     *
     * <p>Các giá trị được sắp theo thứ tự từ điển,
     * không phải thứ tự số.</p>
     */
    public static CategoricalVocabulary subCauseCode() {
        LinkedHashMap<String, Long> mapping =
                new LinkedHashMap<>();

        mapping.put("", 1L);
        mapping.put("107", 2L);
        mapping.put("11", 3L);
        mapping.put("14", 4L);
        mapping.put("403", 5L);
        mapping.put("410", 6L);
        mapping.put("413", 7L);

        return new CategoricalVocabulary(
                "sub_cause_code",
                mapping
        );
    }
}