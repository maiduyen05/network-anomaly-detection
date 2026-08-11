package com.network.preprocess.gold.feature;

import com.network.preprocess.config.GoldFeatureContract;
import com.network.preprocess.config.GoldJobConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Kiểm thử categorical vocabulary của tầng Gold.
 *
 * <p>
 * Vocabulary không hard-code trong encoder.
 * Test đọc chính GoldFeatureContract đang được runtime sử dụng
 * từ application.yaml.
 * </p>
 *
 * <p>
 * Contract hiện tại:
 * </p>
 *
 * <pre>
 * gold-ue-sequence-feature-v2
 *
 * x_cat[:, 0] = event_code
 * x_cat[:, 1] = event_result_code
 * x_cat[:, 2] = normalized_cause_code
 * x_cat[:, 3] = sub_cause_code
 * </pre>
 */
class GoldCategoricalVocabulariesTest {

    private static final String CONFIG_RESOURCE =
            "application.yaml";


    /**
     * EVENT_ID phải giữ đúng fixed vocabulary
     * của feature contract v2.
     *
     * <pre>
     * l_attach                       -> 1
     * l_bearer_modify                -> 2
     * l_dedicated_bearer_activate    -> 3
     * l_dedicated_bearer_deactivate  -> 4
     * l_detach                       -> 5
     * l_handover                     -> 6
     * l_pdn_connect                  -> 7
     * l_pdn_disconnect               -> 8
     * l_service_request              -> 9
     * l_tau                          -> 10
     * </pre>
     */
    @Test
    void shouldEncodeEventCodeUsingV2Vocabulary() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "event_code"
                );

        assertEquals(
                1L,
                vocabulary.encode(
                        "l_attach"
                )
        );

        assertEquals(
                2L,
                vocabulary.encode(
                        "l_bearer_modify"
                )
        );

        assertEquals(
                3L,
                vocabulary.encode(
                        "l_dedicated_bearer_activate"
                )
        );

        assertEquals(
                4L,
                vocabulary.encode(
                        "l_dedicated_bearer_deactivate"
                )
        );

        assertEquals(
                5L,
                vocabulary.encode(
                        "l_detach"
                )
        );

        assertEquals(
                6L,
                vocabulary.encode(
                        "l_handover"
                )
        );

        assertEquals(
                7L,
                vocabulary.encode(
                        "l_pdn_connect"
                )
        );

        assertEquals(
                8L,
                vocabulary.encode(
                        "l_pdn_disconnect"
                )
        );

        assertEquals(
                9L,
                vocabulary.encode(
                        "l_service_request"
                )
        );

        assertEquals(
                10L,
                vocabulary.encode(
                        "l_tau"
                )
        );

        assertEquals(
                10,
                vocabulary.size()
        );
    }


    /**
     * EVENT_RESULT của contract v2 có bốn category.
     *
     * <pre>
     * ""      -> 0
     * abort   -> 1
     * reject  -> 2
     * success -> 3
     * </pre>
     *
     * <p>
     * Empty string là category explicit.
     * Null vẫn là missing.
     * </p>
     */
    @Test
    void shouldEncodeEventResultsUsingV2Vocabulary() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "event_result_code"
                );

        assertEquals(
                0L,
                vocabulary.encode(
                        ""
                )
        );

        assertEquals(
                1L,
                vocabulary.encode(
                        "abort"
                )
        );

        assertEquals(
                2L,
                vocabulary.encode(
                        "reject"
                )
        );

        assertEquals(
                3L,
                vocabulary.encode(
                        "success"
                )
        );

        assertEquals(
                4,
                vocabulary.size()
        );
    }


    /**
     * Kiểm tra toàn bộ mapping CAUSE_CODE của contract v2.
     *
     * <p>
     * Empty string là category hợp lệ và được map về 0.
     * Các category còn lại sử dụng fixed ID đã được freeze
     * trong feature contract v2.
     * </p>
     */
    @Test
    void shouldEncodeNormalizedCauseCodeUsingV2Vocabulary() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "normalized_cause_code"
                );

        Map<String, Long> expected =
                Map.ofEntries(
                        Map.entry("", 0L),
                        Map.entry("0", 1L),
                        Map.entry("10", 2L),
                        Map.entry("11", 3L),
                        Map.entry("13", 4L),
                        Map.entry("15", 5L),
                        Map.entry("16", 6L),
                        Map.entry("17", 7L),
                        Map.entry("26", 8L),
                        Map.entry("3", 9L),
                        Map.entry("30", 10L),
                        Map.entry("31", 11L),
                        Map.entry("32", 12L),
                        Map.entry("34", 13L),
                        Map.entry("38", 14L),
                        Map.entry("39", 15L),
                        Map.entry("40", 16L),
                        Map.entry("43", 17L),
                        Map.entry("53", 18L),
                        Map.entry("6", 19L),
                        Map.entry("65", 20L),
                        Map.entry("7", 21L),
                        Map.entry("73", 22L),
                        Map.entry("87", 23L),
                        Map.entry("88", 24L),
                        Map.entry("9", 25L),
                        Map.entry("unspecified", 26L)
                );

        assertEquals(
                expected,
                vocabulary.getCategoryToId()
        );

        assertEquals(
                27,
                vocabulary.size()
        );
    }


    /**
     * Kiểm tra toàn bộ mapping SUB_CAUSE_CODE
     * của feature contract v2.
     *
     * <p>
     * Empty string = 0.
     * Các category còn lại giữ fixed ID của contract v2.
     * </p>
     */
    @Test
    void shouldEncodeSubCauseCodeUsingV2Vocabulary() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "sub_cause_code"
                );

        Map<String, Long> expected =
                Map.ofEntries(
                        Map.entry("", 0L),
                        Map.entry("1", 1L),
                        Map.entry("102", 2L),
                        Map.entry("104", 3L),
                        Map.entry("107", 4L),
                        Map.entry("108", 5L),
                        Map.entry("109", 6L),
                        Map.entry("11", 7L),
                        Map.entry("12", 8L),
                        Map.entry("13", 9L),
                        Map.entry("14", 10L),
                        Map.entry("15", 11L),
                        Map.entry("16", 12L),
                        Map.entry("17", 13L),
                        Map.entry("2", 14L),
                        Map.entry("204", 15L),
                        Map.entry("3", 16L),
                        Map.entry("304", 17L),
                        Map.entry("306", 18L),
                        Map.entry("308", 19L),
                        Map.entry("311", 20L),
                        Map.entry("312", 21L),
                        Map.entry("315", 22L),
                        Map.entry("316", 23L),
                        Map.entry("317", 24L),
                        Map.entry("318", 25L),
                        Map.entry("403", 26L),
                        Map.entry("405", 27L),
                        Map.entry("410", 28L),
                        Map.entry("413", 29L),
                        Map.entry("419", 30L),
                        Map.entry("420", 31L),
                        Map.entry("422", 32L),
                        Map.entry("423", 33L),
                        Map.entry("424", 34L),
                        Map.entry("427", 35L),
                        Map.entry("428", 36L),
                        Map.entry("429", 37L),
                        Map.entry("430", 38L),
                        Map.entry("435", 39L),
                        Map.entry("440", 40L),
                        Map.entry("441", 41L),
                        Map.entry("5", 42L),
                        Map.entry("503", 43L),
                        Map.entry("504", 44L),
                        Map.entry("505", 45L),
                        Map.entry("506", 46L),
                        Map.entry("507", 47L),
                        Map.entry("509", 48L),
                        Map.entry("603", 49L),
                        Map.entry("605", 50L),
                        Map.entry("606", 51L),
                        Map.entry("607", 52L),
                        Map.entry("608", 53L),
                        Map.entry("610", 54L),
                        Map.entry("611", 55L),
                        Map.entry("7", 56L),
                        Map.entry("704", 57L),
                        Map.entry("706", 58L),
                        Map.entry("709", 59L),
                        Map.entry("710", 60L),
                        Map.entry("8", 61L),
                        Map.entry("9", 62L)
                );

        assertEquals(
                expected,
                vocabulary.getCategoryToId()
        );

        assertEquals(
                63,
                vocabulary.size()
        );
    }


    /**
     * Vocabulary runtime normalize categorical value bằng:
     *
     * <ul>
     *     <li>trim whitespace;</li>
     *     <li>lowercase.</li>
     * </ul>
     *
     * <p>
     * Normalization không được thay đổi ID của category.
     * </p>
     */
    @Test
    void shouldNormalizeWhitespaceAndLetterCase() {

        CategoricalVocabulary eventVocabulary =
                vocabularyFor(
                        "event_code"
                );

        CategoricalVocabulary resultVocabulary =
                vocabularyFor(
                        "event_result_code"
                );

        CategoricalVocabulary causeVocabulary =
                vocabularyFor(
                        "normalized_cause_code"
                );

        assertEquals(
                9L,
                eventVocabulary.encode(
                        "  L_SERVICE_REQUEST  "
                )
        );

        assertEquals(
                3L,
                resultVocabulary.encode(
                        " SUCCESS "
                )
        );

        assertEquals(
                1L,
                resultVocabulary.encode(
                        " ABORT "
                )
        );

        assertEquals(
                26L,
                causeVocabulary.encode(
                        " UNSPECIFIED "
                )
        );
    }


    /**
     * Empty string có ý nghĩa khác nhau tùy feature.
     *
     * <p>
     * Contract v2:
     * </p>
     *
     * <pre>
     * EVENT_ID      "" -> missing
     * EVENT_RESULT  "" -> valid category 0
     * CAUSE_CODE    "" -> valid category 0
     * SUB_CAUSE     "" -> valid category 0
     * </pre>
     */
    @Test
    void shouldTreatBlankAccordingToEachFeatureVocabulary() {

        CategoricalVocabulary eventVocabulary =
                vocabularyFor(
                        "event_code"
                );

        CategoricalVocabulary resultVocabulary =
                vocabularyFor(
                        "event_result_code"
                );

        CategoricalVocabulary causeVocabulary =
                vocabularyFor(
                        "normalized_cause_code"
                );

        CategoricalVocabulary subCauseVocabulary =
                vocabularyFor(
                        "sub_cause_code"
                );


        /*
         * Whitespace sau trim thành empty string.
         *
         * EVENT_RESULT có "" trong vocabulary.
         */
        assertEquals(
                0L,
                resultVocabulary.encode(
                        "   "
                )
        );


        /*
         * CAUSE_CODE có "" trong vocabulary.
         */
        assertEquals(
                0L,
                causeVocabulary.encode(
                        "   "
                )
        );


        /*
         * SUB_CAUSE_CODE có "" trong vocabulary.
         */
        assertEquals(
                0L,
                subCauseVocabulary.encode(
                        "   "
                )
        );


        /*
         * EVENT_ID không có "" trong vocabulary
         * nên blank EVENT_ID là MISSING_VALUE.
         */
        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                eventVocabulary.encode(
                                        "   "
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .MISSING_VALUE,
                exception.getReason()
        );
    }


    /**
     * Phân biệt:
     *
     * <pre>
     * null             -> MISSING_VALUE
     * unknown category -> UNKNOWN_CATEGORY
     * </pre>
     *
     * <p>
     * Ngay cả feature có empty-string category thì null
     * vẫn không được xem là empty string.
     * </p>
     */
    @Test
    void shouldRejectNullAndUnknownCategory() {

        CategoricalVocabulary eventVocabulary =
                vocabularyFor(
                        "event_code"
                );

        CategoricalVocabulary resultVocabulary =
                vocabularyFor(
                        "event_result_code"
                );


        /*
         * Null EVENT_ID -> missing.
         */
        GoldFeatureEncodingException missingEvent =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                eventVocabulary.encode(
                                        null
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .MISSING_VALUE,
                missingEvent.getReason()
        );


        /*
         * Null EVENT_RESULT vẫn là missing,
         * mặc dù "" là category hợp lệ.
         */
        GoldFeatureEncodingException missingResult =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                resultVocabulary.encode(
                                        null
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .MISSING_VALUE,
                missingResult.getReason()
        );


        /*
         * EVENT_ID không tồn tại trong vocabulary -> unknown.
         */
        GoldFeatureEncodingException unknownEvent =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                eventVocabulary.encode(
                                        "l_unknown_event"
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .UNKNOWN_CATEGORY,
                unknownEvent.getReason()
        );


        /*
         * EVENT_RESULT không tồn tại trong vocabulary -> unknown.
         */
        GoldFeatureEncodingException unknownResult =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                resultVocabulary.encode(
                                        "failure"
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .UNKNOWN_CATEGORY,
                unknownResult.getReason()
        );
    }


    /**
     * Map trả về từ getCategoryToId()
     * phải là defensive copy.
     *
     * <p>
     * Caller sửa Map trả về không được làm thay đổi
     * vocabulary đang được encoder sử dụng.
     * </p>
     */
    @Test
    void shouldExposeDefensiveCopyOfMapping() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "event_result_code"
                );

        Map<String, Long> returnedMapping =
                vocabulary.getCategoryToId();


        /*
         * Caller sửa returned Map.
         */
        returnedMapping.put(
                "new_value",
                99L
        );


        /*
         * Vocabulary thật của EVENT_RESULT v2 vẫn chỉ có:
         *
         * ""
         * abort
         * reject
         * success
         */
        assertEquals(
                4,
                vocabulary.size()
        );


        /*
         * Category được thêm vào defensive copy
         * không được xuất hiện trong vocabulary thật.
         */
        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                vocabulary.encode(
                                        "new_value"
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .UNKNOWN_CATEGORY,
                exception.getReason()
        );
    }


    /**
     * Tìm categorical feature trong GoldFeatureContract
     * rồi tạo vocabulary runtime từ feature đó.
     *
     * <p>
     * Test sử dụng đúng application.yaml production để tránh:
     * </p>
     *
     * <pre>
     * test vocabulary != runtime vocabulary
     * </pre>
     */
    private static CategoricalVocabulary vocabularyFor(
            String featureName
    ) {

        GoldFeatureContract contract =
                GoldJobConfig
                        .loadFromClasspath(
                                CONFIG_RESOURCE
                        )
                        .featureContract();

        GoldFeatureContract.CategoricalFeature feature =
                contract
                        .categoricalFeatures()
                        .stream()
                        .filter(
                                candidate ->
                                        candidate
                                                .name()
                                                .equals(
                                                        featureName
                                                )
                        )
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Categorical feature "
                                                        + "not found: "
                                                        + featureName
                                        )
                        );

        return GoldCategoricalVocabularies
                .fromFeature(
                        feature
                );
    }
}