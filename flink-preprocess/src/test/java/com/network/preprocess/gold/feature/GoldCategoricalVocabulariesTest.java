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
 * Vocabulary không còn được hard-code trong Java.
 * Test đọc chính GoldFeatureContract đang được runtime sử dụng.
 * </p>
 */
class GoldCategoricalVocabulariesTest {

    private static final String CONFIG_RESOURCE =
            "application.yaml";


    /**
     * EVENT_ID phải giữ đúng ID của model contract v1.
     */
    @Test
    void shouldEncodeEventCodeUsingFixedVocabulary() {

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
                        "l_service_request"
                )
        );

        assertEquals(
                9L,
                vocabulary.encode(
                        "l_tau"
                )
        );
    }


    /**
     * EVENT_RESULT hiện có hai category:
     *
     * <pre>
     * reject  -> 0
     * success -> 1
     * </pre>
     */
    @Test
    void shouldEncodeTwoEventResultsFromZero() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "event_result_code"
                );

        assertEquals(
                0L,
                vocabulary.encode(
                        "reject"
                )
        );

        assertEquals(
                1L,
                vocabulary.encode(
                        "success"
                )
        );
    }


    /**
     * Kiểm tra mapping CAUSE_CODE.
     *
     * <p>
     * Empty string là category hợp lệ.
     * </p>
     */
    @Test
    void shouldEncodeNormalizedCauseCodeLexicographically() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "normalized_cause_code"
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
                        "10"
                )
        );

        assertEquals(
                2L,
                vocabulary.encode(
                        "38"
                )
        );

        assertEquals(
                3L,
                vocabulary.encode(
                        "9"
                )
        );
    }


    /**
     * Kiểm tra mapping SUB_CAUSE_CODE.
     */
    @Test
    void shouldEncodeSubCauseCodeLexicographically() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "sub_cause_code"
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
                        "107"
                )
        );

        assertEquals(
                2L,
                vocabulary.encode(
                        "11"
                )
        );

        assertEquals(
                3L,
                vocabulary.encode(
                        "14"
                )
        );

        assertEquals(
                4L,
                vocabulary.encode(
                        "403"
                )
        );

        assertEquals(
                5L,
                vocabulary.encode(
                        "410"
                )
        );

        assertEquals(
                6L,
                vocabulary.encode(
                        "413"
                )
        );
    }


    /**
     * Vocabulary runtime hiện normalize:
     *
     * <ul>
     *     <li>trim whitespace;</li>
     *     <li>lowercase.</li>
     * </ul>
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

        assertEquals(
                8L,
                eventVocabulary.encode(
                        "  L_SERVICE_REQUEST  "
                )
        );

        assertEquals(
                1L,
                resultVocabulary.encode(
                        " SUCCESS "
                )
        );
    }


    /**
     * Blank cause là category hợp lệ,
     * nhưng blank EVENT_ID là missing.
     */
    @Test
    void shouldTreatBlankCauseAsKnownCategoryButBlankEventAsMissing() {

        CategoricalVocabulary causeVocabulary =
                vocabularyFor(
                        "normalized_cause_code"
                );

        CategoricalVocabulary eventVocabulary =
                vocabularyFor(
                        "event_code"
                );

        /*
         * "   " sau trim thành "".
         *
         * CAUSE_CODE vocabulary có "" -> 0.
         */
        assertEquals(
                0L,
                causeVocabulary.encode(
                        "   "
                )
        );

        /*
         * EVENT_ID không có category "".
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
     * null            -> MISSING_VALUE
     * unknown category -> UNKNOWN_CATEGORY
     * </pre>
     */
    @Test
    void shouldRejectNullAndUnknownCategory() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "event_code"
                );

        GoldFeatureEncodingException missing =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                vocabulary.encode(
                                        null
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .MISSING_VALUE,
                missing.getReason()
        );


        GoldFeatureEncodingException unknown =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                vocabulary.encode(
                                        "l_unknown_event"
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .UNKNOWN_CATEGORY,
                unknown.getReason()
        );
    }


    /**
     * Map trả về từ getCategoryToId()
     * phải là defensive copy.
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
         * Vocabulary thật vẫn chỉ có:
         *
         * reject
         * success
         */
        assertEquals(
                2,
                vocabulary.size()
        );

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
     * Tìm categorical feature trong contract
     * rồi tạo vocabulary runtime từ feature đó.
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