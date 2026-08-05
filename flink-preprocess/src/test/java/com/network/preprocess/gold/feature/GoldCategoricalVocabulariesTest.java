package com.network.preprocess.gold.feature;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GoldCategoricalVocabulariesTest {

    @Test
    void shouldEncodeEventCodeUsingFixedVocabulary() {
        CategoricalVocabulary vocabulary =
                GoldCategoricalVocabularies.eventCode();

        assertEquals(1L, vocabulary.encode("l_attach"));
        assertEquals(2L, vocabulary.encode("l_bearer_modify"));

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

        assertEquals(5L, vocabulary.encode("l_detach"));
        assertEquals(6L, vocabulary.encode("l_handover"));
        assertEquals(7L, vocabulary.encode("l_pdn_connect"));
        assertEquals(8L, vocabulary.encode("l_service_request"));
        assertEquals(9L, vocabulary.encode("l_tau"));
    }

    @Test
    void shouldEncodeTwoEventResultsFromZero() {
        CategoricalVocabulary vocabulary =
                GoldCategoricalVocabularies.eventResultCode();

        assertEquals(0L, vocabulary.encode("reject"));
        assertEquals(1L, vocabulary.encode("success"));
    }

    @Test
    void shouldEncodeNormalizedCauseCodeLexicographically() {
        CategoricalVocabulary vocabulary =
                GoldCategoricalVocabularies.normalizedCauseCode();

        assertEquals(1L, vocabulary.encode(""));
        assertEquals(2L, vocabulary.encode("10"));
        assertEquals(3L, vocabulary.encode("38"));
        assertEquals(4L, vocabulary.encode("9"));
    }

    @Test
    void shouldEncodeSubCauseCodeLexicographically() {
        CategoricalVocabulary vocabulary =
                GoldCategoricalVocabularies.subCauseCode();

        assertEquals(1L, vocabulary.encode(""));
        assertEquals(2L, vocabulary.encode("107"));
        assertEquals(3L, vocabulary.encode("11"));
        assertEquals(4L, vocabulary.encode("14"));
        assertEquals(5L, vocabulary.encode("403"));
        assertEquals(6L, vocabulary.encode("410"));
        assertEquals(7L, vocabulary.encode("413"));
    }

    @Test
    void shouldNormalizeWhitespaceAndLetterCase() {
        CategoricalVocabulary eventVocabulary =
                GoldCategoricalVocabularies.eventCode();

        CategoricalVocabulary resultVocabulary =
                GoldCategoricalVocabularies.eventResultCode();

        assertEquals(
                8L,
                eventVocabulary.encode(
                        "  L_SERVICE_REQUEST  "
                )
        );

        assertEquals(
                1L,
                resultVocabulary.encode(" SUCCESS ")
        );
    }

    @Test
    void shouldTreatBlankCauseAsKnownCategoryButBlankEventAsMissing() {
        CategoricalVocabulary causeVocabulary =
                GoldCategoricalVocabularies.normalizedCauseCode();

        CategoricalVocabulary eventVocabulary =
                GoldCategoricalVocabularies.eventCode();

        /*
         * Sau trim, chuỗi chỉ có khoảng trắng trở thành "".
         * Cause vocabulary có "" nên kết quả là ID 1.
         */
        assertEquals(
                1L,
                causeVocabulary.encode("   ")
        );

        /*
         * Event vocabulary không có "" nên đây là missing.
         */
        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () -> eventVocabulary.encode("   ")
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.MISSING_VALUE,
                exception.getReason()
        );
    }

    @Test
    void shouldRejectNullAndUnknownCategory() {
        CategoricalVocabulary vocabulary =
                GoldCategoricalVocabularies.eventCode();

        GoldFeatureEncodingException missing =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () -> vocabulary.encode(null)
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.MISSING_VALUE,
                missing.getReason()
        );

        GoldFeatureEncodingException unknown =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () -> vocabulary.encode(
                                "l_unknown_event"
                        )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.UNKNOWN_CATEGORY,
                unknown.getReason()
        );
    }

    @Test
    void shouldExposeDefensiveCopyOfMapping() {
        CategoricalVocabulary vocabulary =
                GoldCategoricalVocabularies.eventResultCode();

        Map<String, Long> returnedMapping =
                vocabulary.getCategoryToId();

        returnedMapping.put("new_value", 99L);

        /*
         * Việc sửa map được trả về không được làm thay đổi
         * vocabulary bên trong encoder.
         */
        assertEquals(2, vocabulary.size());

        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () -> vocabulary.encode("new_value")
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.UNKNOWN_CATEGORY,
                exception.getReason()
        );
    }
}