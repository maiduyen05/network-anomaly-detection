package com.network.preprocess.gold.feature;
import com.network.preprocess.config.GoldJobConfig;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Kiểm thử việc encode các categorical feature của tầng Gold
 */
class GoldCategoricalVocabulariesTest {

    /**
     * Tên file cấu hình được sử dụng cho test.
     *
     * <p>File application.yaml phải nằm trong classpath, thông thường là:
     *
     * <pre>
     * src/main/resources/application.yaml
     * </pre>
     */
    private static final String CONFIG_RESOURCE = "application.yaml";

    /**
     * Lấy CategoricalVocabulary của một feature từ GoldFeatureContract.
     *
     * <p>Quy trình:
     *
     * <ol>
     *     <li>Đọc application.yaml.</li>
     *     <li>Lấy GoldFeatureContract.</li>
     *     <li>Tìm categorical feature theo tên.</li>
     *     <li>Dùng GoldCategoricalVocabularies.fromFeature(...)
     *         để tạo encoder.</li>
     * </ol>
     *
     * @param featureName tên categorical feature,
     *                    ví dụ "event_code"
     *
     * @return vocabulary tương ứng với feature
     *
     * @throws IllegalStateException nếu feature không tồn tại
     *                               trong contract
     */
    private CategoricalVocabulary vocabularyFor(
            String featureName
    ) {

        /*
         * Đọc application.yaml và lấy feature contract
         * đang được Gold Job sử dụng thực tế.
         */
        GoldFeatureContract contract =
                GoldJobConfig
                        .loadFromClasspath(
                                CONFIG_RESOURCE
                        )
                        .featureContract();

        /*
         * Tìm categorical feature theo name.
         *
         * Ví dụ:
         *
         * event_code
         * event_result_code
         * normalized_cause_code
         * sub_cause_code
         */
        GoldFeatureContract.CategoricalFeature feature =
                contract
                        .categoricalFeatures()
                        .stream()
                        .filter(
                                candidate ->
                                        candidate
                                                .name()
                                                .equals(featureName)
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

        /*
         * Tạo vocabulary từ chính feature contract.
         *
         * Từ checkpoint này trở đi đây là cách chuẩn
         * để tạo categorical vocabulary.
         */
        return GoldCategoricalVocabularies
                .fromFeature(feature);
    }

    /**
     * Kiểm tra mapping của event_code.
     *
     * <p>Các ID phải giữ nguyên đúng với feature contract/model contract.
     * Không được tự ý thay đổi mapping nếu model chưa được train lại.
     */
    @Test
    void shouldEncodeEventCodeUsingFixedVocabulary() {

        CategoricalVocabulary vocabulary =
                vocabularyFor("event_code");

        assertEquals(
                1L,
                vocabulary.encode("l_attach")
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
                vocabulary.encode("l_detach")
        );

        assertEquals(
                6L,
                vocabulary.encode("l_handover")
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
                vocabulary.encode("l_tau")
        );
    }

    /**
     * Kiểm tra event_result_code.
     *
     * <p>Vocabulary hiện tại có hai giá trị:
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
                vocabulary.encode("reject")
        );

        assertEquals(
                1L,
                vocabulary.encode("success")
        );
    }

    /**
     * Kiểm tra mapping của normalized_cause_code.
     */
    @Test
    void shouldEncodeNormalizedCauseCodeLexicographically() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "normalized_cause_code"
                );

        assertEquals(
                0L,
                vocabulary.encode("")
        );

        assertEquals(
                1L,
                vocabulary.encode("10")
        );

        assertEquals(
                2L,
                vocabulary.encode("38")
        );

        assertEquals(
                3L,
                vocabulary.encode("9")
        );
    }

    /**
     * Kiểm tra mapping của sub_cause_code.
     */
    @Test
    void shouldEncodeSubCauseCodeLexicographically() {

        CategoricalVocabulary vocabulary =
                vocabularyFor(
                        "sub_cause_code"
                );

        assertEquals(
                0L,
                vocabulary.encode("")
        );

        assertEquals(
                1L,
                vocabulary.encode("107")
        );

        assertEquals(
                2L,
                vocabulary.encode("11")
        );

        assertEquals(
                3L,
                vocabulary.encode("14")
        );

        assertEquals(
                4L,
                vocabulary.encode("403")
        );

        assertEquals(
                5L,
                vocabulary.encode("410")
        );

        assertEquals(
                6L,
                vocabulary.encode("413")
        );
    }

    /**
     * Kiểm tra encoder có normalize:
     *
     * <ul>
     *     <li>Khoảng trắng đầu/cuối.</li>
     *     <li>Chữ hoa/chữ thường.</li>
     * </ul>
     */
    @Test
    void shouldNormalizeWhitespaceAndLetterCase() {

        CategoricalVocabulary eventVocabulary =
                vocabularyFor("event_code");

        CategoricalVocabulary resultVocabulary =
                vocabularyFor(
                        "event_result_code"
                );

        /*
         * Sau normalize:
         *
         * "  L_SERVICE_REQUEST  "
         *
         * trở thành:
         *
         * "l_service_request"
         */
        assertEquals(
                8L,
                eventVocabulary.encode(
                        "  L_SERVICE_REQUEST  "
                )
        );

        /*
         * Sau normalize:
         *
         * " SUCCESS "
         *
         * trở thành:
         *
         * "success"
         */
        assertEquals(
                1L,
                resultVocabulary.encode(
                        " SUCCESS "
                )
        );
    }

    /**
     * Kiểm tra sự khác nhau giữa:
     *
     * <ul>
     *     <li>Blank cause là một category hợp lệ.</li>
     *     <li>Blank event là missing value.</li>
     * </ul>
     */
    @Test
    void shouldTreatBlankCauseAsKnownCategoryButBlankEventAsMissing() {

        CategoricalVocabulary causeVocabulary =
                vocabularyFor(
                        "normalized_cause_code"
                );

        CategoricalVocabulary eventVocabulary =
                vocabularyFor("event_code");

        /*
         * Sau trim:
         *
         * "   " -> ""
         *
         * Cause vocabulary có category "",
         * vì vậy encode thành ID 0.
         */
        assertEquals(
                0L,
                causeVocabulary.encode("   ")
        );

        /*
         * Event vocabulary không có category "".
         *
         * Vì vậy chuỗi blank được coi là missing
         * và phải ném GoldFeatureEncodingException.
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
                        .Reason.MISSING_VALUE,
                exception.getReason()
        );
    }

    /**
     * Kiểm tra hai loại dữ liệu không hợp lệ:
     *
     * <ol>
     *     <li>null -> MISSING_VALUE.</li>
     *     <li>Category không tồn tại -> UNKNOWN_CATEGORY.</li>
     * </ol>
     */
    @Test
    void shouldRejectNullAndUnknownCategory() {

        CategoricalVocabulary vocabulary =
                vocabularyFor("event_code");

        /*
         * null nghĩa là feature không có giá trị.
         */
        GoldFeatureEncodingException missing =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                vocabulary.encode(null)
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.MISSING_VALUE,
                missing.getReason()
        );

        /*
         * Giá trị tồn tại nhưng không nằm trong vocabulary.
         */
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
                        .Reason.UNKNOWN_CATEGORY,
                unknown.getReason()
        );
    }

    /**
     * Kiểm tra getCategoryToId() trả về defensive copy.
     *
     * <p>Code bên ngoài có thể sửa Map được trả về,
     * nhưng việc đó không được làm thay đổi vocabulary
     * thực sự bên trong encoder.
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
         * Thử sửa Map mà caller nhận được.
         */
        returnedMapping.put(
                "new_value",
                99L
        );

        /*
         * Vocabulary thực phải vẫn chỉ chứa:
         *
         * reject
         * success
         */
        assertEquals(
                2,
                vocabulary.size()
        );

        /*
         * "new_value" không được trở thành
         * một category hợp lệ chỉ vì caller
         * đã sửa returnedMapping.
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
                        .Reason.UNKNOWN_CATEGORY,
                exception.getReason()
        );
    }
}