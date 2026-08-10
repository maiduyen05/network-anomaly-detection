package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kiểm thử GoldFeatureContract.
 *
 * <p>
 * Checkpoint 2C có nhiệm vụ bảo vệ contract giữa:
 * </p>
 *
 * <pre>
 * application.yaml
 *        ↓
 * Gold preprocessing
 *        ↓
 * trained model
 * </pre>
 *
 * <p>
 * Khi feature-version vẫn là:
 * </p>
 *
 * <pre>
 * gold-ue-sequence-feature-v1
 * </pre>
 *
 * thì mọi thành phần ảnh hưởng đến model input phải giữ nguyên.
 */
class GoldFeatureContractTest {

    /**
     * Contract v1 hợp lệ phải load được toàn bộ
     * sequence/categorical/numeric configuration.
     */
    @Test
    void shouldLoadCurrentGoldFeatureContract()
            throws Exception {

        GoldFeatureContract contract =
                parseContract(
                        validContract()
                );


        /*
         * =========================================================
         * FEATURE VERSION
         * =========================================================
         */

        assertEquals(
                "gold-ue-sequence-feature-v1",
                contract.featureVersion()
        );


        /*
         * =========================================================
         * SEQUENCE
         * =========================================================
         */

        assertEquals(
                32,
                contract.sequenceLength()
        );

        assertEquals(
                8,
                contract.sequenceStride()
        );

        assertEquals(
                "LEFT",
                contract.paddingSide()
        );

        assertFalse(
                contract.emitPartialWindows()
        );


        /*
         * =========================================================
         * CATEGORICAL GLOBAL CONTRACT
         * =========================================================
         */

        assertEquals(
                "INT64",
                contract.categoricalDtype()
        );

        assertEquals(
                4,
                contract.categoricalFeatureCount()
        );

        assertTrue(
                contract.categoricalTrim()
        );

        assertTrue(
                contract.categoricalLowercase()
        );

        assertEquals(
                "LEXICOGRAPHIC_ASCENDING",
                contract.categoricalOrdering()
        );

        assertEquals(
                "REJECT",
                contract.unknownPolicy()
        );

        assertEquals(
                "REJECT",
                contract.missingPolicy()
        );


        /*
         * =========================================================
         * NUMERIC GLOBAL CONTRACT
         * =========================================================
         */

        assertEquals(
                "FLOAT32",
                contract.numericDtype()
        );

        assertEquals(
                2,
                contract.numericFeatureCount()
        );

        assertEquals(
                -1.0F,
                contract.numericMissingValue()
        );

        assertEquals(
                0.0F,
                contract.normalizedMin()
        );

        assertEquals(
                1.0F,
                contract.normalizedMax()
        );


        /*
         * =========================================================
         * EMPTY STRING CATEGORY
         * =========================================================
         *
         * Empty string là category hợp lệ của:
         *
         * CAUSE_CODE
         * SUB_CAUSE_CODE
         *
         * nên không được mất sau khi parse YAML.
         */

        GoldFeatureContract.CategoricalFeature causeFeature =
                contract
                        .categoricalFeatures()
                        .stream()
                        .filter(
                                feature ->
                                        feature.index() == 2
                        )
                        .findFirst()
                        .orElseThrow();

        assertEquals(
                0,
                causeFeature
                        .vocabulary()
                        .get("")
        );
    }


    /**
     * Model/pipeline hiện tại chưa hỗ trợ partial window.
     *
     * <p>
     * Đây là capability validation nên lỗi được phát hiện
     * ngay trong compact constructor.
     * </p>
     */
    @Test
    void shouldRejectPartialWindows()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "emit-partial-windows: false",
                                "emit-partial-windows: true"
                        );

        JsonNode root =
                parseYaml(
                        yaml
                );

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        GoldFeatureContract
                                .fromRoot(
                                        root
                                )
        );
    }


    /**
     * Model v1 chỉ nhận sequence length = 32.
     */
    @Test
    void shouldRejectDifferentSequenceLength()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "length: 32",
                                "length: 64"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Model/dataset v1 được tạo với stride = 8.
     *
     * <p>
     * Stride không thay tensor shape,
     * nhưng thay đổi cách Gold sinh sample:
     * </p>
     *
     * <pre>
     * stride 8:
     *
     * 1..32
     * 9..40
     * 17..48
     * </pre>
     */
    @Test
    void shouldRejectDifferentSequenceStride()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "stride: 8",
                                "stride: 4"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Padding policy cũng thuộc contract v1.
     */
    @Test
    void shouldRejectDifferentPaddingSide()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "padding-side: LEFT",
                                "padding-side: RIGHT"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Không được đổi feature-version nhưng vẫn sử dụng
     * validator/model v1 hiện tại.
     */
    @Test
    void shouldRejectUnsupportedFeatureVersion()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "gold-ue-sequence-feature-v1",
                                "gold-ue-sequence-feature-v2"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Source của categorical feature là một phần
     * của model contract.
     *
     * <p>
     * Nếu đổi source nhưng giữ feature-version v1,
     * tensor có thể mang ý nghĩa hoàn toàn khác.
     * </p>
     */
    @Test
    void shouldRejectDifferentCategoricalSource()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "source: eventId",
                                "source: anotherEventId"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Transform categorical phải giữ nguyên.
     */
    @Test
    void shouldRejectDifferentCategoricalTransform()
            throws Exception {

        String yaml =
                validContract()
                        .replaceFirst(
                                "transform: fixed_vocabulary_lookup",
                                "transform: another_transform"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Vocabulary ID tuyệt đối không được đổi
     * trong cùng feature-version.
     *
     * <p>
     * Ví dụ:
     * </p>
     *
     * <pre>
     * l_attach -> 1
     * </pre>
     *
     * nếu đổi thành:
     *
     * <pre>
     * l_attach -> 99
     * </pre>
     *
     * tensor vẫn có thể được tạo nhưng model sẽ hiểu sai category.
     */
    @Test
    void shouldRejectChangedVocabularyId()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "l_attach: 1",
                                "l_attach: 99"
                        );

        assertInvalidContract(
                yaml
        );
    }


        /**
         * Không được thêm category mới vào vocabulary v1.
         *
         * <p>
         * Thêm category mới cũng làm thay đổi contract
         * giữa preprocessing và trained model.
         * </p>
         */
        @Test
        void shouldRejectAdditionalVocabularyCategory()
                throws Exception {

        /*
        * Trong validContract(), vocabulary key nằm ở
        * indentation 10 spaces.
        *
        * Không dùng Java text block làm replacement
        * nhiều dòng ở đây vì text block tự loại bỏ
        * incidental indentation, dễ tạo YAML sai cấu trúc.
        */
        String vocabularyEntry =
                "          l_tau: 9";

        String yaml =
                validContract()
                        .replace(
                                vocabularyEntry,
                                vocabularyEntry
                                        + "\n"
                                        + "          l_new_event: 10"
                        );

        assertInvalidContract(
                yaml
        );
        }


    /**
     * Runtime hiện luôn trim categorical input.
     *
     * YAML không được khai báo khác behavior thực tế.
     */
    @Test
    void shouldRejectDisabledCategoricalTrim()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "trim: true",
                                "trim: false"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Runtime CategoricalVocabulary hiện lowercase category.
     */
    @Test
    void shouldRejectDisabledCategoricalLowercase()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "lowercase: true",
                                "lowercase: false"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Ordering của categorical vocabulary thuộc contract v1.
     */
    @Test
    void shouldRejectDifferentCategoricalOrdering()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "ordering: LEXICOGRAPHIC_ASCENDING",
                                "ordering: INSERTION_ORDER"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Runtime hiện reject unknown categorical.
     */
    @Test
    void shouldRejectDifferentUnknownPolicy()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "unknown-policy: REJECT",
                                "unknown-policy: USE_UNKNOWN"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Runtime hiện reject missing categorical.
     */
    @Test
    void shouldRejectDifferentMissingPolicy()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "missing-policy: REJECT",
                                "missing-policy: USE_MISSING"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Numeric missing value của model v1 là -1.0.
     */
    @Test
    void shouldRejectDifferentNumericMissingValue()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "missing-value: -1.0",
                                "missing-value: 0.0"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Output normalized range của model v1 phải là [0, 1].
     */
    @Test
    void shouldRejectDifferentNormalizedRange()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "max: 1.0",
                                "max: 2.0"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Numeric source không được thay đổi trong model v1.
     */
    @Test
    void shouldRejectDifferentNumericSource()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "source: durationMs",
                                "source: requestRetries"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Numeric clip range là một phần của preprocessing
     * đã được dùng khi train model.
     */
    @Test
    void shouldRejectDifferentNumericClipRange()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "raw-clip-max: 600000",
                                "raw-clip-max: 300000"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Numeric transform phải giữ nguyên.
     *
     * <p>
     * Đây là test đặc biệt quan trọng vì cả:
     * </p>
     *
     * <pre>
     * log1p_minmax
     * clipped_minmax
     * </pre>
     *
     * đều có thể được runtime hỗ trợ,
     * nhưng hai transform tạo dữ liệu model khác nhau.
     */
    @Test
    void shouldRejectDifferentNumericTransform()
            throws Exception {

        String yaml =
                validContract()
                        .replace(
                                "transform: log1p_minmax",
                                "transform: clipped_minmax"
                        );

        assertInvalidContract(
                yaml
        );
    }


    /**
     * Helper parse một GoldFeatureContract hợp lệ.
     */
    private static GoldFeatureContract parseContract(
            String yaml
    ) throws Exception {

        JsonNode root =
                parseYaml(
                        yaml
                );

        return GoldFeatureContract
                .fromRoot(
                        root
                );
    }


    /**
     * Helper parse YAML thành JsonNode.
     */
    private static JsonNode parseYaml(
            String yaml
    ) throws Exception {

        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory()
                );

        return mapper.readTree(
                yaml
        );
    }


    /**
     * Helper dùng chung cho các compatibility negative test.
     *
     * <p>
     * Những contract này có cấu trúc YAML hợp lệ,
     * nhưng không tương thích với model v1,
     * nên phải fail bằng IllegalStateException.
     * </p>
     */
    private static void assertInvalidContract(
            String yaml
    ) throws Exception {

        JsonNode root =
                parseYaml(
                        yaml
                );

        assertThrows(
                IllegalStateException.class,
                () ->
                        GoldFeatureContract
                                .fromRoot(
                                        root
                                )
        );
    }


    /**
     * Contract chuẩn duy nhất phục vụ toàn bộ test.
     *
     * <p>
     * Giá trị phải khớp với:
     * </p>
     *
     * <pre>
     * feature-contract
     * </pre>
     *
     * trong application.yaml của model v1.
     */
    private static String validContract() {

        return """
                feature-contract:
                  feature-version: gold-ue-sequence-feature-v1

                  sequence:
                    length: 32
                    stride: 8
                    padding-side: LEFT
                    emit-partial-windows: false

                  categorical:
                    dtype: INT64
                    feature-count: 4

                    normalization:
                      trim: true
                      lowercase: true

                    ordering: LEXICOGRAPHIC_ASCENDING

                    unknown-policy: REJECT
                    missing-policy: REJECT

                    features:

                      - index: 0
                        name: event_code
                        source: eventId
                        transform: fixed_vocabulary_lookup

                        vocabulary:
                          l_attach: 1
                          l_bearer_modify: 2
                          l_dedicated_bearer_activate: 3
                          l_dedicated_bearer_deactivate: 4
                          l_detach: 5
                          l_handover: 6
                          l_pdn_connect: 7
                          l_service_request: 8
                          l_tau: 9


                      - index: 1
                        name: event_result_code
                        source: eventResult
                        transform: fixed_vocabulary_lookup

                        vocabulary:
                          reject: 0
                          success: 1


                      - index: 2
                        name: normalized_cause_code
                        source: rawFields.CAUSE_CODE
                        transform: fixed_vocabulary_lookup

                        vocabulary:
                          "": 0
                          "10": 1
                          "38": 2
                          "9": 3


                      - index: 3
                        name: sub_cause_code
                        source: rawFields.SUB_CAUSE_CODE
                        transform: fixed_vocabulary_lookup

                        vocabulary:
                          "": 0
                          "107": 1
                          "11": 2
                          "14": 3
                          "403": 4
                          "410": 5
                          "413": 6


                  numeric:
                    dtype: FLOAT32
                    feature-count: 2

                    missing-value: -1.0

                    normalized-valid-range:
                      min: 0.0
                      max: 1.0

                    features:

                      - index: 0
                        name: duration_ms
                        source: durationMs

                        raw-clip-min: 0
                        raw-clip-max: 600000

                        transform: log1p_minmax


                      - index: 1
                        name: request_retries
                        source: requestRetries

                        raw-clip-min: 0
                        raw-clip-max: 10

                        transform: clipped_minmax
                """;
    }
}