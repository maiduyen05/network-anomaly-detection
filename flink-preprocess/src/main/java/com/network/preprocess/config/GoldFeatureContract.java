package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Contract mô tả chính xác dữ liệu mà Gold phải tạo cho model.
 *
 * Class này không thực hiện encode feature mà chỉ biểu diễn featrue-contract trong application.yaml
 * Nó chỉ biểu diễn cấu hình trong:
 * ==> Tránh việc sequence length, vocabulary,
 * numeric range... bị hard-code rải rác trong nhiều class Java.
 *
 * Nếu thay đổi ý nghĩa feature hoặc vocabulary thì phải tạo
 * feature-version mới và model tương ứng.
 * </p>
 */
public record GoldFeatureContract(

        /*
         * =========================================================
         * FEATURE VERSION
         * =========================================================
         */

        String featureVersion,


        /*
         * =========================================================
         * SEQUENCE CONTRACT
         * =========================================================
         */

        int sequenceLength,

        int sequenceStride,

        String paddingSide,

        boolean emitPartialWindows,


        /*
         * =========================================================
         * CATEGORICAL CONTRACT
         * =========================================================
         */

        String categoricalDtype,

        int categoricalFeatureCount,

        boolean categoricalTrim,

        boolean categoricalLowercase,

        String categoricalOrdering,

        String unknownPolicy,

        String missingPolicy,

        List<CategoricalFeature> categoricalFeatures,


        /*
         * =========================================================
         * NUMERIC CONTRACT
         * =========================================================
         */

        String numericDtype,

        int numericFeatureCount,

        float numericMissingValue,

        float normalizedMin,

        float normalizedMax,

        List<NumericFeature> numericFeatures

) implements Serializable {

    /**
     * Feature categorical.
     *
     * Ví dụ:
     *
     * <pre>
     * index = 0
     * name = event_code
     * source = eventId
     * transform = fixed_vocabulary_lookup
     * </pre>
     */
    public record CategoricalFeature(
            int index,
            String name,
            String source,
            String transform,
            Map<String, Integer> vocabulary
    ) implements Serializable {

        public CategoricalFeature {

            if (index < 0) {
                throw new IllegalArgumentException(
                        "categorical feature index must not be negative"
                );
            }

            name = requiredText(
                    name,
                    "categorical feature name"
            );

            source = requiredText(
                    source,
                    "categorical feature source"
            );

            transform = requiredText(
                    transform,
                    "categorical feature transform"
            );

            Objects.requireNonNull(
                    vocabulary,
                    "categorical vocabulary must not be null"
            );

            /*
             * LinkedHashMap được dùng để:
             *
             * - giữ thứ tự ổn định;
             * - không giữ reference Map của Jackson;
             * - phù hợp serialization;
             * - giữ được key "" vì empty string là category hợp lệ
             *   của cause_code/sub_cause_code.
             */
            vocabulary =
                    new LinkedHashMap<>(
                            vocabulary
                    );
        }
    }

    /**
     * Numeric feature.
     *
     * Ví dụ:
     *
     * <pre>
     * duration_ms
     * raw range: 0..600000
     * transform: log1p_minmax
     * </pre>
     */
    public record NumericFeature(
            int index,
            String name,
            String source,
            double rawClipMin,
            double rawClipMax,
            String transform
    ) implements Serializable {

        public NumericFeature {

            if (index < 0) {
                throw new IllegalArgumentException(
                        "numeric feature index must not be negative"
                );
            }

            name = requiredText(
                    name,
                    "numeric feature name"
            );

            source = requiredText(
                    source,
                    "numeric feature source"
            );

            transform = requiredText(
                    transform,
                    "numeric feature transform"
            );

            if (rawClipMax < rawClipMin) {
                throw new IllegalArgumentException(
                        "rawClipMax must be >= rawClipMin for "
                                + name
                );
            }
        }
    }

    /**
     * Compact constructor kiểm tra tính hợp lệ của toàn contract.
     */
    public GoldFeatureContract {

        featureVersion =
                requiredText(
                        featureVersion,
                        "featureVersion"
                );

        if (sequenceLength <= 0) {
            throw new IllegalArgumentException(
                    "sequenceLength must be positive"
            );
        }

        if (sequenceStride <= 0
                || sequenceStride > sequenceLength) {

            throw new IllegalArgumentException(
                    "sequenceStride must be between 1 and sequenceLength"
            );
        }

        paddingSide =
                requiredText(
                        paddingSide,
                        "paddingSide"
                );

        categoricalOrdering =
                requiredText(
                        categoricalOrdering,
                        "categoricalOrdering"
                );

        categoricalDtype =
                requiredText(
                        categoricalDtype,
                        "categoricalDtype"
                );

        numericDtype =
                requiredText(
                        numericDtype,
                        "numericDtype"
                );

        unknownPolicy =
                requiredText(
                        unknownPolicy,
                        "unknownPolicy"
                );

        missingPolicy =
                requiredText(
                        missingPolicy,
                        "missingPolicy"
                );

        if (categoricalFeatureCount <= 0) {
            throw new IllegalArgumentException(
                    "categoricalFeatureCount must be positive"
            );
        }

        if (numericFeatureCount <= 0) {
            throw new IllegalArgumentException(
                    "numericFeatureCount must be positive"
            );
        }

        if (normalizedMax < normalizedMin) {
            throw new IllegalArgumentException(
                    "normalizedMax must be >= normalizedMin"
            );
        }

        Objects.requireNonNull(
                categoricalFeatures,
                "categoricalFeatures must not be null"
        );

        Objects.requireNonNull(
                numericFeatures,
                "numericFeatures must not be null"
        );

        categoricalFeatures =
                new ArrayList<>(
                        categoricalFeatures
                );

        numericFeatures =
                new ArrayList<>(
                        numericFeatures
                );

        if (categoricalFeatures.size()
                != categoricalFeatureCount) {

            throw new IllegalArgumentException(
                    "categorical feature-count does not match "
                            + "number of categorical features"
            );
        }

        if (numericFeatures.size()
                != numericFeatureCount) {

            throw new IllegalArgumentException(
                    "numeric feature-count does not match "
                            + "number of numeric features"
            );
        }

        validateIndexes(
                categoricalFeatures
                        .stream()
                        .map(CategoricalFeature::index)
                        .toList(),
                categoricalFeatureCount,
                "categorical"
        );

        validateIndexes(
                numericFeatures
                        .stream()
                        .map(NumericFeature::index)
                        .toList(),
                numericFeatureCount,
                "numeric"
        );

        /*
         * Pipeline hiện tại không implement padding/partial window.
         *
         * Nếu ai đó sửa YAML thành:
         *
         * emit-partial-windows: true
         *
         * job phải fail ngay lúc startup thay vì âm thầm
         * tạo dữ liệu sai contract.
         */
        if (emitPartialWindows) {
            throw new IllegalArgumentException(
                    "emit-partial-windows=true is not supported "
                            + "by the current Gold pipeline"
            );
        }
    }

    /**
     * Parse nhóm feature-contract từ root application.yaml.
     */
    public static GoldFeatureContract fromRoot(
            JsonNode root
    ) {

        Objects.requireNonNull(
                root,
                "root must not be null"
        );

        JsonNode contract =
                requiredNode(
                        root,
                        "feature-contract"
                );

        JsonNode sequence =
                requiredNode(
                        contract,
                        "sequence"
                );

        JsonNode categorical =
                requiredNode(
                        contract,
                        "categorical"
                );

        JsonNode categoricalNormalization =
                requiredNode(
                        categorical,
                        "normalization"
                );

        JsonNode numeric =
                requiredNode(
                        contract,
                        "numeric"
                );

        JsonNode normalizedRange =
                requiredNode(
                        numeric,
                        "normalized-valid-range"
                );

        List<CategoricalFeature>
                categoricalFeatures =
                parseCategoricalFeatures(
                        requiredNode(
                                categorical,
                                "features"
                        )
                );

        List<NumericFeature>
                numericFeatures =
                parseNumericFeatures(
                        requiredNode(
                                numeric,
                                "features"
                        )
                );

        GoldFeatureContract result =
                new GoldFeatureContract(

                        /*
                        * =================================================
                        * FEATURE VERSION
                        * =================================================
                        */

                        requiredText(
                                contract,
                                "feature-version"
                        ),


                        /*
                        * =================================================
                        * SEQUENCE
                        * =================================================
                        */

                        requiredPositiveInt(
                                sequence,
                                "length"
                        ),

                        requiredPositiveInt(
                                sequence,
                                "stride"
                        ),

                        requiredText(
                                sequence,
                                "padding-side"
                        ),

                        requiredBoolean(
                                sequence,
                                "emit-partial-windows"
                        ),


                        /*
                        * =================================================
                        * CATEGORICAL
                        * =================================================
                        */

                        requiredText(
                                categorical,
                                "dtype"
                        ),

                        requiredPositiveInt(
                                categorical,
                                "feature-count"
                        ),


                        /*
                        * Categorical normalization.
                        */
                        requiredBoolean(
                                categoricalNormalization,
                                "trim"
                        ),

                        requiredBoolean(
                                categoricalNormalization,
                                "lowercase"
                        ),

                        requiredText(
                                categorical,
                                "ordering"
                        ),


                        /*
                        * Missing / unknown policy.
                        */
                        requiredText(
                                categorical,
                                "unknown-policy"
                        ),

                        requiredText(
                                categorical,
                                "missing-policy"
                        ),

                        categoricalFeatures,


                        /*
                        * =================================================
                        * NUMERIC
                        * =================================================
                        */

                        requiredText(
                                numeric,
                                "dtype"
                        ),

                        requiredPositiveInt(
                                numeric,
                                "feature-count"
                        ),

                        (float) requiredDouble(
                                numeric,
                                "missing-value"
                        ),

                        (float) requiredDouble(
                                normalizedRange,
                                "min"
                        ),

                        (float) requiredDouble(
                                normalizedRange,
                                "max"
                        ),

                        numericFeatures
                );

        /*
         * Kiểm tra contract v1 mà model hiện tại hỗ trợ.
         *
         * Không dùng check này để encode.
         * Đây chỉ là compatibility guard.
         */
        validateCurrentModelCompatibility(
                result
        );

        return result;
    }

    /**
     * Parse danh sách categorical features.
     */
    private static List<CategoricalFeature>
    parseCategoricalFeatures(
            JsonNode featuresNode
    ) {

        if (!featuresNode.isArray()) {
            throw new IllegalStateException(
                    "feature-contract.categorical.features "
                            + "must be an array"
            );
        }

        List<CategoricalFeature> result =
                new ArrayList<>();

        for (JsonNode feature : featuresNode) {

            Map<String, Integer> vocabulary =
                    parseVocabulary(
                            requiredNode(
                                    feature,
                                    "vocabulary"
                            )
                    );

            result.add(
                    new CategoricalFeature(
                            requiredNonNegativeInt(
                                    feature,
                                    "index"
                            ),
                            requiredText(
                                    feature,
                                    "name"
                            ),
                            requiredText(
                                    feature,
                                    "source"
                            ),
                            requiredText(
                                    feature,
                                    "transform"
                            ),
                            vocabulary
                    )
            );
        }

        return result;
    }

    /**
     * Parse numeric feature definitions.
     */
    private static List<NumericFeature>
    parseNumericFeatures(
            JsonNode featuresNode
    ) {

        if (!featuresNode.isArray()) {
            throw new IllegalStateException(
                    "feature-contract.numeric.features must be an array"
            );
        }

        List<NumericFeature> result =
                new ArrayList<>();

        for (JsonNode feature : featuresNode) {

            result.add(
                    new NumericFeature(
                            requiredNonNegativeInt(
                                    feature,
                                    "index"
                            ),
                            requiredText(
                                    feature,
                                    "name"
                            ),
                            requiredText(
                                    feature,
                                    "source"
                            ),
                            requiredDouble(
                                    feature,
                                    "raw-clip-min"
                            ),
                            requiredDouble(
                                    feature,
                                    "raw-clip-max"
                            ),
                            requiredText(
                                    feature,
                                    "transform"
                            )
                    )
            );
        }

        return result;
    }

    /**
     * Parse vocabulary.
     *
     * Không trim key ở đây.
     *
     * Lý do:
     *
     * <pre>
     * vocabulary:
     *   "": 0
     * </pre>
     *
     * Empty string là category hợp lệ trong contract hiện tại.
     */
    private static Map<String, Integer>
    parseVocabulary(
            JsonNode vocabularyNode
    ) {

        if (!vocabularyNode.isObject()) {
            throw new IllegalStateException(
                    "vocabulary must be an object"
            );
        }

        Map<String, Integer> result =
                new LinkedHashMap<>();

        vocabularyNode
                .fields()
                .forEachRemaining(
                        entry -> {

                            JsonNode value =
                                    entry.getValue();

                            if (!value.canConvertToInt()) {
                                throw new IllegalStateException(
                                        "Vocabulary value must be integer: "
                                                + entry.getKey()
                                );
                            }

                            result.put(
                                    entry.getKey(),
                                    value.asInt()
                            );
                        }
                );

        if (result.isEmpty()) {
            throw new IllegalStateException(
                    "vocabulary must not be empty"
            );
        }

        return result;
    }

        /**
         * Validate feature contract với model đang được deploy.
         *
         * <p>
         * Đây là compatibility guard giữa:
         * </p>
         *
         * <pre>
         * application.yaml
         *        ↕
         * Gold preprocessing
         *        ↕
         * trained model
         * </pre>
         *
         * <p>
         * Với feature-version:
         * </p>
         *
         * <pre>
         * gold-ue-sequence-feature-v2
         * </pre>
         *
         * mọi thành phần ảnh hưởng đến tensor phải giữ nguyên.
         *
         * Nếu muốn thay đổi một trong các giá trị này thì phải:
         *
         * <ol>
         *     <li>Tạo feature-version mới.</li>
         *     <li>Train lại model tương ứng.</li>
         *     <li>Deploy preprocessing + model mới cùng nhau.</li>
         * </ol>
         */
        private static void validateCurrentModelCompatibility(
                GoldFeatureContract contract
        ) {

        Objects.requireNonNull(
                contract,
                "contract must not be null"
        );

        /*
        * =============================================================
        * FEATURE VERSION
        * =============================================================
        */

        requireExact(
                "gold-ue-sequence-feature-v2",
                contract.featureVersion(),
                "feature-version"
        );


        /*
        * =============================================================
        * SEQUENCE
        * =============================================================
        *
        * Length ảnh hưởng trực tiếp tensor shape.
        *
        * Stride ảnh hưởng cách sinh sample:
        *
        * 1..32
        * 9..40
        * 17..48
        *
        * Vì model/dataset v1 được định nghĩa với stride 8,
        * không cho phép thay đổi trong cùng feature-version.
        */

        requireExact(
                32,
                contract.sequenceLength(),
                "sequence.length"
        );

        requireExact(
                8,
                contract.sequenceStride(),
                "sequence.stride"
        );

        requireExact(
                "LEFT",
                contract.paddingSide(),
                "sequence.padding-side"
        );

        if (contract.emitPartialWindows()) {

                throw incompatible(
                        "sequence.emit-partial-windows",
                        false,
                        true
                );
        }


        /*
        * =============================================================
        * CATEGORICAL GLOBAL CONTRACT
        * =============================================================
        */

        requireExact(
                "INT64",
                contract.categoricalDtype(),
                "categorical.dtype"
        );

        requireExact(
                4,
                contract.categoricalFeatureCount(),
                "categorical.feature-count"
        );

        /*
        * Runtime CategoricalVocabulary hiện luôn:
        *
        * trim()
        * lowercase(Locale.ROOT)
        *
        * Vì vậy YAML phải mô tả đúng behavior đó.
        */
        if (!contract.categoricalTrim()) {

                throw incompatible(
                        "categorical.normalization.trim",
                        true,
                        false
                );
        }

        if (!contract.categoricalLowercase()) {

                throw incompatible(
                        "categorical.normalization.lowercase",
                        true,
                        false
                );
        }

        requireExact(
                "LEXICOGRAPHIC_ASCENDING",
                contract.categoricalOrdering(),
                "categorical.ordering"
        );

        /*
        * Runtime hiện reject missing/unknown categorical.
        */
        requireExact(
                "REJECT",
                contract.unknownPolicy(),
                "categorical.unknown-policy"
        );

        requireExact(
                "REJECT",
                contract.missingPolicy(),
                "categorical.missing-policy"
        );


        /*
        * =============================================================
        * CATEGORICAL FEATURE 0
        * EVENT_ID
        * =============================================================
        */

        requireCategoricalFeature(
                contract,
                0,
                "event_code",
                "eventId",
                "fixed_vocabulary_lookup",
                Map.ofEntries(
                        Map.entry("l_attach", 1),
                        Map.entry("l_bearer_modify", 2),
                        Map.entry(
                                "l_dedicated_bearer_activate",
                                3
                        ),
                        Map.entry(
                                "l_dedicated_bearer_deactivate",
                                4
                        ),
                        Map.entry("l_detach", 5),
                        Map.entry("l_handover", 6),
                        Map.entry("l_pdn_connect", 7),
                        Map.entry("l_pdn_disconnect", 8),
                        Map.entry("l_service_request", 9),
                        Map.entry("l_tau", 10)
                )
        );


        /*
        * =============================================================
        * CATEGORICAL FEATURE 1
        * EVENT_RESULT
        * =============================================================
        */

        requireCategoricalFeature(
                contract,
                1,
                "event_result_code",
                "eventResult",
                "fixed_vocabulary_lookup",
                Map.of(
                        "abort", 1,
                        "reject", 2,
                        "success", 3
                )
        );


        /*
        * =============================================================
        * CATEGORICAL FEATURE 2
        * CAUSE_CODE
        * =============================================================
        *
        * Empty string là category thật của model v1.
        */

        Map<String, Integer> expectedCauseVocabulary =
                new LinkedHashMap<>();

        expectedCauseVocabulary.put("", 0);
        expectedCauseVocabulary.put("0", 1);
        expectedCauseVocabulary.put("10", 2);
        expectedCauseVocabulary.put("11", 3);
        expectedCauseVocabulary.put("13", 4);
        expectedCauseVocabulary.put("15", 5);
        expectedCauseVocabulary.put("16", 6);
        expectedCauseVocabulary.put("17", 7);
        expectedCauseVocabulary.put("26", 8);
        expectedCauseVocabulary.put("3", 9);
        expectedCauseVocabulary.put("30", 10);
        expectedCauseVocabulary.put("31", 11);
        expectedCauseVocabulary.put("32", 12);
        expectedCauseVocabulary.put("34", 13);
        expectedCauseVocabulary.put("38", 14);
        expectedCauseVocabulary.put("39", 15);
        expectedCauseVocabulary.put("40", 16);
        expectedCauseVocabulary.put("43", 17);
        expectedCauseVocabulary.put("53", 18);
        expectedCauseVocabulary.put("6", 19);
        expectedCauseVocabulary.put("65", 20);
        expectedCauseVocabulary.put("7", 21);
        expectedCauseVocabulary.put("73", 22);
        expectedCauseVocabulary.put("87", 23);
        expectedCauseVocabulary.put("88", 24);
        expectedCauseVocabulary.put("9", 25);
        expectedCauseVocabulary.put("unspecified", 26);

        requireCategoricalFeature(
                contract,
                2,
                "normalized_cause_code",
                "rawFields.CAUSE_CODE",
                "fixed_vocabulary_lookup",
                expectedCauseVocabulary
        );


        /*
        * =============================================================
        * CATEGORICAL FEATURE 3
        * SUB_CAUSE_CODE
        * =============================================================
        */

       Map<String, Integer> expectedSubCauseVocabulary =
        new LinkedHashMap<>();

        expectedSubCauseVocabulary.put("", 0);
        expectedSubCauseVocabulary.put("1", 1);
        expectedSubCauseVocabulary.put("102", 2);
        expectedSubCauseVocabulary.put("104", 3);
        expectedSubCauseVocabulary.put("107", 4);
        expectedSubCauseVocabulary.put("108", 5);
        expectedSubCauseVocabulary.put("109", 6);
        expectedSubCauseVocabulary.put("11", 7);
        expectedSubCauseVocabulary.put("12", 8);
        expectedSubCauseVocabulary.put("13", 9);
        expectedSubCauseVocabulary.put("14", 10);
        expectedSubCauseVocabulary.put("15", 11);
        expectedSubCauseVocabulary.put("16", 12);
        expectedSubCauseVocabulary.put("17", 13);
        expectedSubCauseVocabulary.put("2", 14);
        expectedSubCauseVocabulary.put("204", 15);
        expectedSubCauseVocabulary.put("3", 16);
        expectedSubCauseVocabulary.put("304", 17);
        expectedSubCauseVocabulary.put("306", 18);
        expectedSubCauseVocabulary.put("308", 19);
        expectedSubCauseVocabulary.put("311", 20);
        expectedSubCauseVocabulary.put("312", 21);
        expectedSubCauseVocabulary.put("315", 22);
        expectedSubCauseVocabulary.put("316", 23);
        expectedSubCauseVocabulary.put("317", 24);
        expectedSubCauseVocabulary.put("318", 25);
        expectedSubCauseVocabulary.put("403", 26);
        expectedSubCauseVocabulary.put("405", 27);
        expectedSubCauseVocabulary.put("410", 28);
        expectedSubCauseVocabulary.put("413", 29);
        expectedSubCauseVocabulary.put("419", 30);
        expectedSubCauseVocabulary.put("420", 31);
        expectedSubCauseVocabulary.put("422", 32);
        expectedSubCauseVocabulary.put("423", 33);
        expectedSubCauseVocabulary.put("424", 34);
        expectedSubCauseVocabulary.put("427", 35);
        expectedSubCauseVocabulary.put("428", 36);
        expectedSubCauseVocabulary.put("429", 37);
        expectedSubCauseVocabulary.put("430", 38);
        expectedSubCauseVocabulary.put("435", 39);
        expectedSubCauseVocabulary.put("440", 40);
        expectedSubCauseVocabulary.put("441", 41);
        expectedSubCauseVocabulary.put("5", 42);
        expectedSubCauseVocabulary.put("503", 43);
        expectedSubCauseVocabulary.put("504", 44);
        expectedSubCauseVocabulary.put("505", 45);
        expectedSubCauseVocabulary.put("506", 46);
        expectedSubCauseVocabulary.put("507", 47);
        expectedSubCauseVocabulary.put("509", 48);
        expectedSubCauseVocabulary.put("603", 49);
        expectedSubCauseVocabulary.put("605", 50);
        expectedSubCauseVocabulary.put("606", 51);
        expectedSubCauseVocabulary.put("607", 52);
        expectedSubCauseVocabulary.put("608", 53);
        expectedSubCauseVocabulary.put("610", 54);
        expectedSubCauseVocabulary.put("611", 55);
        expectedSubCauseVocabulary.put("7", 56);
        expectedSubCauseVocabulary.put("704", 57);
        expectedSubCauseVocabulary.put("706", 58);
        expectedSubCauseVocabulary.put("709", 59);
        expectedSubCauseVocabulary.put("710", 60);
        expectedSubCauseVocabulary.put("8", 61);
        expectedSubCauseVocabulary.put("9", 62);

        requireCategoricalFeature(
                contract,
                3,
                "sub_cause_code",
                "rawFields.SUB_CAUSE_CODE",
                "fixed_vocabulary_lookup",
                expectedSubCauseVocabulary
        );


        /*
        * =============================================================
        * NUMERIC GLOBAL CONTRACT
        * =============================================================
        */

        requireExact(
                "FLOAT32",
                contract.numericDtype(),
                "numeric.dtype"
        );

        requireExact(
                2,
                contract.numericFeatureCount(),
                "numeric.feature-count"
        );

        requireFloatExact(
                -1.0F,
                contract.numericMissingValue(),
                "numeric.missing-value"
        );

        /*
        * NumericFeatureEncoder hiện normalize về [0, 1].
        *
        * Không cho YAML đổi range nhưng vẫn dùng model v1.
        */
        requireFloatExact(
                0.0F,
                contract.normalizedMin(),
                "numeric.normalized-valid-range.min"
        );

        requireFloatExact(
                1.0F,
                contract.normalizedMax(),
                "numeric.normalized-valid-range.max"
        );


        /*
        * =============================================================
        * NUMERIC FEATURE 0
        * DURATION_MS
        * =============================================================
        */

        requireNumericFeature(
                contract,
                0,
                "duration_ms",
                "durationMs",
                0.0D,
                600_000.0D,
                "log1p_minmax"
        );


        /*
        * =============================================================
        * NUMERIC FEATURE 1
        * REQUEST_RETRIES
        * =============================================================
        */

        requireNumericFeature(
                contract,
                1,
                "request_retries",
                "requestRetries",
                0.0D,
                10.0D,
                "clipped_minmax"
        );
        }

        /**
         * Validate chính xác một categorical feature.
         */
        private static void requireCategoricalFeature(
                GoldFeatureContract contract,
                int index,
                String expectedName,
                String expectedSource,
                String expectedTransform,
                Map<String, Integer> expectedVocabulary
        ) {

        CategoricalFeature feature =
                contract
                        .categoricalFeatures()
                        .stream()
                        .filter(
                                candidate ->
                                        candidate.index() == index
                        )
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Missing categorical feature "
                                                        + "at index "
                                                        + index
                                        )
                        );

        requireExact(
                expectedName,
                feature.name(),
                "categorical.features["
                        + index
                        + "].name"
        );

        requireExact(
                expectedSource,
                feature.source(),
                "categorical.features["
                        + index
                        + "].source"
        );

        requireExact(
                expectedTransform,
                feature.transform(),
                "categorical.features["
                        + index
                        + "].transform"
        );

        /*
        * Map.equals() kiểm tra:
        *
        * - cùng key;
        * - cùng value;
        *
        * nhưng không phụ thuộc insertion order.
        *
        * Đây là điều ta cần vì vocabulary ID mới là
        * phần ảnh hưởng trực tiếp model.
        */
        if (!expectedVocabulary.equals(
                feature.vocabulary()
        )) {

                throw incompatible(
                        "categorical.features["
                                + index
                                + "].vocabulary",
                        expectedVocabulary,
                        feature.vocabulary()
                );
        }
        }

        /**
         * Validate chính xác một numeric feature.
         */
        private static void requireNumericFeature(
                GoldFeatureContract contract,
                int index,
                String expectedName,
                String expectedSource,
                double expectedRawClipMin,
                double expectedRawClipMax,
                String expectedTransform
        ) {

        NumericFeature feature =
                contract
                        .numericFeatures()
                        .stream()
                        .filter(
                                candidate ->
                                        candidate.index() == index
                        )
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Missing numeric feature "
                                                        + "at index "
                                                        + index
                                        )
                        );

        requireExact(
                expectedName,
                feature.name(),
                "numeric.features["
                        + index
                        + "].name"
        );

        requireExact(
                expectedSource,
                feature.source(),
                "numeric.features["
                        + index
                        + "].source"
        );

        requireDoubleExact(
                expectedRawClipMin,
                feature.rawClipMin(),
                "numeric.features["
                        + index
                        + "].raw-clip-min"
        );

        requireDoubleExact(
                expectedRawClipMax,
                feature.rawClipMax(),
                "numeric.features["
                        + index
                        + "].raw-clip-max"
        );

        requireExact(
                expectedTransform,
                feature.transform(),
                "numeric.features["
                        + index
                        + "].transform"
        );
        }

    private static void validateIndexes(
            List<Integer> indexes,
            int expectedCount,
            String groupName
    ) {

        for (int expected = 0;
             expected < expectedCount;
             expected++) {

            if (!indexes.contains(expected)) {
                throw new IllegalArgumentException(
                        groupName
                                + " feature index missing: "
                                + expected
                );
            }
        }
    }

    private static JsonNode requiredNode(
            JsonNode parent,
            String property
    ) {

        JsonNode node =
                parent.get(property);

        if (node == null || node.isNull()) {
            throw new IllegalStateException(
                    "Missing configuration property: "
                            + property
            );
        }

        return node;
    }

    private static String requiredText(
            JsonNode parent,
            String property
    ) {

        JsonNode node =
                requiredNode(
                        parent,
                        property
                );

        if (!node.isTextual()
                || node.asText().isBlank()) {

            throw new IllegalStateException(
                    "Configuration property must be text: "
                            + property
            );
        }

        return node
                .asText()
                .trim();
    }

    private static String requiredText(
            String value,
            String fieldName
    ) {

        if (value == null
                || value.isBlank()) {

            throw new IllegalArgumentException(
                    fieldName
                            + " must not be blank"
            );
        }

        return value.trim();
    }

    private static int requiredPositiveInt(
            JsonNode parent,
            String property
    ) {

        int value =
                requiredNonNegativeInt(
                        parent,
                        property
                );

        if (value <= 0) {
            throw new IllegalStateException(
                    property
                            + " must be positive"
            );
        }

        return value;
    }

        /**
         * Validate String chính xác.
         */
        private static void requireExact(
                String expected,
                String actual,
                String path
        ) {

        if (!Objects.equals(
                expected,
                actual
        )) {

                throw incompatible(
                        path,
                        expected,
                        actual
                );
        }
        }


        /**
         * Validate int chính xác.
         */
        private static void requireExact(
                int expected,
                int actual,
                String path
        ) {

        if (expected != actual) {

                throw incompatible(
                        path,
                        expected,
                        actual
                );
        }
        }


        /**
         * Float trong contract là các constant cấu hình
         * như -1, 0, 1 nên Float.compare phù hợp.
         */
        private static void requireFloatExact(
                float expected,
                float actual,
                String path
        ) {

        if (Float.compare(
                expected,
                actual
        ) != 0) {

                throw incompatible(
                        path,
                        expected,
                        actual
                );
        }
        }


        /**
         * Numeric clip range hiện dùng các giá trị nguyên
         * được parse thành double nên Double.compare phù hợp.
         */
        private static void requireDoubleExact(
                double expected,
                double actual,
                String path
        ) {

        if (Double.compare(
                expected,
                actual
        ) != 0) {

                throw incompatible(
                        path,
                        expected,
                        actual
                );
        }
        }


        /**
         * Tạo thông báo lỗi thống nhất khi YAML không còn
         * tương thích với model v1.
         */
        private static IllegalStateException incompatible(
                String path,
                Object expected,
                Object actual
        ) {

        return new IllegalStateException(
                "Feature contract is incompatible with "
                        + "gold-ue-sequence-feature-v2: "
                        + path
                        + " expected <"
                        + expected
                        + "> but was <"
                        + actual
                        + ">"
        );
        }

    private static int requiredNonNegativeInt(
            JsonNode parent,
            String property
    ) {

        JsonNode node =
                requiredNode(
                        parent,
                        property
                );

        if (!node.canConvertToInt()) {
            throw new IllegalStateException(
                    property
                            + " must be integer"
            );
        }

        int value =
                node.asInt();

        if (value < 0) {
            throw new IllegalStateException(
                    property
                            + " must not be negative"
            );
        }

        return value;
    }

    private static double requiredDouble(
            JsonNode parent,
            String property
    ) {

        JsonNode node =
                requiredNode(
                        parent,
                        property
                );

        if (!node.isNumber()) {
            throw new IllegalStateException(
                    property
                            + " must be numeric"
            );
        }

        return node.asDouble();
    }

    private static boolean requiredBoolean(
            JsonNode parent,
            String property
    ) {

        JsonNode node =
                requiredNode(
                        parent,
                        property
                );

        if (!node.isBoolean()) {
            throw new IllegalStateException(
                    property
                            + " must be boolean"
            );
        }

        return node.asBoolean();
    }
}