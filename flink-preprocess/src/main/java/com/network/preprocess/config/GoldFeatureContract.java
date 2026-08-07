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

        String featureVersion,

        int sequenceLength,
        int sequenceStride,
        boolean emitPartialWindows,

        String categoricalDtype,
        int categoricalFeatureCount,
        String unknownPolicy,
        String missingPolicy,

        List<CategoricalFeature> categoricalFeatures,

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

                        requiredText(
                                contract,
                                "feature-version"
                        ),

                        requiredPositiveInt(
                                sequence,
                                "length"
                        ),

                        requiredPositiveInt(
                                sequence,
                                "stride"
                        ),

                        requiredBoolean(
                                sequence,
                                "emit-partial-windows"
                        ),

                        requiredText(
                                categorical,
                                "dtype"
                        ),

                        requiredPositiveInt(
                                categorical,
                                "feature-count"
                        ),

                        requiredText(
                                categorical,
                                "unknown-policy"
                        ),

                        requiredText(
                                categorical,
                                "missing-policy"
                        ),

                        categoricalFeatures,

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
     * Guard cho model hiện tại.
     *
     * README/model hiện tại yêu cầu:
     *
     * x_cat[32][4]
     * x_num[32][2]
     */
    private static void validateCurrentModelCompatibility(
            GoldFeatureContract contract
    ) {

        if (contract.sequenceLength() != 32) {
            throw new IllegalStateException(
                    "Current model requires sequence length 32"
            );
        }

        if (contract.categoricalFeatureCount() != 4) {
            throw new IllegalStateException(
                    "Current model requires 4 categorical features"
            );
        }

        if (contract.numericFeatureCount() != 2) {
            throw new IllegalStateException(
                    "Current model requires 2 numeric features"
            );
        }

        if (!"INT64".equals(
                contract.categoricalDtype()
        )) {
            throw new IllegalStateException(
                    "Current model requires categorical dtype INT64"
            );
        }

        if (!"FLOAT32".equals(
                contract.numericDtype()
        )) {
            throw new IllegalStateException(
                    "Current model requires numeric dtype FLOAT32"
            );
        }
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