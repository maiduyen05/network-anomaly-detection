package com.network.preprocess.gold.feature;

import com.network.preprocess.config.GoldFeatureContract;
import com.network.preprocess.model.GoldModelInput;
import com.network.preprocess.model.GoldSequenceEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Chuyển GoldSequenceEvent thành tensor model-ready
 * theo đúng GoldFeatureContract.
 *
 * <p>
 * Encoder KHÔNG hard-code:
 * </p>
 *
 * <ul>
 *     <li>sequence length;</li>
 *     <li>số categorical feature;</li>
 *     <li>số numeric feature;</li>
 *     <li>categorical vocabulary;</li>
 *     <li>numeric clip range;</li>
 *     <li>missing numeric value.</li>
 * </ul>
 *
 * <p>
 * Tất cả giá trị trên lấy từ feature-contract.
 * </p>
 */
public final class GoldFeatureEncoder
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final GoldFeatureContract contract;

    private final List<
            GoldFeatureContract.CategoricalFeature
            > categoricalFeatures;

    private final List<
            GoldFeatureContract.NumericFeature
            > numericFeatures;

    private final List<CategoricalVocabulary>
            categoricalVocabularies;

    private final NumericFeatureEncoder
            numericFeatureEncoder;


    /**
     * Encoder bắt buộc phải nhận feature contract.
     *
     * <p>
     * Không tạo constructor không tham số vì nếu encoder
     * tự load application.yaml thì configuration sẽ bị
     * phân tán trở lại.
     * </p>
     */
    public GoldFeatureEncoder(
            GoldFeatureContract contract
    ) {

        this.contract =
                Objects.requireNonNull(
                        contract,
                        "contract must not be null"
                );

        /*
         * Tạo copy và sort theo index.
         *
         * Nhờ vậy thứ tự trong tensor luôn là:
         *
         * x_cat[:, feature.index]
         *
         * chứ không phụ thuộc vào thứ tự List từ YAML.
         */
        this.categoricalFeatures =
                new ArrayList<>(
                        contract.categoricalFeatures()
                );

        this.categoricalFeatures.sort(
                Comparator.comparingInt(
                        GoldFeatureContract
                                .CategoricalFeature::index
                )
        );

        this.numericFeatures =
                new ArrayList<>(
                        contract.numericFeatures()
                );

        this.numericFeatures.sort(
                Comparator.comparingInt(
                        GoldFeatureContract
                                .NumericFeature::index
                )
        );

        /*
         * Build runtime vocabulary từ contract.
         */
        this.categoricalVocabularies =
                new ArrayList<>();

        for (
                GoldFeatureContract.CategoricalFeature feature
                        : categoricalFeatures
        ) {

            categoricalVocabularies.add(
                    GoldCategoricalVocabularies
                            .fromFeature(
                                    feature
                            )
            );
        }

        /*
         * Numeric encoder nhận normalization policy
         * từ cùng một contract.
         */
        this.numericFeatureEncoder =
                new NumericFeatureEncoder(
                        contract.numericMissingValue(),
                        contract.normalizedMin(),
                        contract.normalizedMax()
                );
    }


    /**
     * Encode một sequence model-ready.
     */
    public GoldModelInput encode(
            List<GoldSequenceEvent> sequence
    ) {

        Objects.requireNonNull(
                sequence,
                "sequence must not be null"
        );

        int sequenceLength =
                contract.sequenceLength();

        int categoricalFeatureCount =
                contract.categoricalFeatureCount();

        int numericFeatureCount =
                contract.numericFeatureCount();


        /*
         * Không còn hard-code 32 trong encoder.
         *
         * Với feature-contract v1 giá trị vẫn là 32.
         */
        if (sequence.size() != sequenceLength) {

            throw new GoldFeatureEncodingException(
                    "sequence",
                    GoldFeatureEncodingException
                            .Reason.INVALID_SEQUENCE_LENGTH,
                    String.valueOf(sequence.size()),
                    "Gold sequence must contain exactly "
                            + sequenceLength
                            + " events, but received "
                            + sequence.size()
            );
        }


        /*
         * Shape tensor lấy trực tiếp từ contract.
         *
         * Contract v1:
         *
         * x_cat[32][4]
         * x_num[32][2]
         */
        long[][] xCat =
                new long[
                        sequenceLength
                        ][
                        categoricalFeatureCount
                        ];

        float[][] xNum =
                new float[
                        sequenceLength
                        ][
                        numericFeatureCount
                        ];


        for (
                int timestep = 0;
                timestep < sequenceLength;
                timestep++
        ) {

            GoldSequenceEvent event =
                    sequence.get(
                            timestep
                    );

            if (event == null) {

                throw new GoldFeatureEncodingException(
                        "sequence",
                        GoldFeatureEncodingException
                                .Reason.NULL_SEQUENCE_EVENT,
                        String.valueOf(timestep),
                        "Sequence contains null event at timestep "
                                + timestep
                );
            }


            /*
             * =====================================================
             * CATEGORICAL FEATURES
             * =====================================================
             */

            for (
                    int position = 0;
                    position < categoricalFeatures.size();
                    position++
            ) {

                GoldFeatureContract.CategoricalFeature feature =
                        categoricalFeatures.get(
                                position
                        );

                CategoricalVocabulary vocabulary =
                        categoricalVocabularies.get(
                                position
                        );

                String rawValue =
                        resolveCategoricalSource(
                                event,
                                feature.source()
                        );

                xCat[timestep][feature.index()] =
                        vocabulary.encode(
                                rawValue
                        );
            }


            /*
             * =====================================================
             * NUMERIC FEATURES
             * =====================================================
             */

            for (
                    GoldFeatureContract.NumericFeature feature
                            : numericFeatures
            ) {

                Number rawValue =
                        resolveNumericSource(
                                event,
                                feature.source()
                        );

                xNum[timestep][feature.index()] =
                        numericFeatureEncoder.encode(
                                feature,
                                rawValue
                        );
            }
        }


        return new GoldModelInput(
                xCat,
                xNum
        );
    }


    /**
     * Resolve categorical source khai báo trong YAML.
     *
     * <p>
     * Contract hiện tại:
     * </p>
     *
     * <pre>
     * eventId
     * eventResult
     * rawFields.CAUSE_CODE
     * rawFields.SUB_CAUSE_CODE
     * </pre>
     *
     * <p>
     * GoldSequenceEvent đã project hai raw field cuối thành:
     *
     * normalizedCauseCode
     * subCauseCode
     *
     * nên encoder sử dụng hai field đó.
     * </p>
     */
    private static String resolveCategoricalSource(
            GoldSequenceEvent event,
            String source
    ) {

        return switch (source) {

            case "eventId" ->
                    event.getEventId();

            case "eventResult" ->
                    event.getEventResult();

            case "rawFields.CAUSE_CODE" ->
                    event.getNormalizedCauseCode();

            case "rawFields.SUB_CAUSE_CODE" ->
                    event.getSubCauseCode();

            default ->
                    throw new IllegalArgumentException(
                            "Unsupported categorical feature source: "
                                    + source
                    );
        };
    }


    /**
     * Resolve numeric source khai báo trong YAML.
     *
     * Contract hiện tại:
     *
     * <pre>
     * durationMs
     * requestRetries
     * </pre>
     */
    private static Number resolveNumericSource(
            GoldSequenceEvent event,
            String source
    ) {

        return switch (source) {

            case "durationMs" ->
                    event.getDurationMs();

            case "requestRetries" ->
                    event.getRequestRetries();

            default ->
                    throw new IllegalArgumentException(
                            "Unsupported numeric feature source: "
                                    + source
                    );
        };
    }
}