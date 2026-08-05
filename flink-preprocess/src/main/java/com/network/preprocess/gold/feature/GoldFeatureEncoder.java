package com.network.preprocess.gold.feature;

import com.network.preprocess.model.GoldModelInput;
import com.network.preprocess.model.GoldSequenceEvent;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Chuyển đúng 32 GoldSequenceEvent thành:
 *
 * <pre>
 * x_cat: long[32][4]
 * x_num: float[32][2]
 * </pre>
 *
 * <p>Class này không:</p>
 *
 * <ul>
 *     <li>Sắp xếp lại event.</li>
 *     <li>Tạo sequence.</li>
 *     <li>Trượt theo stride.</li>
 *     <li>Tự thêm category mới.</li>
 * </ul>
 * Chỉ encode đúng 32 event đã được sắp xếp theo event time.
 */
public final class GoldFeatureEncoder
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int SEQUENCE_LENGTH = 32;
    public static final int CATEGORICAL_FEATURE_COUNT = 4;
    public static final int NUMERIC_FEATURE_COUNT = 2;

    private final CategoricalVocabulary eventCodeVocabulary;
    private final CategoricalVocabulary eventResultVocabulary;
    private final CategoricalVocabulary normalizedCauseVocabulary;
    private final CategoricalVocabulary subCauseVocabulary;

    public GoldFeatureEncoder() {
        this.eventCodeVocabulary =
                GoldCategoricalVocabularies.eventCode();

        this.eventResultVocabulary =
                GoldCategoricalVocabularies.eventResultCode();

        this.normalizedCauseVocabulary =
                GoldCategoricalVocabularies.normalizedCauseCode();

        this.subCauseVocabulary =
                GoldCategoricalVocabularies.subCauseCode();
    }

    /**
     * Encode một sequence đủ 32 event.
     *
     * @param sequence danh sách đã được Checkpoint 11 sắp theo event time
     * @return hai tensor đúng datatype và shape
     */
    public GoldModelInput encode(
            List<GoldSequenceEvent> sequence
    ) {
        Objects.requireNonNull(
                sequence,
                "sequence must not be null"
        );

        if (sequence.size() != SEQUENCE_LENGTH) {
            throw new GoldFeatureEncodingException(
                    "sequence",
                    GoldFeatureEncodingException
                            .Reason.INVALID_SEQUENCE_LENGTH,
                    String.valueOf(sequence.size()),
                    "Gold sequence must contain exactly "
                            + SEQUENCE_LENGTH
                            + " events, but received "
                            + sequence.size()
            );
        }

        long[][] xCat =
                new long[
                        SEQUENCE_LENGTH
                        ][
                        CATEGORICAL_FEATURE_COUNT
                        ];

        float[][] xNum =
                new float[
                        SEQUENCE_LENGTH
                        ][
                        NUMERIC_FEATURE_COUNT
                        ];

        for (int timestep = 0;
             timestep < SEQUENCE_LENGTH;
             timestep++) {

            GoldSequenceEvent event =
                    sequence.get(timestep);

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
             * x_cat[timestep][0] = event_code
             */
            xCat[timestep][0] =
                    eventCodeVocabulary.encode(
                            event.getEventId()
                    );

            /*
             * x_cat[timestep][1] = event_result_code
             */
            xCat[timestep][1] =
                    eventResultVocabulary.encode(
                            event.getEventResult()
                    );

            /*
             * x_cat[timestep][2] = normalized_cause_code.
             *
             * Chuỗi rỗng là category hợp lệ và trả về ID 0.
             */
            xCat[timestep][2] =
                    normalizedCauseVocabulary.encode(
                            event.getNormalizedCauseCode()
                    );

            /*
             * x_cat[timestep][3] = sub_cause_code.
             *
             * Chuỗi rỗng cũng là category hợp lệ và trả về ID 0.
             */
            xCat[timestep][3] =
                    subCauseVocabulary.encode(
                            event.getSubCauseCode()
                    );

            /*
             * x_num[timestep][0] = duration_ms.
             */
            xNum[timestep][0] =
                    NumericFeatureEncoder.encodeDurationMs(
                            event.getDurationMs()
                    );

            /*
             * x_num[timestep][1] = request_retries.
             */
            xNum[timestep][1] =
                    NumericFeatureEncoder
                            .encodeRequestRetries(
                                    event.getRequestRetries()
                            );
        }

        return new GoldModelInput(xCat, xNum);
    }
}