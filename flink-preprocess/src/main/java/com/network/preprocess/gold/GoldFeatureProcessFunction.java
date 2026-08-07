package com.network.preprocess.gold;

import com.network.preprocess.gold.feature
        .GoldFeatureEncodingException;
import com.network.preprocess.model.GoldSequenceSample;
import com.network.preprocess.model.GoldSequenceWindow;
import com.network.preprocess.model.InvalidGoldFeatureRecord;
import com.network.preprocess.config.GoldFeatureContract;
import org.apache.flink.streaming.api.functions
        .ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


import java.time.Instant;
import java.util.Objects;

/**
 * Chuyển GoldSequenceWindow thành GoldSequenceSample model-ready.
 *
 * <p>Main output:</p>
 *
 * <pre>
 * GoldSequenceSample
 * </pre>
 *
 * <p>Side output:</p>
 *
 * <pre>
 * InvalidGoldFeatureRecord
 * </pre>
 *
 * <p>Chỉ lỗi dữ liệu GoldFeatureEncodingException được route sang
 * side output. Những RuntimeException khác vẫn phải làm job fail,
 * vì chúng có thể là lỗi lập trình chứ không phải lỗi dữ liệu.</p>
 */
public final class GoldFeatureProcessFunction
        extends ProcessFunction<
                GoldSequenceWindow,
                GoldSequenceSample> {

    /**
     * Side output chứa các window vi phạm feature contract.
     */
    public static final OutputTag<InvalidGoldFeatureRecord>
            INVALID_FEATURE_TAG =
            new OutputTag<InvalidGoldFeatureRecord>(
                    "gold-invalid-feature-records"
            ) {
            };

    private final String invalidRecordSchemaVersion;
    private final GoldSequenceSampleFactory sampleFactory;

        public GoldFeatureProcessFunction(
                String invalidRecordSchemaVersion,
                GoldFeatureContract featureContract
        ) {

        if (invalidRecordSchemaVersion == null
                || invalidRecordSchemaVersion.isBlank()) {

                throw new IllegalArgumentException(
                        "invalidRecordSchemaVersion must not be blank"
                );
        }

        this.invalidRecordSchemaVersion =
                invalidRecordSchemaVersion.trim();

        this.sampleFactory =
                new GoldSequenceSampleFactory(
                        Objects.requireNonNull(
                                featureContract,
                                "featureContract must not be null"
                        )
                );
        }

    @Override
    public void processElement(
            GoldSequenceWindow window,
            Context context,
            Collector<GoldSequenceSample> output
    ) {
        Objects.requireNonNull(
                window,
                "window must not be null"
        );

        try {
            /*
             * Window hợp lệ được encode và đưa ra main output.
             */
            GoldSequenceSample sample =
                    sampleFactory.create(
                            window
                    );

            output.collect(
                    sample
            );

        } catch (GoldFeatureEncodingException exception) {
            /*
             * ID deterministic:
             *
             * Cùng sample + cùng feature lỗi + cùng reason
             * sẽ tạo cùng invalidFeatureId.
             */
            String invalidFeatureId =
                    window.sampleId()
                            + ":"
                            + exception.getFeatureName()
                            + ":"
                            + exception.getReason().name();

            long failedAtEpochMs =
                    context
                            .timerService()
                            .currentProcessingTime();

            InvalidGoldFeatureRecord invalidRecord =
                    new InvalidGoldFeatureRecord(
                            invalidRecordSchemaVersion,
                            invalidFeatureId,

                            window.sampleId(),
                            window.ueKey(),
                            window.imsi(),
                            window.featureVersion(),

                            exception.getFeatureName(),
                            exception.getReason(),
                            exception.getRejectedValue(),
                            exception.getMessage(),

                            Instant
                                    .ofEpochMilli(
                                            failedAtEpochMs
                                    )
                                    .toString(),

                            window
                    );

            context.output(
                    INVALID_FEATURE_TAG,
                    invalidRecord
            );
        }
    }
}