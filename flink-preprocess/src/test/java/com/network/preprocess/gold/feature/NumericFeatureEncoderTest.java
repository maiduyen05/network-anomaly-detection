package com.network.preprocess.gold.feature;

import com.network.preprocess.config.GoldFeatureContract;
import com.network.preprocess.config.GoldJobConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NumericFeatureEncoderTest {

    private static final float DELTA =
            0.000001F;

    /**
     * Feature duration_ms lấy từ contract.
     */
    private static GoldFeatureContract.NumericFeature
    durationFeature() {

        return contract()
                .numericFeatures()
                .stream()
                .filter(
                        feature ->
                                feature.name()
                                        .equals("duration_ms")
                )
                .findFirst()
                .orElseThrow();
    }

    /**
     * Feature request_retries lấy từ contract.
     */
    private static GoldFeatureContract.NumericFeature
    retriesFeature() {

        return contract()
                .numericFeatures()
                .stream()
                .filter(
                        feature ->
                                feature.name()
                                        .equals("request_retries")
                )
                .findFirst()
                .orElseThrow();
    }

    private static GoldFeatureContract contract() {

        return GoldJobConfig
                .loadFromClasspath(
                        "application.yaml"
                )
                .featureContract();
    }

    private static NumericFeatureEncoder encoder() {

        GoldFeatureContract contract =
                contract();

        return new NumericFeatureEncoder(
                contract.numericMissingValue(),
                contract.normalizedMin(),
                contract.normalizedMax()
        );
    }


    @Test
    void shouldReturnMissingValueForNullDuration() {

        assertEquals(
                -1.0F,
                encoder().encode(
                        durationFeature(),
                        null
                ),
                DELTA
        );
    }


    @Test
    void shouldClipNegativeDurationToZero() {

        assertEquals(
                0.0F,
                encoder().encode(
                        durationFeature(),
                        -100L
                ),
                DELTA
        );
    }


    @Test
    void shouldEncodeMaximumDurationAsOne() {

        assertEquals(
                1.0F,
                encoder().encode(
                        durationFeature(),
                        600_000L
                ),
                DELTA
        );
    }


    @Test
    void shouldClipDurationAboveMaximum() {

        assertEquals(
                1.0F,
                encoder().encode(
                        durationFeature(),
                        900_000L
                ),
                DELTA
        );
    }


    @Test
    void shouldApplyLog1pMinmaxToDuration() {

        float actual =
                encoder().encode(
                        durationFeature(),
                        1_000L
                );

        float expected =
                (float) (
                        Math.log1p(1_000.0)
                                / Math.log1p(600_000.0)
                );

        assertEquals(
                expected,
                actual,
                DELTA
        );

        assertTrue(
                actual > 0.0F
        );

        assertTrue(
                actual < 1.0F
        );
    }


    @Test
    void shouldReturnMissingValueForNullRetries() {

        assertEquals(
                -1.0F,
                encoder().encode(
                        retriesFeature(),
                        null
                ),
                DELTA
        );
    }


    @Test
    void shouldClipNegativeRetriesToZero() {

        assertEquals(
                0.0F,
                encoder().encode(
                        retriesFeature(),
                        -5
                ),
                DELTA
        );
    }


    @Test
    void shouldEncodeFiveRetriesAsOneHalf() {

        assertEquals(
                0.5F,
                encoder().encode(
                        retriesFeature(),
                        5
                ),
                DELTA
        );
    }


    @Test
    void shouldEncodeTenRetriesAsOne() {

        assertEquals(
                1.0F,
                encoder().encode(
                        retriesFeature(),
                        10
                ),
                DELTA
        );
    }


    @Test
    void shouldClipRetriesAboveMaximum() {

        assertEquals(
                1.0F,
                encoder().encode(
                        retriesFeature(),
                        20
                ),
                DELTA
        );
    }
}