package com.network.preprocess.gold.feature;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NumericFeatureEncoderTest {

    private static final float DELTA = 0.000001F;

    @Test
    void shouldReturnMissingValueForNullDuration() {
        assertEquals(
                -1.0F,
                NumericFeatureEncoder.encodeDurationMs(null),
                DELTA
        );
    }

    @Test
    void shouldClipNegativeDurationToZero() {
        assertEquals(
                0.0F,
                NumericFeatureEncoder.encodeDurationMs(-100L),
                DELTA
        );
    }

    @Test
    void shouldEncodeMaximumDurationAsOne() {
        assertEquals(
                1.0F,
                NumericFeatureEncoder
                        .encodeDurationMs(600_000L),
                DELTA
        );
    }

    @Test
    void shouldClipDurationAboveMaximum() {
        assertEquals(
                1.0F,
                NumericFeatureEncoder
                        .encodeDurationMs(900_000L),
                DELTA
        );
    }

    @Test
    void shouldApplyLog1pMinmaxToDuration() {
        float actual =
                NumericFeatureEncoder.encodeDurationMs(1_000L);

        float expected =
                (float) (
                        Math.log1p(1_000.0)
                                / Math.log1p(600_000.0)
                );

        assertEquals(expected, actual, DELTA);
        assertTrue(actual > 0.0F);
        assertTrue(actual < 1.0F);
    }

    @Test
    void shouldReturnMissingValueForNullRetries() {
        assertEquals(
                -1.0F,
                NumericFeatureEncoder
                        .encodeRequestRetries(null),
                DELTA
        );
    }

    @Test
    void shouldClipNegativeRetriesToZero() {
        assertEquals(
                0.0F,
                NumericFeatureEncoder
                        .encodeRequestRetries(-5),
                DELTA
        );
    }

    @Test
    void shouldEncodeFiveRetriesAsOneHalf() {
        assertEquals(
                0.5F,
                NumericFeatureEncoder
                        .encodeRequestRetries(5),
                DELTA
        );
    }

    @Test
    void shouldEncodeTenRetriesAsOne() {
        assertEquals(
                1.0F,
                NumericFeatureEncoder
                        .encodeRequestRetries(10),
                DELTA
        );
    }

    @Test
    void shouldClipRetriesAboveMaximum() {
        assertEquals(
                1.0F,
                NumericFeatureEncoder
                        .encodeRequestRetries(20),
                DELTA
        );
    }
}