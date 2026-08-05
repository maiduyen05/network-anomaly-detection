package com.network.preprocess.gold.feature;

/**
 * Mã hóa hai numeric feature thành FLOAT32.
 *
 * <pre>
 * x_num[32][0] = duration_ms
 * x_num[32][1] = request_retries
 * </pre>
 */
public final class NumericFeatureEncoder {

    public static final float MISSING_VALUE = -1.0F;

    public static final long DURATION_MIN_MS = 0L;
    public static final long DURATION_MAX_MS = 600_000L;

    public static final int RETRIES_MIN = 0;
    public static final int RETRIES_MAX = 10;

    private NumericFeatureEncoder() {
        // Utility class không cần tạo object.
    }

    /**
     * Encode duration theo log1p_minmax.
     *
     * <pre>
     * clipped = clip(duration, 0, 600000)
     *
     * normalized =
     *     log(1 + clipped)
     *     ----------------
     *     log(1 + 600000)
     * </pre>
     */
    public static float encodeDurationMs(Long rawDurationMs) {
        if (rawDurationMs == null) {
            return MISSING_VALUE;
        }

        long clippedDuration = Math.max(
                DURATION_MIN_MS,
                Math.min(rawDurationMs, DURATION_MAX_MS)
        );

        double normalized =
                Math.log1p(clippedDuration)
                        / Math.log1p(DURATION_MAX_MS);

        return clampToUnitRange((float) normalized);
    }

    /**
     * Encode requestRetries theo clipped min-max.
     *
     * <pre>
     * clipped = clip(requestRetries, 0, 10)
     * normalized = clipped / 10
     * </pre>
     */
    public static float encodeRequestRetries(
            Integer rawRequestRetries
    ) {
        if (rawRequestRetries == null) {
            return MISSING_VALUE;
        }

        int clippedRetries = Math.max(
                RETRIES_MIN,
                Math.min(rawRequestRetries, RETRIES_MAX)
        );

        float normalized =
                (float) clippedRetries
                        / (float) RETRIES_MAX;

        return clampToUnitRange(normalized);
    }

    private static float clampToUnitRange(float value) {
        return Math.max(
                0.0F,
                Math.min(value, 1.0F)
        );
    }
}