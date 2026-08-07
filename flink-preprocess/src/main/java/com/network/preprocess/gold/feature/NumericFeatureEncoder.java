package com.network.preprocess.gold.feature;

import com.network.preprocess.config.GoldFeatureContract;

import java.io.Serializable;
import java.util.Objects;

/**
 * Encode một numeric feature theo đúng GoldFeatureContract.
 *
 * <p>
 * Class này không hard-code:
 * </p>
 *
 * <ul>
 *     <li>missing value;</li>
 *     <li>raw clip min;</li>
 *     <li>raw clip max;</li>
 *     <li>transform.</li>
 * </ul>
 *
 * <p>
 * Các giá trị trên được lấy từ application.yaml.
 * </p>
 */
public final class NumericFeatureEncoder
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final float missingValue;
    private final float normalizedMin;
    private final float normalizedMax;

    public NumericFeatureEncoder(
            float missingValue,
            float normalizedMin,
            float normalizedMax
    ) {

        if (normalizedMax < normalizedMin) {
            throw new IllegalArgumentException(
                    "normalizedMax must be >= normalizedMin"
            );
        }

        this.missingValue = missingValue;
        this.normalizedMin = normalizedMin;
        this.normalizedMax = normalizedMax;
    }

    /**
     * Encode numeric value theo feature definition.
     *
     * @param feature feature trong GoldFeatureContract
     * @param rawValue giá trị nguồn; null nghĩa là missing
     */
    public float encode(
            GoldFeatureContract.NumericFeature feature,
            Number rawValue
    ) {

        Objects.requireNonNull(
                feature,
                "feature must not be null"
        );

        /*
         * Missing numeric được biểu diễn bằng đúng giá trị
         * khai báo trong feature contract.
         *
         * Hiện tại:
         *
         * missing-value: -1.0
         */
        if (rawValue == null) {
            return missingValue;
        }

        double raw =
                rawValue.doubleValue();

        /*
         * Clip trước transform.
         */
        double clipped =
                Math.max(
                        feature.rawClipMin(),
                        Math.min(
                                raw,
                                feature.rawClipMax()
                        )
                );

        double normalized =
                switch (feature.transform()) {

                    /*
                     * duration_ms hiện dùng:
                     *
                     * log1p_minmax
                     */
                    case "log1p_minmax" ->
                            encodeLog1pMinMax(
                                    clipped,
                                    feature.rawClipMin(),
                                    feature.rawClipMax()
                            );

                    /*
                     * request_retries hiện dùng:
                     *
                     * clipped_minmax
                     */
                    case "clipped_minmax" ->
                            encodeMinMax(
                                    clipped,
                                    feature.rawClipMin(),
                                    feature.rawClipMax()
                            );

                    default ->
                            throw new IllegalArgumentException(
                                    "Unsupported numeric transform '"
                                            + feature.transform()
                                            + "' for feature '"
                                            + feature.name()
                                            + "'"
                            );
                };

        return clampToConfiguredRange(
                (float) normalized
        );
    }

    /**
     * Min-max normalization:
     *
     * (x - min)
     * ----------
     * (max - min)
     */
    private static double encodeMinMax(
            double value,
            double min,
            double max
    ) {

        if (max == min) {
            return 0.0D;
        }

        return (value - min)
                / (max - min);
    }

    /**
     * Log1p + min-max.
     *
     * <p>
     * Với contract hiện tại min = 0:
     * </p>
     *
     * <pre>
     * log(1 + x)
     * ----------------
     * log(1 + max)
     * </pre>
     *
     * <p>
     * Viết dạng tổng quát để không hard-code min = 0.
     * </p>
     */
    private static double encodeLog1pMinMax(
            double value,
            double min,
            double max
    ) {

        if (max == min) {
            return 0.0D;
        }

        /*
         * log1p chỉ hợp lệ khi input > -1.
         */
        if (min <= -1.0D
                || max <= -1.0D
                || value <= -1.0D) {

            throw new IllegalArgumentException(
                    "log1p_minmax requires values greater than -1"
            );
        }

        double transformedValue =
                Math.log1p(value);

        double transformedMin =
                Math.log1p(min);

        double transformedMax =
                Math.log1p(max);

        return (transformedValue - transformedMin)
                / (transformedMax - transformedMin);
    }

    /**
     * Đưa normalized value về range output
     * được contract khai báo.
     *
     * Hiện tại range là [0, 1].
     */
    private float clampToConfiguredRange(
            float value
    ) {

        return Math.max(
                normalizedMin,
                Math.min(
                        value,
                        normalizedMax
                )
        );
    }
}