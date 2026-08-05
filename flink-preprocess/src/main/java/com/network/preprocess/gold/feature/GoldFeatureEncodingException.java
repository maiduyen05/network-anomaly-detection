package com.network.preprocess.gold.feature;

/**
 * Lỗi xảy ra khi Gold không thể chuyển dữ liệu event
 * thành tensor đúng theo feature contract.
 *
 * <p>Đây là lỗi dữ liệu, không phải lỗi tạm thời. Việc retry cùng
 * một record sẽ không làm category không hợp lệ trở thành hợp lệ.</p>
 */
public final class GoldFeatureEncodingException
        extends RuntimeException {

    /**
     * Phân loại nguyên nhân lỗi để Checkpoint sau có thể:
     *
     * <ul>
     *     <li>Đếm metric theo từng loại lỗi.</li>
     *     <li>Đưa record lỗi sang side output.</li>
     *     <li>Ghi rõ nguyên nhân vào invalid-gold-feature topic.</li>
     * </ul>
     */
    public enum Reason {
        MISSING_VALUE,
        UNKNOWN_CATEGORY,
        INVALID_VOCABULARY,
        INVALID_SEQUENCE_LENGTH,
        NULL_SEQUENCE_EVENT
    }

    private final String featureName;
    private final Reason reason;
    private final String rejectedValue;

    public GoldFeatureEncodingException(
            String featureName,
            Reason reason,
            String rejectedValue,
            String message
    ) {
        super(message);

        this.featureName = featureName;
        this.reason = reason;
        this.rejectedValue = rejectedValue;
    }

    public String getFeatureName() {
        return featureName;
    }

    public Reason getReason() {
        return reason;
    }

    public String getRejectedValue() {
        return rejectedValue;
    }
}