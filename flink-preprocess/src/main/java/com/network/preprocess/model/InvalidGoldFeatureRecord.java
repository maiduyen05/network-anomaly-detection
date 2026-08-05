package com.network.preprocess.model;

import com.network.preprocess.gold.feature
        .GoldFeatureEncodingException;

import java.io.Serializable;
import java.util.Objects;

/**
 * Record được tạo khi một Gold window không thể encode
 * theo feature contract.
 *
 * <p>Ví dụ:</p>
 *
 * <ul>
 *     <li>EVENT_ID không thuộc vocabulary.</li>
 *     <li>EVENT_RESULT không phải success/reject.</li>
 *     <li>CAUSE_CODE không thuộc vocabulary.</li>
 *     <li>Category bắt buộc bị null.</li>
 * </ul>
 *
 * <p>Giữ rejectedWindow để có thể điều tra hoặc replay.</p>
 */
public class InvalidGoldFeatureRecord
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private String schemaVersion;
    private String invalidFeatureId;

    private String sampleId;
    private String ueKey;
    private String imsi;
    private String featureVersion;

    private String featureName;
    private GoldFeatureEncodingException.Reason reason;
    private String rejectedValue;
    private String errorMessage;
    private String failedAt;

    private GoldSequenceWindow rejectedWindow;

    /**
     * Constructor rỗng cho Jackson và Flink.
     */
    public InvalidGoldFeatureRecord() {
    }

    public InvalidGoldFeatureRecord(
            String schemaVersion,
            String invalidFeatureId,
            String sampleId,
            String ueKey,
            String imsi,
            String featureVersion,
            String featureName,
            GoldFeatureEncodingException.Reason reason,
            String rejectedValue,
            String errorMessage,
            String failedAt,
            GoldSequenceWindow rejectedWindow
    ) {
        this.schemaVersion =
                requiredText(
                        schemaVersion,
                        "schemaVersion"
                );

        this.invalidFeatureId =
                requiredText(
                        invalidFeatureId,
                        "invalidFeatureId"
                );

        this.sampleId =
                requiredText(
                        sampleId,
                        "sampleId"
                );

        this.ueKey =
                requiredText(
                        ueKey,
                        "ueKey"
                );

        this.imsi =
                requiredText(
                        imsi,
                        "imsi"
                );

        this.featureVersion =
                requiredText(
                        featureVersion,
                        "featureVersion"
                );

        this.featureName =
                requiredText(
                        featureName,
                        "featureName"
                );

        this.reason =
                Objects.requireNonNull(
                        reason,
                        "reason must not be null"
                );

        /*
         * rejectedValue có thể null khi dữ liệu bị thiếu.
         */
        this.rejectedValue = rejectedValue;

        this.errorMessage =
                requiredText(
                        errorMessage,
                        "errorMessage"
                );

        this.failedAt =
                requiredText(
                        failedAt,
                        "failedAt"
                );

        this.rejectedWindow =
                Objects.requireNonNull(
                        rejectedWindow,
                        "rejectedWindow must not be null"
                );
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getInvalidFeatureId() {
        return invalidFeatureId;
    }

    public void setInvalidFeatureId(
            String invalidFeatureId
    ) {
        this.invalidFeatureId = invalidFeatureId;
    }

    public String getSampleId() {
        return sampleId;
    }

    public void setSampleId(String sampleId) {
        this.sampleId = sampleId;
    }

    public String getUeKey() {
        return ueKey;
    }

    public void setUeKey(String ueKey) {
        this.ueKey = ueKey;
    }

    public String getImsi() {
        return imsi;
    }

    public void setImsi(String imsi) {
        this.imsi = imsi;
    }

    public String getFeatureVersion() {
        return featureVersion;
    }

    public void setFeatureVersion(
            String featureVersion
    ) {
        this.featureVersion = featureVersion;
    }

    public String getFeatureName() {
        return featureName;
    }

    public void setFeatureName(String featureName) {
        this.featureName = featureName;
    }

    public GoldFeatureEncodingException.Reason getReason() {
        return reason;
    }

    public void setReason(
            GoldFeatureEncodingException.Reason reason
    ) {
        this.reason = reason;
    }

    public String getRejectedValue() {
        return rejectedValue;
    }

    public void setRejectedValue(
            String rejectedValue
    ) {
        this.rejectedValue = rejectedValue;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(
            String errorMessage
    ) {
        this.errorMessage = errorMessage;
    }

    public String getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(String failedAt) {
        this.failedAt = failedAt;
    }

    public GoldSequenceWindow getRejectedWindow() {
        return rejectedWindow;
    }

    public void setRejectedWindow(
            GoldSequenceWindow rejectedWindow
    ) {
        this.rejectedWindow = rejectedWindow;
    }

    private static String requiredText(
            String value,
            String fieldName
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    fieldName + " must not be blank"
            );
        }

        return value.trim();
    }
}