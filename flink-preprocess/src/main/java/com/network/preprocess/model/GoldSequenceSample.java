package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Gold sample model-ready được ghi vào gold.ue.sequence.
 *
 * <p>Mỗi sample chứa hai phần:</p>
 *
 * <ul>
 *     <li>modelInput: tensor được đưa trực tiếp vào model.</li>
 *     <li>evidence: 32 event nguồn phục vụ audit và hiển thị.</li>
 * </ul>
 *
 * <p>event time là metadata của window, không được đưa vào tensor.</p>
 */
public class GoldSequenceSample implements Serializable {

    private static final long serialVersionUID = 1L;

    /*
     * =========================================================
     * VERSION VÀ IDENTITY
     * =========================================================
     */

    private String schemaVersion;
    private String featureVersion;
    private String sampleId;
    private String ueKey;
    private String imsi;

    /*
     * =========================================================
     * THÔNG TIN WINDOW
     * =========================================================
     *
     * Lưu timestamp dưới dạng ISO-8601 String để:
     * - JSON dễ đọc;
     * - không cần cấu hình JavaTimeModule cho Jackson;
     * - không đưa Instant vào object graph của Flink.
     */

    private String windowStartEventTime;
    private String windowEndEventTime;

    private int sequenceLength;
    private int stride;

    /*
     * =========================================================
     * MODEL INPUT VÀ EVIDENCE
     * =========================================================
     */

    private GoldModelInput modelInput;
    private GoldEvidence evidence;

    /**
     * Constructor rỗng cho Jackson và Flink POJO serializer.
     */
    public GoldSequenceSample() {
    }

    public GoldSequenceSample(
            String schemaVersion,
            String featureVersion,
            String sampleId,
            String ueKey,
            String imsi,
            String windowStartEventTime,
            String windowEndEventTime,
            int sequenceLength,
            int stride,
            GoldModelInput modelInput,
            GoldEvidence evidence
    ) {
        this.schemaVersion =
                requiredText(
                        schemaVersion,
                        "schemaVersion"
                );

        this.featureVersion =
                requiredText(
                        featureVersion,
                        "featureVersion"
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

        this.windowStartEventTime =
                requiredText(
                        windowStartEventTime,
                        "windowStartEventTime"
                );

        this.windowEndEventTime =
                requiredText(
                        windowEndEventTime,
                        "windowEndEventTime"
                );

        if (sequenceLength <= 0) {
            throw new IllegalArgumentException(
                    "sequenceLength must be positive"
            );
        }

        if (stride <= 0 || stride > sequenceLength) {
            throw new IllegalArgumentException(
                    "stride must be between 1 and sequenceLength"
            );
        }

        this.sequenceLength = sequenceLength;
        this.stride = stride;

        this.modelInput =
                Objects.requireNonNull(
                        modelInput,
                        "modelInput must not be null"
                );

        this.evidence =
                Objects.requireNonNull(
                        evidence,
                        "evidence must not be null"
                );
    }

    /*
     * =========================================================
     * GETTERS VÀ SETTERS DÀNH CHO FLINK/JACKSON
     * =========================================================
     */

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getFeatureVersion() {
        return featureVersion;
    }

    public void setFeatureVersion(String featureVersion) {
        this.featureVersion = featureVersion;
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

    public String getWindowStartEventTime() {
        return windowStartEventTime;
    }

    public void setWindowStartEventTime(
            String windowStartEventTime
    ) {
        this.windowStartEventTime =
                windowStartEventTime;
    }

    public String getWindowEndEventTime() {
        return windowEndEventTime;
    }

    public void setWindowEndEventTime(
            String windowEndEventTime
    ) {
        this.windowEndEventTime =
                windowEndEventTime;
    }

    public int getSequenceLength() {
        return sequenceLength;
    }

    public void setSequenceLength(int sequenceLength) {
        this.sequenceLength = sequenceLength;
    }

    public int getStride() {
        return stride;
    }

    public void setStride(int stride) {
        this.stride = stride;
    }

    public GoldModelInput getModelInput() {
        return modelInput;
    }

    public void setModelInput(
            GoldModelInput modelInput
    ) {
        this.modelInput = modelInput;
    }

    public GoldEvidence getEvidence() {
        return evidence;
    }

    public void setEvidence(
            GoldEvidence evidence
    ) {
        this.evidence = evidence;
    }

    /*
     * Accessor dạng record dùng cho code nghiệp vụ.
     */

    public String sampleId() {
        return sampleId;
    }

    public String ueKey() {
        return ueKey;
    }

    public GoldModelInput modelInput() {
        return modelInput;
    }

    public GoldEvidence evidence() {
        return evidence;
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