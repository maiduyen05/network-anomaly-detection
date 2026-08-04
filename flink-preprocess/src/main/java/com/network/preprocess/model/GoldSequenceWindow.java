package com.network.preprocess.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Một Gold sliding sequence đã đủ số lượng event.
 *
 * <p>Lớp được viết theo chuẩn Flink POJO để có thể:</p>
 *
 * <ul>
 *     <li>Đi qua network shuffle.</li>
 *     <li>Được Flink copy giữa các operator.</li>
 *     <li>Được lưu trong state nếu cần ở checkpoint sau.</li>
 *     <li>Không phụ thuộc Kryo FieldSerializer.</li>
 * </ul>
 */
public class GoldSequenceWindow implements Serializable {

    private static final long serialVersionUID = 1L;

    private String schemaVersion;
    private String featureVersion;
    private String sampleId;
    private String ueKey;
    private String imsi;

    /**
     * Dùng long để tránh đưa java.time.Instant vào object graph
     * mà Flink cần serialize.
     */
    private long windowStartEventTimeEpochMs;
    private long windowEndEventTimeEpochMs;

    private int sequenceLength;
    private int stride;

    /**
     * Khai báo List với generic type cụ thể để Flink biết
     * chính xác kiểu phần tử cần serialize.
     */
    private List<GoldSequenceEvent> events =
            new ArrayList<>();

    /**
     * Constructor rỗng bắt buộc cho Flink POJO.
     */
    public GoldSequenceWindow() {
    }

    public GoldSequenceWindow(
            String schemaVersion,
            String featureVersion,
            String sampleId,
            String ueKey,
            String imsi,
            Instant windowStartEventTime,
            Instant windowEndEventTime,
            int sequenceLength,
            int stride,
            List<GoldSequenceEvent> events
    ) {
        this.schemaVersion =
                requiredText(schemaVersion, "schemaVersion");

        this.featureVersion =
                requiredText(featureVersion, "featureVersion");

        this.sampleId =
                requiredText(sampleId, "sampleId");

        this.ueKey =
                requiredText(ueKey, "ueKey");

        this.imsi =
                requiredText(imsi, "imsi");

        this.windowStartEventTimeEpochMs =
                Objects.requireNonNull(
                        windowStartEventTime,
                        "windowStartEventTime must not be null"
                ).toEpochMilli();

        this.windowEndEventTimeEpochMs =
                Objects.requireNonNull(
                        windowEndEventTime,
                        "windowEndEventTime must not be null"
                ).toEpochMilli();

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

        Objects.requireNonNull(
                events,
                "events must not be null"
        );

        if (events.size() != sequenceLength) {
            throw new IllegalArgumentException(
                    "Expected "
                            + sequenceLength
                            + " events but received "
                            + events.size()
            );
        }

        this.sequenceLength = sequenceLength;
        this.stride = stride;

        /*
         * Không giữ List.of() hoặc unmodifiable list.
         */
        this.events = new ArrayList<>(events);
    }

    /*
     * JavaBean getters/setters dành cho Flink.
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

    public long getWindowStartEventTimeEpochMs() {
        return windowStartEventTimeEpochMs;
    }

    public void setWindowStartEventTimeEpochMs(
            long windowStartEventTimeEpochMs
    ) {
        this.windowStartEventTimeEpochMs =
                windowStartEventTimeEpochMs;
    }

    public long getWindowEndEventTimeEpochMs() {
        return windowEndEventTimeEpochMs;
    }

    public void setWindowEndEventTimeEpochMs(
            long windowEndEventTimeEpochMs
    ) {
        this.windowEndEventTimeEpochMs =
                windowEndEventTimeEpochMs;
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

    public List<GoldSequenceEvent> getEvents() {
        return events;
    }

    public void setEvents(
            List<GoldSequenceEvent> events
    ) {
        if (events == null) {
            this.events = new ArrayList<>();
        } else {
            this.events = new ArrayList<>(events);
        }
    }

    /*
     * Accessor kiểu record để code Checkpoint 11 không phải sửa.
     */

    public String schemaVersion() {
        return schemaVersion;
    }

    public String featureVersion() {
        return featureVersion;
    }

    public String sampleId() {
        return sampleId;
    }

    public String ueKey() {
        return ueKey;
    }

    public String imsi() {
        return imsi;
    }

    public Instant windowStartEventTime() {
        return Instant.ofEpochMilli(
                windowStartEventTimeEpochMs
        );
    }

    public Instant windowEndEventTime() {
        return Instant.ofEpochMilli(
                windowEndEventTimeEpochMs
        );
    }

    public int sequenceLength() {
        return sequenceLength;
    }

    public int stride() {
        return stride;
    }

    public List<GoldSequenceEvent> events() {
        return events;
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