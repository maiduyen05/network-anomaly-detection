package com.network.preprocess.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Một event trung gian được tầng Gold dùng để tạo sequence.
 *
 * <p>Lớp này được viết theo chuẩn Flink POJO thay vì Java record.
 * Nhờ vậy Flink sử dụng PojoSerializer, không phải Kryo FieldSerializer.</p>
 *
 * <p>Điều kiện quan trọng để được Flink nhận diện là POJO:</p>
 *
 * <ul>
 *     <li>Lớp là public.</li>
 *     <li>Có public constructor không tham số.</li>
 *     <li>Các field có getter và setter theo JavaBean.</li>
 *     <li>Field dùng kiểu dữ liệu mà Flink có thể serialize.</li>
 * </ul>
 */
public class GoldSequenceEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Thứ tự deterministic dùng khi tạo sequence:
     *
     * <ol>
     *     <li>Sắp xếp theo event time.</li>
     *     <li>Nếu event time giống nhau, sắp theo sourceOrderKey.</li>
     * </ol>
     */
    public static final Comparator<GoldSequenceEvent> EVENT_TIME_ORDER =
            Comparator
                    .comparingLong(
                            GoldSequenceEvent::getEventTimeEpochMs
                    )
                    .thenComparing(
                            GoldSequenceEvent::getSourceOrderKey
                    );

    private String ueKey;
    private String imsi;
    private String eventId;
    private int eventCode;
    private String eventResult;
    private int eventResultCode;
    private long durationMs;

    /**
     * Lưu timestamp dưới dạng long thay vì Instant.
     *
     * <p>long được Flink serialize trực tiếp. Nếu lưu Instant trong state,
     * tùy phiên bản Flink, field này có thể tiếp tục rơi xuống Kryo.</p>
     */
    private long eventTimeEpochMs;

    private Map<String, String> featureSourceFields =
            new LinkedHashMap<>();

    private Map<String, String> displayFields =
            new LinkedHashMap<>();

    private Map<String, String> qualityFields =
            new LinkedHashMap<>();

    private String sourceOrderKey;

    /**
     * Constructor rỗng bắt buộc đối với Flink POJO.
     *
     * <p>Không xóa constructor này, dù code nghiệp vụ không gọi trực tiếp.
     * Flink dùng nó khi khôi phục object từ state/checkpoint.</p>
     */
    public GoldSequenceEvent() {
    }

    /**
     * Constructor đầy đủ được code nghiệp vụ và unit test sử dụng.
     */
    public GoldSequenceEvent(
            String ueKey,
            String imsi,
            String eventId,
            int eventCode,
            String eventResult,
            int eventResultCode,
            long durationMs,
            Instant eventTime,
            Map<String, String> featureSourceFields,
            Map<String, String> displayFields,
            Map<String, String> qualityFields,
            String sourceOrderKey
    ) {
        this.ueKey = requiredText(ueKey, "ueKey");
        this.imsi = requiredText(imsi, "imsi");
        this.eventId = requiredText(eventId, "eventId");
        this.eventCode = eventCode;
        this.eventResult =
                requiredText(eventResult, "eventResult");
        this.eventResultCode = eventResultCode;
        this.durationMs = durationMs;

        this.eventTimeEpochMs =
                Objects.requireNonNull(
                        eventTime,
                        "eventTime must not be null"
                ).toEpochMilli();

        /*
         * Luôn tạo mutable copy.
         *
         * Không giữ Map.of() hoặc Collections.unmodifiableMap(),
         * vì Flink có thể copy/khôi phục collection trong state.
         */
        this.featureSourceFields =
                mutableCopy(featureSourceFields);

        this.displayFields =
                mutableCopy(displayFields);

        this.qualityFields =
                mutableCopy(qualityFields);

        this.sourceOrderKey =
                requiredText(sourceOrderKey, "sourceOrderKey");
    }

    /*
     * =========================================================
     * JavaBean getters/setters dành cho Flink PojoSerializer
     * =========================================================
     */

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

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public int getEventCode() {
        return eventCode;
    }

    public void setEventCode(int eventCode) {
        this.eventCode = eventCode;
    }

    public String getEventResult() {
        return eventResult;
    }

    public void setEventResult(String eventResult) {
        this.eventResult = eventResult;
    }

    public int getEventResultCode() {
        return eventResultCode;
    }

    public void setEventResultCode(int eventResultCode) {
        this.eventResultCode = eventResultCode;
    }

    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    public long getEventTimeEpochMs() {
        return eventTimeEpochMs;
    }

    public void setEventTimeEpochMs(long eventTimeEpochMs) {
        this.eventTimeEpochMs = eventTimeEpochMs;
    }

    public Map<String, String> getFeatureSourceFields() {
        return featureSourceFields;
    }

    public void setFeatureSourceFields(
            Map<String, String> featureSourceFields
    ) {
        this.featureSourceFields =
                mutableCopy(featureSourceFields);
    }

    public Map<String, String> getDisplayFields() {
        return displayFields;
    }

    public void setDisplayFields(
            Map<String, String> displayFields
    ) {
        this.displayFields =
                mutableCopy(displayFields);
    }

    public Map<String, String> getQualityFields() {
        return qualityFields;
    }

    public void setQualityFields(
            Map<String, String> qualityFields
    ) {
        this.qualityFields =
                mutableCopy(qualityFields);
    }

    public String getSourceOrderKey() {
        return sourceOrderKey;
    }

    public void setSourceOrderKey(String sourceOrderKey) {
        this.sourceOrderKey = sourceOrderKey;
    }

    /*
     * =========================================================
     * Accessor kiểu record
     * =========================================================
     *
     * Các method này giúp những file đã viết ở Checkpoint 11
     * tiếp tục dùng:
     *
     * event.ueKey()
     * event.eventTime()
     * event.sourceOrderKey()
     *
     * Vì vậy không cần sửa GoldSequenceProcessFunction,
     * GoldSequenceWindowFactory hoặc các test hiện tại.
     */

    public String ueKey() {
        return ueKey;
    }

    public String imsi() {
        return imsi;
    }

    public String eventId() {
        return eventId;
    }

    public int eventCode() {
        return eventCode;
    }

    public String eventResult() {
        return eventResult;
    }

    public int eventResultCode() {
        return eventResultCode;
    }

    public long durationMs() {
        return durationMs;
    }

    /**
     * Chuyển timestamp long trở lại Instant cho code nghiệp vụ.
     */
    public Instant eventTime() {
        return Instant.ofEpochMilli(eventTimeEpochMs);
    }

    public Map<String, String> featureSourceFields() {
        return featureSourceFields;
    }

    public Map<String, String> displayFields() {
        return displayFields;
    }

    public Map<String, String> qualityFields() {
        return qualityFields;
    }

    public String sourceOrderKey() {
        return sourceOrderKey;
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

    private static Map<String, String> mutableCopy(
            Map<String, String> source
    ) {
        if (source == null) {
            return new LinkedHashMap<>();
        }

        return new LinkedHashMap<>(source);
    }
}