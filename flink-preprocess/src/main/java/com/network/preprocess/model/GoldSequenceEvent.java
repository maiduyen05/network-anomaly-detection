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

    /*
     * =========================================================
     * Dữ liệu nguồn dùng bởi GoldFeatureEncoder
     * =========================================================
     *
     * Các categorical field phải giữ nguyên ở dạng chuỗi.
     *
     * Ví dụ:
     *
     * eventId = "l_service_request"
     *
     * Không được lưu ID đã encode:
     *
     * eventId = "8"
     *
     * GoldFeatureEncoder chịu trách nhiệm chuyển category thành ID.
     */

    private String eventId;
    private String eventResult;
    private String normalizedCauseCode;
    private String subCauseCode;

    /*
     * Dùng wrapper type để biểu diễn được trường hợp thiếu dữ liệu.
     *
     * null khác với 0:
     *
     * durationMs = null       → không có dữ liệu
     * durationMs = 0L         → duration thực sự bằng 0
     *
     * requestRetries = null   → không có dữ liệu
     * requestRetries = 0      → không retry
     */
    private Long durationMs;
    private Integer requestRetries;

    /*
     * Hai field này được giữ lại từ Checkpoint 11 để không làm hỏng
     * các file đang sử dụng GoldSequenceEvent.
     *
     * GoldFeatureEncoder mới không lấy category từ hai field này.
     * Encoder phải đọc eventId và eventResult ở dạng chuỗi nguồn.
     */
    private int eventCode;
    private int eventResultCode;

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
     *
     * <p>Các category được truyền vào constructor dưới dạng chuỗi nguồn.
     * Constructor không thực hiện encode feature.</p>
     */
    public GoldSequenceEvent(
            String ueKey,
            String imsi,
            String eventId,
            int eventCode,
            String eventResult,
            int eventResultCode,
            String normalizedCauseCode,
            String subCauseCode,
            Long durationMs,
            Integer requestRetries,
            Instant eventTime,
            Map<String, String> featureSourceFields,
            Map<String, String> displayFields,
            Map<String, String> qualityFields,
            String sourceOrderKey
    ) {
        this.ueKey = requiredText(ueKey, "ueKey");
        this.imsi = requiredText(imsi, "imsi");

        /*
         * eventId và eventResult vẫn là category dạng chuỗi.
         *
         * Ví dụ:
         *
         * eventId = "l_service_request"
         * eventResult = "success"
         */
        this.eventId = requiredText(eventId, "eventId");
        this.eventResult =
                requiredText(eventResult, "eventResult");

        /*
         * Hai cause field có thể là chuỗi rỗng.
         *
         * Chuỗi rỗng đang là một category hợp lệ trong vocabulary,
         * vì vậy không dùng requiredText() cho hai field này.
         */
        this.normalizedCauseCode = normalizedCauseCode;
        this.subCauseCode = subCauseCode;

        /*
         * Numeric source giữ nguyên giá trị nguồn.
         *
         * GoldFeatureEncoder mới chịu trách nhiệm clip,
         * normalize hoặc chuyển giá trị thiếu.
         */
        this.durationMs = durationMs;
        this.requestRetries = requestRetries;

        /*
         * Giữ lại hai giá trị của Checkpoint 11 để tương thích
         * với những phần code cũ đang sử dụng.
         */
        this.eventCode = eventCode;
        this.eventResultCode = eventResultCode;

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

    /**
     * Nhận category dạng chuỗi, không nhận vocabulary ID.
     *
     * <p>Đúng: setEventId("l_service_request")</p>
     * <p>Sai: setEventId("8")</p>
     */
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventResult() {
        return eventResult;
    }

    /**
     * Nhận category dạng chuỗi, ví dụ "success" hoặc "reject".
     */
    public void setEventResult(String eventResult) {
        this.eventResult = eventResult;
    }

    public String getNormalizedCauseCode() {
        return normalizedCauseCode;
    }

    /**
     * Giữ category nguồn, bao gồm cả chuỗi rỗng.
     */
    public void setNormalizedCauseCode(
            String normalizedCauseCode
    ) {
        this.normalizedCauseCode = normalizedCauseCode;
    }

    public String getSubCauseCode() {
        return subCauseCode;
    }

    /**
     * Giữ category nguồn, bao gồm cả chuỗi rỗng.
     */
    public void setSubCauseCode(String subCauseCode) {
        this.subCauseCode = subCauseCode;
    }

    public Long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(Long durationMs) {
        this.durationMs = durationMs;
    }

    public Integer getRequestRetries() {
        return requestRetries;
    }

    public void setRequestRetries(Integer requestRetries) {
        this.requestRetries = requestRetries;
    }

    public int getEventCode() {
        return eventCode;
    }

    public void setEventCode(int eventCode) {
        this.eventCode = eventCode;
    }

    public int getEventResultCode() {
        return eventResultCode;
    }

    public void setEventResultCode(int eventResultCode) {
        this.eventResultCode = eventResultCode;
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
     * Giữ lại để code đã viết ở Checkpoint 11 tiếp tục gọi:
     *
     * event.ueKey()
     * event.eventId()
     * event.eventTime()
     * event.sourceOrderKey()
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

    public String eventResult() {
        return eventResult;
    }

    public String normalizedCauseCode() {
        return normalizedCauseCode;
    }

    public String subCauseCode() {
        return subCauseCode;
    }

    public Long durationMs() {
        return durationMs;
    }

    public Integer requestRetries() {
        return requestRetries;
    }

    public int eventCode() {
        return eventCode;
    }

    public int eventResultCode() {
        return eventResultCode;
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

    /**
     * Kiểm tra các trường bắt buộc không được null hoặc rỗng.
     */
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

    /**
     * Tạo một mutable Map mới để Flink có thể copy object an toàn.
     */
    private static Map<String, String> mutableCopy(
            Map<String, String> source
    ) {
        if (source == null) {
            return new LinkedHashMap<>();
        }

        return new LinkedHashMap<>(source);
    }
}