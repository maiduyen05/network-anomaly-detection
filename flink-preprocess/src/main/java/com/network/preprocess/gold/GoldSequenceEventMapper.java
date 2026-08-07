package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.SilverEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.Locale;

/**
 * Chuyển SilverEvent thành event trung gian của tầng Gold.
 *
 * <p>Mapper chỉ lấy dữ liệu nguồn. Nó không chuyển category
 * thành vocabulary ID.</p>
 */
public final class GoldSequenceEventMapper
        implements MapFunction<
                SilverEvent,
                GoldSequenceEvent> {

    @Override
    public GoldSequenceEvent map(
            SilverEvent silverEvent
    ) {
        Objects.requireNonNull(
                silverEvent,
                "silverEvent must not be null"
        );

        Map<String, String> rawFields =
                silverEvent.rawFields();

        /*
         * CAUSE_CODE và SUB_CAUSE_CODE phải tồn tại trong
         * rawFields vì Bronze parser luôn tạo đủ 52 field.
         *
         * Giá trị "" được giữ nguyên vì đó là category hợp lệ.
         */
        String normalizedCauseCode =
                requireRawCategory(
                        rawFields,
                        "CAUSE_CODE"
                );

        String subCauseCode =
                requireRawCategory(
                        rawFields,
                        "SUB_CAUSE_CODE"
                );

        GoldSequenceEvent goldEvent =
                new GoldSequenceEvent();

        /*
         * Identity đã được resolve tại Silver.
         */
        goldEvent.setUeKey(
                silverEvent.ueKey()
        );

        goldEvent.setImsi(
                silverEvent.imsi()
        );

        /*
         * Giữ category dạng chuỗi.
         */
        goldEvent.setEventId(
                silverEvent.eventId()
        );

        goldEvent.setEventResult(
                silverEvent
                        .eventResult()
                        .wireValue()
        );

        goldEvent.setNormalizedCauseCode(
                normalizedCauseCode
        );

        goldEvent.setSubCauseCode(
                subCauseCode
        );

        /*
         * Numeric feature vẫn là giá trị nguồn.
         */
        goldEvent.setDurationMs(
                silverEvent.durationMs()
        );

        goldEvent.setRequestRetries(
                silverEvent.requestRetries()
        );

        goldEvent.setEventTimeEpochMs(
                Instant.parse(
                        silverEvent.eventTime()
                ).toEpochMilli()
        );

        /*
        * Map này chỉ chứa những raw field mà feature contract thực sự cần.
        * Không dùng toàn bộ 52 fields vì làm GoldSequenceEvent lớn không cần thiết, mỗi Gold sample chứa 32 event nên dữ liệu dư bị nhân 32 lần.
        * Không copy toàn bộ 52 raw fields vào đây vì:
        */
        Map<String, String> featureSourceFields =
                new LinkedHashMap<>();

        featureSourceFields.put(
                "CAUSE_CODE",
                requireRawCategory(
                        rawFields,
                        "CAUSE_CODE"
                )
        );

        featureSourceFields.put(
                "SUB_CAUSE_CODE",
                requireRawCategory(
                        rawFields,
                        "SUB_CAUSE_CODE"
                )
        );

        goldEvent.setFeatureSourceFields(
                featureSourceFields
        );

        /*
         * =========================================================
         * DISPLAY EVIDENCE
         * =========================================================
         *
         * Đây là metadata dành cho UI và điều tra.
         * Không feature nào đọc dữ liệu từ map này.
         */
        Map<String, String> displayFields =
                new LinkedHashMap<>();

        displayFields.put(
                "event_name",
                silverEvent.display().eventName()
        );

        displayFields.put(
                "event_result_label",
                silverEvent.display().eventResultLabel()
        );

        goldEvent.setDisplayFields(
                displayFields
        );

        /*
         * =========================================================
         * QUALITY EVIDENCE
         * =========================================================
         *
         * Giữ thông tin Silver đã chuẩn hóa event và identity
         * như thế nào.
         */
        Map<String, String> qualityFields =
                new LinkedHashMap<>();

        qualityFields.put(
                "identity_resolution_source",
                silverEvent
                        .quality()
                        .identityResolutionSource()
                        .name()
                        .toLowerCase(Locale.ROOT)
        );

        qualityFields.put(
                "event_id_changed",
                Boolean.toString(
                        silverEvent
                                .quality()
                                .eventIdChanged()
                )
        );

        qualityFields.put(
                "event_result_changed",
                Boolean.toString(
                        silverEvent
                                .quality()
                                .eventResultChanged()
                )
        );

        qualityFields.put(
                "event_result_recognized",
                Boolean.toString(
                        silverEvent
                                .quality()
                                .eventResultRecognized()
                )
        );

        /*
         * Map chỉ chứa String nên danh sách warning được nối bằng "|".
         *
         * Không có warning → chuỗi rỗng.
         */
        qualityFields.put(
                "warnings",
                String.join(
                        "|",
                        silverEvent
                                .quality()
                                .warnings()
                )
        );

        goldEvent.setQualityFields(
                qualityFields
        );

        /*
         * Dùng rawRecordId để phân định thứ tự khi hai event
         * có cùng event time.
         */
        goldEvent.setSourceOrderKey(
                silverEvent.rawRecordId()
        );

        return goldEvent;
    }

    /**
     * Phân biệt:
     *
     * <ul>
     *     <li>Không có field hoặc value null → vi phạm contract.</li>
     *     <li>Value bằng "" → category hợp lệ.</li>
     * </ul>
     */
    private static String requireRawCategory(
            Map<String, String> rawFields,
            String fieldName
    ) {
        if (!rawFields.containsKey(fieldName)
                || rawFields.get(fieldName) == null) {

            throw new IllegalStateException(
                    "Silver rawFields must contain "
                            + fieldName
            );
        }

        return rawFields.get(fieldName);
    }
}