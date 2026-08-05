package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.SilverEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

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
         * Giữ raw fields phục vụ evidence và audit.
         * Setter sẽ tạo mutable copy.
         */
        goldEvent.setFeatureSourceFields(
                rawFields
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