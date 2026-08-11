package com.network.preprocess.silver.event;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.EventResult;
import com.network.preprocess.model.EventDefinition;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.SilverDisplay;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.SilverQuality;
import com.network.preprocess.model.UnsupportedEventReason;
import com.network.preprocess.model.UnsupportedEventRecord;
import com.network.preprocess.silver.SilverTransformationResult;
import com.network.preprocess.util.DeterministicId;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Chuyển IdentityResolvedEvent thành SilverEvent.
 *
 * <p>Trách nhiệm chính của class:</p>
 *
 * <ul>
 *     <li>Chuẩn hóa EVENT_ID.</li>
 *     <li>Kiểm tra EVENT_ID có được model hỗ trợ hay không.</li>
 *     <li>Chuẩn hóa EVENT_RESULT.</li>
 *     <li>Tạo quality metadata và display metadata.</li>
 *     <li>Tạo ID có tính deterministic.</li>
 *     <li>Route event không được hỗ trợ sang UnsupportedEventRecord.</li>
 * </ul>
 *
 * <p>Class không phụ thuộc Flink API nên có thể unit test
 * toàn bộ business logic mà không cần khởi động Flink runtime.</p>
 */
public final class SilverEventTransformer
        implements Serializable {

    private static final String SILVER_SCHEMA_VERSION =
            "silver-v1";

    private static final String UNSUPPORTED_SCHEMA_VERSION =
            "unsupported-event-v1";

    private final EventCatalog eventCatalog;

    public SilverEventTransformer(
            EventCatalog eventCatalog
    ) {
        this.eventCatalog = Objects.requireNonNull(
                eventCatalog,
                "eventCatalog must not be null"
        );
    }

    /**
     * Chuyển một event đã resolve identity thành kết quả Silver.
     *
     * @param resolvedEvent kết quả resolve identity từ Checkpoint 7
     * @param processingTimeMillis processing time của Silver operator
     * @return SilverEvent nếu được hỗ trợ, ngược lại là UnsupportedEventRecord
     */
    public SilverTransformationResult transform(
            IdentityResolvedEvent resolvedEvent,
            long processingTimeMillis
    ) {
        Objects.requireNonNull(
                resolvedEvent,
                "resolvedEvent must not be null"
        );

        BronzeEvent bronzeEvent =
                Objects.requireNonNull(
                        resolvedEvent.bronzeEvent(),
                        "bronzeEvent must not be null"
                );

        Objects.requireNonNull(
                bronzeEvent.rawRecordId(),
                "BronzeEvent.rawRecordId must not be null"
        );

        /*
         * Bước 1: chuẩn hóa EVENT_ID thành lookup key.
         *
         * Ví dụ:
         *
         * " L_SERVICE_REQUEST "
         *      -> "l_service_request"
         *
         * "L-Service Request"
         *      -> "l_service_request"
         */
        Optional<String> normalizedEventId =
                EventIdNormalizer.normalizeLookupKey(
                        bronzeEvent.eventId()
                );

        /*
         * EVENT_ID null, rỗng hoặc chỉ chứa ký tự phân cách
         * không thể dùng để tra cứu catalog.
         */
        if (normalizedEventId.isEmpty()) {
            return unsupported(
                    resolvedEvent,
                    UnsupportedEventReason.MISSING_EVENT_ID,
                    processingTimeMillis
            );
        }

        /*
         * Bước 2: kiểm tra EVENT_ID có tồn tại trong catalog
         * mà model đã phê duyệt hay không.
         */
        Optional<EventDefinition> definition =
                requireValidCatalogResult(
                        eventCatalog.findByEventId(
                                normalizedEventId.get()
                        )
                );

        /*
         * EVENT_ID có format hợp lệ nhưng không tồn tại trong
         * model catalog thì route sang unsupported-event.
         */
        if (definition.isEmpty()) {
            return unsupported(
                    resolvedEvent,
                    UnsupportedEventReason.UNSUPPORTED_EVENT_ID,
                    processingTimeMillis
            );
        }

        EventDefinition eventDefinition =
                definition.get();

        /*
         * Bước 3: chuẩn hóa EVENT_RESULT theo feature contract.
         *
         * Chỉ hai category "reject" và "success" được phép đi tiếp.
         * Giá trị thiếu hoặc nằm ngoài vocabulary được route sang
         * unsupported-event để Gold không nhận category không encode được.
         */
        String rawEventResult =
                bronzeEvent.eventResult();

        Optional<EventResult> normalizedEventResult =
                EventResultNormalizer.normalize(
                        rawEventResult
                );

        /*
         * Không đưa category lạ vào Silver vì GoldFeatureEncoder
         * không thể encode category nằm ngoài feature contract.
         */
        if (normalizedEventResult.isEmpty()) {
            UnsupportedEventReason reason =
                    rawEventResult == null
                            || rawEventResult.isBlank()
                            ? UnsupportedEventReason
                                    .MISSING_EVENT_RESULT
                            : UnsupportedEventReason
                                    .UNSUPPORTED_EVENT_RESULT;

            return unsupported(
                    resolvedEvent,
                    reason,
                    processingTimeMillis
            );
        }

        EventResult eventResult =
                normalizedEventResult.get();

        boolean eventResultChanged =
                EventResultNormalizer.wasChanged(
                        rawEventResult,
                        eventResult
                );

        /*
         * Bước 4: lấy các trường vẫn đang nằm trong rawFields.
         *
         * Không bổ sung các trường này vào BronzeEvent để tránh
         * thay đổi contract Bronze đã ổn định.
         */
        String subType =
                readOptionalRawField(
                        bronzeEvent,
                        "SUB_TYPE"
                );

        String reportSide =
                readOptionalRawField(
                        bronzeEvent,
                        "REPORT_SIDE"
                );

        /*
         * Danh sách 52 trường raw hiện tại có SGSN, không có PGW.
         */
        String sgsn =
                readOptionalRawField(
                        bronzeEvent,
                        "SGSN"
                );

        /*
         * Bước 5: xác định EVENT_ID có bị thay đổi sau khi
         * chuẩn hóa hay không.
         */
        boolean eventIdChanged =
                rawValueChanged(
                        bronzeEvent.eventId(),
                        eventDefinition.canonicalEventId()
                );

        /*
         * Bước 6: tạo danh sách cảnh báo chất lượng dữ liệu.
         */
        List<String> warnings =
                new ArrayList<>();

        if (eventIdChanged) {
            warnings.add(
                    "EVENT_ID_NORMALIZED"
            );
        }

        if (eventResultChanged) {
            warnings.add(
                    "EVENT_RESULT_NORMALIZED"
            );
        }

        SilverQuality quality =
                new SilverQuality(
                        resolvedEvent.resolutionSource(),
                        eventIdChanged,
                        eventResultChanged,

                        /*
                        * Silver output chỉ chứa event result hợp lệ,
                        * nên giá trị này luôn là true.
                        */
                        true,
                        warnings
                );

        /*
         * Display metadata chỉ phục vụ hiển thị.
         *
         * Logic model vẫn phải sử dụng canonical EVENT_ID
         * và EventResult enum.
         */
        SilverDisplay display =
                new SilverDisplay(
                        eventDefinition.displayName(),
                        eventResult.displayLabel()
                );

        /*
         * Bước 7: tạo Silver event ID có tính deterministic.
         *
         * ID chỉ phụ thuộc rawRecordId, không phụ thuộc:
         *
         * - processing time
         * - thời điểm Flink restart
         * - số lần Kafka record được đọc lại
         *
         * Vì vậy cùng một raw record luôn sinh cùng một
         * silverEventId.
         */
        String silverEventId =
                DeterministicId.sha256(
                        "silver-event:"
                                + bronzeEvent.rawRecordId()
                );

        /*
         * Bước 8: tạo SilverEvent hoàn chỉnh.
         */
        SilverEvent silverEvent =
                new SilverEvent(
                        SILVER_SCHEMA_VERSION,
                        silverEventId,
                        bronzeEvent.rawRecordId(),

                        /*
                        * Identity đã được resolve tại Checkpoint 7.
                        */
                        resolvedEvent.ueKey(),
                        resolvedEvent.imsi(),

                        /*
                         * EVENT_ID giữ ở dạng chuỗi canonical.
                         * EVENT_RESULT giữ ở dạng enum domain; khi ghi JSON,
                         * @JsonValue serialize enum thành "success"/"reject".
                         *
                         * GoldFeatureEncoder mới chuyển category thành ID.
                         */
                        eventDefinition.canonicalEventId(),
                        eventResult,

                        /*
                        * Numeric fields giữ nguyên giá trị nguồn.
                        * GoldFeatureEncoder mới thực hiện normalize.
                        */
                        bronzeEvent.durationMs(),
                        bronzeEvent.requestRetries(),
                        subType,

                        /*
                        * Thời gian và report side.
                        */
                        bronzeEvent.eventTime(),
                        reportSide,

                        /*
                        * Các identity gốc phục vụ audit.
                        */
                        bronzeEvent.msisdn(),
                        bronzeEvent.mtmsi(),
                        bronzeEvent.imeisv(),

                        /*
                        * Thông tin location và network node.
                        */
                        bronzeEvent.mmegi(),
                        bronzeEvent.mmec(),
                        bronzeEvent.tac(),
                        bronzeEvent.eci(),
                        bronzeEvent.sgw(),
                        sgsn,

                        /*
                        * Metadata đã chuẩn hóa.
                        */
                        display,
                        quality,

                        /*
                        * Giữ dữ liệu gốc và Kafka metadata để
                        * Gold lấy cause/sub-cause và phục vụ audit.
                        */
                        bronzeEvent.rawFields(),
                        bronzeEvent.source()
                );

        return SilverTransformationResult.supported(
                silverEvent
        );
    }

    /**
     * Kiểm tra kết quả trả về từ EventCatalog.
     *
     * <p>EventCatalog phải trả Optional.empty() khi không tìm thấy.
     * Catalog không được trả null.</p>
     *
     * <p>Nếu catalog trả null thì đây là lỗi code hoặc lỗi provider,
     * không phải lỗi dữ liệu của event hiện tại.</p>
     */
    private Optional<EventDefinition> requireValidCatalogResult(
            Optional<EventDefinition> result
    ) {
        return Objects.requireNonNull(
                result,
                "EventCatalog must not return null"
        );
    }

    /**
     * Tạo UnsupportedEventRecord cho EVENT_ID bị thiếu
     * hoặc không được model hỗ trợ.
     */
    private SilverTransformationResult unsupported(
            IdentityResolvedEvent event,
            UnsupportedEventReason reason,
            long processingTimeMillis
    ) {
        BronzeEvent bronzeEvent =
                Objects.requireNonNull(
                        event.bronzeEvent(),
                        "bronzeEvent must not be null"
                );

        /*
         * unsupportedEventId có tính deterministic.
         *
         * failedAt không tham gia tạo ID vì processing time
         * sẽ thay đổi khi Flink replay record.
         */
        String unsupportedEventId =
                DeterministicId.sha256(
                        "unsupported-event:"
                                + bronzeEvent.rawRecordId()
                                + ":"
                                + reason.name()
                );

        String failedAt =
                Instant.ofEpochMilli(
                        processingTimeMillis
                ).toString();

        UnsupportedEventRecord record =
                new UnsupportedEventRecord(
                        UNSUPPORTED_SCHEMA_VERSION,
                        unsupportedEventId,
                        bronzeEvent.rawRecordId(),
                        reason,
                        safeMessage(reason),
                        failedAt,
                        event
                );

        return SilverTransformationResult.unsupported(
                record
        );
    }

    /**
     * Tạo error message an toàn.
     *
     * <p>Không đưa raw EVENT_ID vào message vì dữ liệu nguồn
     * có thể chứa text bất thường hoặc thông tin không nên ghi log.</p>
     */
        private String safeMessage(
                UnsupportedEventReason reason
        ) {
                return switch (reason) {
                        case MISSING_EVENT_ID ->
                                "EVENT_ID is missing";

                        case UNSUPPORTED_EVENT_ID ->
                                "EVENT_ID is not supported by the model catalog";

                        case MISSING_EVENT_RESULT ->
                                "EVENT_RESULT is missing";

                        case UNSUPPORTED_EVENT_RESULT ->
                                "EVENT_RESULT is not supported by the feature contract";
                };
        }

    /**
     * Đọc một trường tùy chọn từ Bronze rawFields.
     *
     * <p>Giá trị null, rỗng hoặc chỉ chứa khoảng trắng
     * được chuyển thành null.</p>
     *
     * <p>rawFields trong BronzeEvent vẫn được giữ nguyên.
     * Chỉ giá trị đưa sang Silver mới được trim.</p>
     */
    private String readOptionalRawField(
            BronzeEvent bronzeEvent,
            String fieldName
    ) {
        Objects.requireNonNull(
                bronzeEvent,
                "bronzeEvent must not be null"
        );

        Objects.requireNonNull(
                fieldName,
                "fieldName must not be null"
        );

        String rawValue =
                bronzeEvent.rawFields().get(fieldName);

        if (rawValue == null || rawValue.isBlank()) {
            return null;
        }

        return rawValue.trim();
    }

    /**
     * Kiểm tra raw value có khác canonical value hay không.
     *
     * <p>Ví dụ:</p>
     *
     * <ul>
     *     <li>"l_service_request" → không thay đổi.</li>
     *     <li>" L_SERVICE_REQUEST " → có thay đổi.</li>
     *     <li>"L-Service Request" → có thay đổi.</li>
     * </ul>
     */
    private boolean rawValueChanged(
            String rawValue,
            String canonicalValue
    ) {
        Objects.requireNonNull(
                canonicalValue,
                "canonicalValue must not be null"
        );

        if (rawValue == null) {
            return true;
        }

        return !rawValue.trim().equals(
                canonicalValue
        );
    }
}