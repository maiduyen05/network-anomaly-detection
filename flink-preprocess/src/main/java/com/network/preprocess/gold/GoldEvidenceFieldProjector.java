package com.network.preprocess.gold;

import com.network.preprocess.model.SilverEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Chọn các field metadata được phép đưa vào evidence của Gold.
 *
 * <p>
 * Class này KHÔNG tạo feature cho model.
 * </p>
 *
 * <p>
 * Danh sách evidence field được lấy từ:
 * </p>
 *
 * <pre>
 * gold:
 *   evidence:
 *     fields:
 *       - event_id
 *       - event_time
 *       - imsi
 *       ...
 * </pre>
 *
 * <p>
 * Việc thêm hoặc bỏ field evidence chỉ cần thay đổi
 * application.yaml và không làm thay đổi x_cat/x_num.
 * </p>
 */
public final class GoldEvidenceFieldProjector
        implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Danh sách evidence field đã được normalize.
     */
    private final List<String> configuredFields;

    /**
     * Tạo projector từ danh sách evidence field trong config.
     *
     * @param configuredFields danh sách field từ gold.evidence.fields
     */
    public GoldEvidenceFieldProjector(
            List<String> configuredFields
    ) {

        Objects.requireNonNull(
                configuredFields,
                "configuredFields must not be null"
        );

        if (configuredFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "configuredFields must not be empty"
            );
        }

        List<String> normalizedFields =
                new ArrayList<>();

        Set<String> seenFields =
                new HashSet<>();

        for (String configuredField : configuredFields) {

            if (configuredField == null
                    || configuredField.isBlank()) {

                throw new IllegalArgumentException(
                        "configuredFields must not contain "
                                + "null or blank values"
                );
            }

            /*
             * Normalize field name một lần ngay tại constructor.
             *
             * Ví dụ:
             *
             * " IMSI "
             *
             * trở thành:
             *
             * "imsi"
             */
            String normalizedField =
                    configuredField
                            .trim()
                            .toLowerCase(
                                    Locale.ROOT
                            );

            /*
             * Không cho phép khai báo cùng một evidence field
             * nhiều lần.
             */
            if (!seenFields.add(normalizedField)) {

                throw new IllegalArgumentException(
                        "Duplicated evidence field: "
                                + normalizedField
                );
            }

            normalizedFields.add(
                    normalizedField
            );
        }

        /*
         * Lưu ArrayList mutable bên trong object.
         *
         * Không giữ List.of()/unmodifiable collection
         * để thân thiện hơn với Flink serialization.
         */
        this.configuredFields =
                new ArrayList<>(
                        normalizedFields
                );
    }

    /**
     * Tạo evidence map cho một SilverEvent.
     *
     * <p>
     * Chỉ những field được khai báo trong
     * gold.evidence.fields mới được đưa vào result.
     * </p>
     *
     * <p>
     * Field có value = null sẽ không được đưa vào map,
     * giúp giảm kích thước Gold payload.
     * </p>
     *
     * @param event SilverEvent nguồn
     * @return evidence map theo đúng cấu hình
     */
    public Map<String, String> project(
            SilverEvent event
    ) {

        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        Map<String, String> result =
                new LinkedHashMap<>();

        for (String field : configuredFields) {

            String value =
                    resolveValue(
                            event,
                            field
                    );

            /*
             * Chỉ output field có dữ liệu.
             *
             * Empty string "" vẫn được giữ vì:
             *
             * null = không có dữ liệu
             * ""   = dữ liệu tồn tại nhưng rỗng
             */
            if (value != null) {

                result.put(
                        field,
                        value
                );
            }
        }

        return result;
    }

    /**
     * Map tên evidence field sang dữ liệu SilverEvent.
     *
     * <p>
     * Ưu tiên lấy từ typed field của SilverEvent.
     * Nếu không có mapping riêng thì fallback sang rawFields.
     * </p>
     */
    private String resolveValue(
            SilverEvent event,
            String field
    ) {

        return switch (field) {

            /*
             * =====================================================
             * EVENT
             * =====================================================
             */

            case "event_id" ->
                    event.eventId();

            case "event_result" ->
                    event.eventResult() == null
                            ? null
                            : event
                                    .eventResult()
                                    .wireValue();

            /*
             * Hai field phục vụ UI.
             *
             * Trước đây GoldSequenceEventMapper
             * hard-code hai field này.
             *
             * Từ checkpoint hiện tại chúng cũng được
             * điều khiển bởi gold.evidence.fields.
             */
            case "event_name" ->
                    event.display() == null
                            ? null
                            : event
                                    .display()
                                    .eventName();

            case "event_result_label" ->
                    event.display() == null
                            ? null
                            : event
                                    .display()
                                    .eventResultLabel();

            case "duration_ms" ->
                    event.durationMs() == null
                            ? null
                            : event
                                    .durationMs()
                                    .toString();

            case "event_time" ->
                    event.eventTime();


            /*
             * =====================================================
             * UE IDENTITY
             * =====================================================
             */

            case "msisdn" ->
                    event.msisdn();

            case "imsi" ->
                    event.imsi();

            case "mtmsi" ->
                    event.mtmsi();

            case "imeisv" ->
                    event.imeisv();


            /*
             * =====================================================
             * NETWORK IDENTITY
             * =====================================================
             */

            case "mmegi" ->
                    event.mmegi();

            case "mmec" ->
                    event.mmec();


            /*
             * =====================================================
             * LOCATION / SERVING NETWORK
             * =====================================================
             */

            case "tac" ->
                    event.tac();

            case "eci" ->
                    event.eci();

            case "sgw" ->
                    event.sgw();

            case "sgsn" ->
                    event.sgsn();


            /*
             * =====================================================
             * RAW-FIELD EVIDENCE
             * =====================================================
             */

            case "sub_cause_code" ->
                    rawField(
                            event,
                            "SUB_CAUSE_CODE"
                    );

            case "msc" ->
                    rawField(
                            event,
                            "MSC"
                    );

            case "pdn_pgw" ->
                    rawField(
                            event,
                            "PDN_PGW"
                    );


            /*
             * =====================================================
             * GENERIC RAW FIELD FALLBACK
             * =====================================================
             *
             * Cho phép thêm evidence mới mà không phải sửa Java.
             *
             * Ví dụ YAML:
             *
             * gold:
             *   evidence:
             *     fields:
             *       - apn
             *
             * sẽ lookup:
             *
             * rawFields["APN"]
             */
            default ->
                    rawField(
                            event,
                            field.toUpperCase(
                                    Locale.ROOT
                            )
                    );
        };
    }

    /**
     * Đọc một field từ Silver rawFields một cách null-safe.
     */
    private static String rawField(
            SilverEvent event,
            String fieldName
    ) {

        Map<String, String> rawFields =
                event.rawFields();

        if (rawFields == null) {
            return null;
        }

        return rawFields.get(
                fieldName
        );
    }

    /**
     * Trả về copy của danh sách config để code bên ngoài
     * không sửa được trạng thái nội bộ của projector.
     */
    public List<String> configuredFields() {

        return new ArrayList<>(
                configuredFields
        );
    }
}