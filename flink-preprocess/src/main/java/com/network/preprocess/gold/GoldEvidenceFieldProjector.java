package com.network.preprocess.gold;

import com.network.preprocess.model.SilverEvent;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Chọn các field metadata được phép đưa vào evidence của Gold.
 *
 * <p>Class này không tạo feature cho model.</p>
 *
 * <p>Việc thêm hoặc bỏ field trong evidence chỉ cần thay đổi
 * cấu hình application.yaml.</p>
 */
public final class GoldEvidenceFieldProjector
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<String> configuredFields;

    public GoldEvidenceFieldProjector(
            List<String> configuredFields
    ) {
        this.configuredFields =
                List.copyOf(
                        Objects.requireNonNull(
                                configuredFields,
                                "configuredFields must not be null"
                        )
                );
    }

    /**
     * Tạo map evidence theo đúng danh sách cấu hình.
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

        for (String configuredField : configuredFields) {

            String field =
                    configuredField
                            .trim()
                            .toLowerCase(Locale.ROOT);

            String value =
                    resolveValue(
                            event,
                            field
                    );

            /*
             * Chỉ ghi key đã được cấu hình.
             *
             * Có thể giữ null nếu sau này muốn schema cố định,
             * nhưng ở phiên bản đầu tôi khuyên bỏ null để payload
             * Gold nhỏ hơn.
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
     * Map tên field cấu hình sang dữ liệu SilverEvent.
     */
    private String resolveValue(
            SilverEvent event,
            String field
    ) {
        return switch (field) {

            case "event_id" ->
                    event.eventId();

            case "event_result" ->
                    event.eventResult().wireValue();

            case "duration_ms" ->
                    event.durationMs() == null
                            ? null
                            : event.durationMs().toString();

            case "event_time" ->
                    event.eventTime();

            case "msisdn" ->
                    event.msisdn();

            case "imsi" ->
                    event.imsi();

            case "mtmsi" ->
                    event.mtmsi();

            case "imeisv" ->
                    event.imeisv();

            case "mmegi" ->
                    event.mmegi();

            case "mmec" ->
                    event.mmec();

            case "tac" ->
                    event.tac();

            case "eci" ->
                    event.eci();

            case "sgw" ->
                    event.sgw();

            case "sgsn" ->
                    event.sgsn();

            case "sub_cause_code" ->
                    event.rawFields()
                            .get("SUB_CAUSE_CODE");

            case "msc" ->
                    event.rawFields()
                            .get("MSC");

            case "pdn_pgw" ->
                    event.rawFields()
                            .get("PDN_PGW");

            /*
             * Các field khác vẫn có thể lấy từ 52 rawFields.
             *
             * Ví dụ config:
             *
             *     - apn
             *
             * sẽ tìm APN.
             */
            default ->
                    event.rawFields()
                            .get(
                                    field.toUpperCase(
                                            Locale.ROOT
                                    )
                            );
        };
    }
}