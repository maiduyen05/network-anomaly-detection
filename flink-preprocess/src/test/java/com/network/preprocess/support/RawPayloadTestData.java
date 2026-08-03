package com.network.preprocess.support;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tạo raw payload đúng contract 52 field.
 *
 * <p>Class này chỉ dùng trong test. Mục đích là tránh phải viết thủ công
 * một chuỗi 52 field trong từng test case.</p>
 */
public final class RawPayloadTestData {

    /**
     * Danh sách field phải giữ đúng thứ tự của raw log.
     *
     * <p>Chỉ số trong List là chỉ số 0-based:</p>
     *
     * <ul>
     *     <li>EVENT_ID nằm ở vị trí 0.</li>
     *     <li>EVENT_TIME nằm ở vị trí 48.</li>
     *     <li>PAGING_ATTEMPTS nằm ở vị trí 49.</li>
     *     <li>DATE_HOUR nằm ở vị trí 51.</li>
     * </ul>
     */
    public static final List<String> RAW_FIELD_NAMES = List.of(
            "EVENT_ID",
            "EVENT_RESULT",
            "DURATION",
            "REQUEST_RETRIES",
            "SUB_TYPE",
            "MSISDN",
            "IMSI",
            "MTMSI",
            "IMEISV",
            "MMEGI",
            "MMEC",
            "TAC",
            "ECI",
            "SGW",
            "SGSN",
            "L_CAUSE_PROT_TYPE",
            "CAUSE_CODE",
            "SUB_CAUSE_CODE",
            "APN",
            "PDN_DEFAULT_BEARER_ID",
            "PDN_PAA",
            "PDN_PGW",
            "ORIGINATING_CAUSE_PROT_TYPE",
            "ORIGINATING_CAUSE_CODE",
            "CSG_ID",
            "OLD_MTMSI",
            "OLD_TAC",
            "OLD_MMEGI",
            "OLD_MMEC",
            "OLD_ECI",
            "OLD_SGW",
            "OLD_SGSN",
            "MSC",
            "TARGET_LAC",
            "LAC",
            "RAC",
            "CI",
            "HANDOVER_NODE_ROLE",
            "HANDOVER_RAT_CHANGE_TYPE",
            "HANDOVER_SGW_CHANGE_TYPE",
            "TARGET_RNC_ID",
            "TARGET_MACRO_ENODEB_ID",
            "SRVCC_TYPE",
            "CS_FALLBACK_SERVICE_TYPE",
            "CSFB_TRIGGERED",
            "L_SERVICE_REQ_TRIGGER",
            "COMBINED_TAU_TYPE",
            "DETACH_TRIGGER",
            "EVENT_TIME",
            "PAGING_ATTEMPTS",
            "UE_REQUESTED_APN",
            "DATE_HOUR"
    );

    /**
     * Utility class không cần tạo object.
     */
    private RawPayloadTestData() {
    }

    /**
     * Tạo payload hợp lệ có đúng 52 field.
     */
    public static String validPayload() {

        /*
         * Tạo List có đúng 52 phần tử.
         * Ban đầu tất cả field là chuỗi rỗng.
         */
        List<String> values = new ArrayList<>(
                Collections.nCopies(
                        RAW_FIELD_NAMES.size(),
                        ""
                )
        );

        /*
         * Gán các field quan trọng mà BronzeTransformer sử dụng.
         */
        set(values, "EVENT_ID", "l_service_request");
        set(values, "EVENT_RESULT", "success");
        set(values, "DURATION", "1500");
        set(values, "REQUEST_RETRIES", "2");
        set(values, "SUB_TYPE", "normal");

        set(values, "MSISDN", "84901234567");
        set(values, "IMSI", "452010123456789");
        set(values, "MTMSI", "A1B2C3D4");
        set(values, "IMEISV", "3567890123456789");

        set(values, "MMEGI", "100");
        set(values, "MMEC", "10");
        set(values, "TAC", "200");
        set(values, "ECI", "300");

        /*
         * EVENT_TIME là timestamp thực sự của event.
         * DATE_HOUR không được dùng thay EVENT_TIME.
         */
        set(values, "EVENT_TIME", "2026-08-03 13:15:30");
        set(values, "PAGING_ATTEMPTS", "1");
        set(values, "DATE_HOUR", "2026080313");

        /*
         * String.join vẫn giữ delimiter trước field cuối bị rỗng.
         */
        return String.join(";", values);
    }

    /**
     * Thay một field trong payload hợp lệ.
     *
     * @param fieldName tên field cần thay
     * @param value giá trị mới
     * @return payload mới có đúng 52 field
     */
    public static String validPayloadWith(
            String fieldName,
            String value
    ) {
        List<String> values = new ArrayList<>(
                List.of(validPayload().split(";", -1))
        );

        set(values, fieldName, value);

        return String.join(";", values);
    }

    /**
     * Gán giá trị theo tên field để test không phụ thuộc magic number.
     */
    private static void set(
            List<String> values,
            String fieldName,
            String value
    ) {
        int index = RAW_FIELD_NAMES.indexOf(fieldName);

        if (index < 0) {
            throw new IllegalArgumentException(
                    "Unknown raw field: " + fieldName
            );
        }

        values.set(index, value);
    }
}