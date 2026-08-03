package com.network.preprocess.parser;

import com.network.preprocess.model.BronzeErrorCode;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Parser chuyển một raw log line thành map gồm đúng 52 field.
 *
 * <p>Mỗi phần tử trong log được ánh xạ theo đúng thứ tự của
 * {@link #FIELD_NAMES}.</p>
 *
 * <p>Parser sử dụng {@code split(..., -1)} để giữ lại các field rỗng
 * ở cuối dòng. Nếu không truyền {@code -1}, Java sẽ tự loại bỏ các
 * phần tử rỗng cuối cùng và khiến việc kiểm tra số lượng field sai.</p>
 */
public final class RawLogLineParser implements Serializable {

    /**
     * Danh sách tên 52 field theo đúng thứ tự xuất hiện trong raw log.
     *
     * <p>Thứ tự của danh sách này phải luôn đồng nhất với định dạng
     * dữ liệu do log producer gửi vào Kafka.</p>
     */
    public static final List<String> FIELD_NAMES = List.of(
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
     * Ký tự phân cách các field trong raw log.
     */
    private final String delimiter;

    /**
     * Số lượng field bắt buộc của mỗi raw log line.
     */
    private final int expectedFieldCount;

    /**
     * Khởi tạo parser.
     *
     * @param delimiter          ký tự phân cách các field, ví dụ {@code ;}
     * @param expectedFieldCount số field bắt buộc, hiện tại phải bằng 52
     */
    public RawLogLineParser(
            String delimiter,
            int expectedFieldCount
    ) {
        /*
         * Không cho phép delimiter null hoặc rỗng vì parser sẽ không
         * thể xác định ranh giới giữa các field.
         */
        if (delimiter == null || delimiter.isEmpty()) {
            throw new IllegalArgumentException(
                    "delimiter must not be empty"
            );
        }

        /*
         * Số field cấu hình phải khớp với danh sách tên field.
         * Kiểm tra ngay khi khởi tạo giúp phát hiện lỗi cấu hình sớm.
         */
        if (expectedFieldCount != FIELD_NAMES.size()) {
            throw new IllegalArgumentException(
                    "expectedFieldCount must match FIELD_NAMES size"
            );
        }

        this.delimiter = delimiter;
        this.expectedFieldCount = expectedFieldCount;
    }

    /**
     * Parse một raw log line thành map có thứ tự.
     *
     * @param rawPayload chuỗi raw log nhận được từ Kafka envelope
     * @return map chứa tên field và giá trị tương ứng
     * @throws BronzeDataException khi payload rỗng, toàn bộ field rỗng
     *                            hoặc số lượng field không hợp lệ
     */
    public Map<String, String> parse(String rawPayload)
            throws BronzeDataException {

        /*
         * Trường hợp payload null, chuỗi rỗng hoặc chỉ chứa whitespace.
         */
        if (rawPayload == null || rawPayload.isBlank()) {
            throw new BronzeDataException(
                    BronzeErrorCode.EMPTY_RAW_PAYLOAD,
                    "Raw payload is empty"
            );
        }

        /*
         * Pattern.quote() giúp delimiter được hiểu là chuỗi thông thường,
         * không bị diễn giải như biểu thức chính quy.
         *
         * Tham số -1 bắt buộc để giữ lại các field rỗng ở cuối dòng.
         */
        String[] values = rawPayload.split(
                Pattern.quote(delimiter),
                -1
        );

        /*
         * Mỗi raw log hợp lệ phải có chính xác số field đã cấu hình.
         */
        if (values.length != expectedFieldCount) {
            throw new BronzeDataException(
                    BronzeErrorCode.WRONG_RAW_FIELD_COUNT,
                    "Expected " + expectedFieldCount
                            + " fields but received "
                            + values.length
            );
        }

        /*
         * Một payload chỉ gồm các delimiter vẫn có thể tạo ra đúng
         * 52 phần tử sau khi split, ví dụ:
         *
         * ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
         *
         * Tuy nhiên, bản ghi đó không có bất kỳ dữ liệu nghiệp vụ nào
         * nên phải được xem là payload rỗng.
         *
         * String::isBlank nhận diện:
         * - chuỗi rỗng;
         * - chuỗi chỉ chứa dấu cách;
         * - tab và các ký tự whitespace khác.
         */
        boolean allFieldsAreEmpty = Arrays.stream(values)
                .allMatch(String::isBlank);

        if (allFieldsAreEmpty) {
            throw new BronzeDataException(
                    BronzeErrorCode.EMPTY_RAW_PAYLOAD,
                    "Raw log payload contains no populated fields"
            );
        }

        /*
         * LinkedHashMap giữ nguyên thứ tự chèn của 52 field.
         * Điều này giúp dữ liệu đầu ra ổn định, dễ kiểm thử và debug.
         */
        Map<String, String> parsedFields = new LinkedHashMap<>();

        for (int index = 0; index < FIELD_NAMES.size(); index++) {
            parsedFields.put(
                    FIELD_NAMES.get(index),
                    values[index]
            );
        }

        return parsedFields;
    }
}