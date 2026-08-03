package com.network.preprocess.silver.identity;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Chuẩn hóa các identity trước khi lookup.
 *
 * <p>Class này không resolve mapping. Nó chỉ:</p>
 *
 * <ul>
 *     <li>Loại khoảng trắng không có ý nghĩa.</li>
 *     <li>Kiểm tra định dạng.</li>
 *     <li>Trả Optional.empty() khi giá trị không hợp lệ.</li>
 * </ul>
 */
public final class IdentityNormalizer {

    /**
     * IMSI gồm chữ số và có tối đa 15 chữ số.
     *
     * <p>Giới hạn tối thiểu 5 ký tự giúp loại các giá trị rác
     * quá ngắn nhưng không hard-code riêng MCC/MNC của một quốc gia.</p>
     */
    private static final Pattern IMSI_PATTERN =
            Pattern.compile("[0-9]{5,15}");

    /**
     * MSISDN sau khi bỏ ký tự trình bày phải là chuỗi số.
     */
    private static final Pattern MSISDN_PATTERN =
            Pattern.compile("[0-9]{5,15}");

    /**
     * Một số nguồn xuất MTMSI ở dạng số thập phân.
     *
     * <p>MTMSI là giá trị 32 bit nên dạng thập phân có tối đa
     * 10 chữ số.</p>
     */
    private static final Pattern MTMSI_DECIMAL_PATTERN =
            Pattern.compile("[0-9]{1,10}");

    /**
     * Một số nguồn xuất MTMSI ở dạng hexadecimal.
     */
    private static final Pattern MTMSI_HEX_PATTERN =
            Pattern.compile("[0-9A-F]{1,8}");

    private IdentityNormalizer() {
        /*
         * Utility class không được khởi tạo.
         */
    }

    /**
     * Chuẩn hóa IMSI.
     *
     * <p>Không chuyển sang kiểu long vì IMSI là identifier,
     * không phải số dùng để tính toán. Giữ String cũng bảo toàn
     * chữ số 0 ở đầu nếu nguồn có sử dụng.</p>
     */
    public static Optional<String> normalizeImsi(
            String rawImsi
    ) {
        String value = trimToNull(rawImsi);

        if (value == null || !IMSI_PATTERN.matcher(value).matches()) {
            return Optional.empty();
        }

        return Optional.of(value);
    }

    /**
     * Chuẩn hóa MSISDN về dạng chỉ chứa chữ số.
     *
     * <p>Các ký tự trình bày được loại bỏ:</p>
     *
     * <ul>
     *     <li>Dấu + ở đầu.</li>
     *     <li>Khoảng trắng.</li>
     *     <li>Dấu gạch ngang.</li>
     *     <li>Dấu ngoặc tròn.</li>
     * </ul>
     */
    public static Optional<String> normalizeMsisdn(
            String rawMsisdn
    ) {
        String value = trimToNull(rawMsisdn);

        if (value == null) {
            return Optional.empty();
        }

        String compact = value.replaceAll(
                "[\\s()+-]",
                ""
        );

        if (!MSISDN_PATTERN.matcher(compact).matches()) {
            return Optional.empty();
        }

        return Optional.of(compact);
    }

    /**
     * Chuẩn hóa MTMSI.
     *
     * <p>Nếu giá trị có prefix 0x thì prefix được bỏ.
     * Hexadecimal được chuyển thành chữ hoa để key lookup ổn định.</p>
     */
    public static Optional<String> normalizeMtmsi(
            String rawMtmsi
    ) {
        String value = trimToNull(rawMtmsi);

        if (value == null) {
            return Optional.empty();
        }

        String compact = value
                .toUpperCase(Locale.ROOT);

        if (compact.startsWith("0X")) {
            compact = compact.substring(2);
        }

        boolean validDecimal =
                MTMSI_DECIMAL_PATTERN
                        .matcher(compact)
                        .matches();

        boolean validHexadecimal =
                MTMSI_HEX_PATTERN
                        .matcher(compact)
                        .matches();

        if (!validDecimal && !validHexadecimal) {
            return Optional.empty();
        }

        return Optional.of(compact);
    }

    /**
     * Kiểm tra field có thực sự chứa dữ liệu hay không.
     *
     * <p>Method này khác normalize: một giá trị có thể có text
     * nhưng vẫn sai format.</p>
     */
    public static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String trimToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        return value.trim();
    }
}