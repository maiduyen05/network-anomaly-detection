package com.network.preprocess.operator;

import com.network.preprocess.model.BronzeErrorCode;
import com.network.preprocess.parser.BronzeDataException;

import java.io.Serializable;
import java.util.Objects;

/**
 * Chuyển các numeric field dạng String sang kiểu số.
 *
 * <p>Chính sách Bronze:</p>
 *
 * <ul>
 *     <li>Giá trị null, rỗng hoặc chỉ có khoảng trắng → trả về null.</li>
 *     <li>Giá trị là số nguyên không âm → trả về số đã parse.</li>
 *     <li>Giá trị âm → ném BronzeDataException.</li>
 *     <li>Giá trị sai format → ném BronzeDataException.</li>
 *     <li>Giá trị vượt giới hạn kiểu số → ném BronzeDataException.</li>
 * </ul>
 *
 * <p>Class này không tự chọn error code vì mỗi numeric field có mã lỗi
 * riêng. BronzeTransformer truyền error code tương ứng vào method.</p>
 */
public final class TypeCastOperator implements Serializable {

    /**
     * Parse String thành Long không âm.
     *
     * <p>Method này được dùng cho DURATION vì duration có thể lớn hơn
     * giới hạn Integer.</p>
     *
     * @param rawValue giá trị lấy từ raw field
     * @param errorCode error code dành riêng cho field đang parse
     * @return Long không âm hoặc null nếu field rỗng
     * @throws BronzeDataException nếu giá trị sai hoặc âm
     */
    public Long parseNullableNonNegativeLong(
            String rawValue,
            BronzeErrorCode errorCode
    ) throws BronzeDataException {

        /*
         * errorCode được lập trình viên truyền từ BronzeTransformer.
         *
         * Nếu errorCode null thì đây là lỗi code, không phải lỗi dữ liệu.
         * Objects.requireNonNull sẽ làm job fail để lỗi được phát hiện.
         */
        Objects.requireNonNull(
                errorCode,
                "errorCode must not be null"
        );

        /*
         * Numeric field không bắt buộc:
         *
         * null  -> null
         * ""    -> null
         * "   " -> null
         */
        if (rawValue == null || rawValue.isBlank()) {
            return null;
        }

        try {
            /*
             * trim() cho phép giá trị như " 100 " được parse thành 100.
             */
            long parsedValue =
                    Long.parseLong(
                            rawValue.trim()
                    );

            /*
             * Duration và count không được âm.
             */
            if (parsedValue < 0) {
                throw new BronzeDataException(
                        errorCode,
                        "Numeric value must not be negative"
                );
            }

            /*
             * Giá trị hợp lệ.
             */
            return parsedValue;

        } catch (NumberFormatException exception) {
            /*
             * NumberFormatException xảy ra khi:
             *
             * - Có chữ: "abc".
             * - Có số thập phân: "1.5".
             * - Có ký tự lạ: "10ms".
             * - Giá trị vượt giới hạn Long.
             *
             * Không đưa rawValue vào message để tránh sao chép dữ liệu
             * nguồn không cần thiết.
             */
            throw new BronzeDataException(
                    errorCode,
                    "Numeric value has invalid Long format"
            );
        }
    }

    /**
     * Parse String thành Integer không âm.
     *
     * <p>Method này được dùng cho REQUEST_RETRIES và PAGING_ATTEMPTS.</p>
     *
     * @param rawValue giá trị lấy từ raw field
     * @param errorCode error code dành riêng cho field đang parse
     * @return Integer không âm hoặc null nếu field rỗng
     * @throws BronzeDataException nếu giá trị sai hoặc âm
     */
    public Integer parseNullableNonNegativeInteger(
            String rawValue,
            BronzeErrorCode errorCode
    ) throws BronzeDataException {

        /*
         * errorCode null biểu thị lỗi lập trình.
         */
        Objects.requireNonNull(
                errorCode,
                "errorCode must not be null"
        );

        /*
         * Numeric field rỗng được giữ thành null.
         */
        if (rawValue == null || rawValue.isBlank()) {
            return null;
        }

        try {
            /*
             * Chuyển String thành Integer.
             */
            int parsedValue =
                    Integer.parseInt(
                            rawValue.trim()
                    );

            /*
             * Retry count và paging attempt không được âm.
             */
            if (parsedValue < 0) {
                throw new BronzeDataException(
                        errorCode,
                        "Numeric value must not be negative"
                );
            }

            /*
             * Giá trị hợp lệ.
             */
            return parsedValue;

        } catch (NumberFormatException exception) {
            /*
             * Bao gồm cả trường hợp vượt giới hạn Integer.
             */
            throw new BronzeDataException(
                    errorCode,
                    "Numeric value has invalid Integer format"
            );
        }
    }
}