package com.network.preprocess.model;

/**
 * Danh mục nguyên nhân khiến Silver không xác định được UE.
 *
 * <p>Đây là lỗi ở tầng Silver, không thêm các mã này vào
 * BronzeErrorCode.</p>
 */
public enum InvalidIdentityReason {

    /**
     * BronzeEvent có IMSI nhưng IMSI sai định dạng.
     *
     * <p>Nếu IMSI đã xuất hiện nhưng sai, Silver không âm thầm
     * fallback sang MSISDN/MTMSI. Làm như vậy có thể che giấu
     * dữ liệu IMSI bị hỏng.</p>
     */
    INVALID_DIRECT_IMSI,

    /**
     * Không có IMSI, MSISDN hoặc MTMSI để thực hiện resolve.
     *
     * <p>IMEISV không được tính là subscriber identity.</p>
     */
    MISSING_IMSI_AND_ALIASES,

    /**
     * Có MSISDN hoặc MTMSI nhưng tất cả alias đều sai định dạng.
     */
    INVALID_IDENTITY_ALIASES,

    /**
     * Có ít nhất một alias hợp lệ nhưng lookup không tìm thấy IMSI.
     */
    IDENTITY_MAPPING_NOT_FOUND
}