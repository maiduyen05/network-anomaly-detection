package com.network.preprocess.model;

/**
 * Cho biết Silver đã xác định IMSI bằng cách nào.
 *
 * <p>Field này giúp:</p>
 *
 * <ul>
 *     <li>Đối soát chất lượng identity.</li>
 *     <li>Đo tỷ lệ record có IMSI trực tiếp.</li>
 *     <li>Đo tỷ lệ record phải dùng bảng mapping.</li>
 *     <li>Phát hiện mapping MSISDN/MTMSI có vấn đề.</li>
 * </ul>
 */
public enum IdentityResolutionSource {

    /**
     * BronzeEvent đã có IMSI hợp lệ.
     *
     * <p>Đây là nguồn được ưu tiên cao nhất.</p>
     */
    DIRECT_IMSI,

    /**
     * IMSI rỗng và được tìm thấy bằng MSISDN.
     */
    MSISDN_MAPPING,

    /**
     * IMSI rỗng, không tìm thấy bằng MSISDN và cuối cùng
     * được tìm thấy bằng MTMSI.
     */
    MTMSI_MAPPING
}