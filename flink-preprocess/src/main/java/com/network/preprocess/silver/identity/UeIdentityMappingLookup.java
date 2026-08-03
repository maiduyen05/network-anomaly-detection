package com.network.preprocess.silver.identity;

import java.io.Serializable;
import java.util.Optional;

/**
 * Cổng tra cứu alias của UE sang IMSI.
 *
 * <p>Interface giúp UeIdentityResolver không phụ thuộc trực tiếp vào:</p>
 *
 * <ul>
 *     <li>Database.</li>
 *     <li>Redis.</li>
 *     <li>Kafka compacted topic.</li>
 *     <li>Broadcast state.</li>
 *     <li>File snapshot.</li>
 * </ul>
 *
 * <p>Nguồn mapping thật sẽ được cắm vào interface này sau khi
 * contract nguồn reference data được chốt.</p>
 */
public interface UeIdentityMappingLookup extends Serializable {

    /**
     * Tra IMSI bằng MSISDN đã được chuẩn hóa.
     *
     * @param normalizedMsisdn MSISDN chỉ chứa chữ số
     * @return IMSI nếu mapping tồn tại
     */
    Optional<String> findImsiByMsisdn(
            String normalizedMsisdn
    );

    /**
     * Tra IMSI bằng MTMSI đã được chuẩn hóa.
     *
     * @param normalizedMtmsi MTMSI chuẩn hóa
     * @return IMSI nếu mapping tồn tại
     */
    Optional<String> findImsiByMtmsi(
            String normalizedMtmsi
    );
}