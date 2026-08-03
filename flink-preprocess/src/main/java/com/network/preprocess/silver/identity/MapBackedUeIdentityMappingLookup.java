package com.network.preprocess.silver.identity;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Lookup identity bằng Map bất biến.
 *
 * <p>Implementation này phù hợp cho:</p>
 *
 * <ul>
 *     <li>Unit test.</li>
 *     <li>Local development.</li>
 *     <li>Snapshot mapping nhỏ được tải khi khởi động job.</li>
 * </ul>
 *
 * <p>Không sửa Map sau khi job đã chạy. Mapping động sẽ cần
 * implementation khác, ví dụ reference stream/broadcast state.</p>
 */
public final class MapBackedUeIdentityMappingLookup
        implements UeIdentityMappingLookup {

    private final Map<String, String> msisdnToImsi;
    private final Map<String, String> mtmsiToImsi;

    public MapBackedUeIdentityMappingLookup(
            Map<String, String> msisdnToImsi,
            Map<String, String> mtmsiToImsi
    ) {
        this.msisdnToImsi =
                normalizeMsisdnMappings(msisdnToImsi);

        this.mtmsiToImsi =
                normalizeMtmsiMappings(mtmsiToImsi);
    }

    @Override
    public Optional<String> findImsiByMsisdn(
            String normalizedMsisdn
    ) {
        return Optional.ofNullable(
                msisdnToImsi.get(normalizedMsisdn)
        );
    }

    @Override
    public Optional<String> findImsiByMtmsi(
            String normalizedMtmsi
    ) {
        return Optional.ofNullable(
                mtmsiToImsi.get(normalizedMtmsi)
        );
    }

    private static Map<String, String> normalizeMsisdnMappings(
            Map<String, String> source
    ) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> result =
                new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : source.entrySet()) {
            String normalizedKey =
                    IdentityNormalizer
                            .normalizeMsisdn(entry.getKey())
                            .orElseThrow(
                                    () -> new IllegalArgumentException(
                                            "MSISDN mapping contains "
                                                    + "an invalid key"
                                    )
                            );

            String normalizedImsi =
                    normalizeMappedImsi(entry.getValue());

            putWithoutConflict(
                    result,
                    normalizedKey,
                    normalizedImsi,
                    "MSISDN"
            );
        }

        return Collections.unmodifiableMap(result);
    }

    private static Map<String, String> normalizeMtmsiMappings(
            Map<String, String> source
    ) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> result =
                new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : source.entrySet()) {
            String normalizedKey =
                    IdentityNormalizer
                            .normalizeMtmsi(entry.getKey())
                            .orElseThrow(
                                    () -> new IllegalArgumentException(
                                            "MTMSI mapping contains "
                                                    + "an invalid key"
                                    )
                            );

            String normalizedImsi =
                    normalizeMappedImsi(entry.getValue());

            putWithoutConflict(
                    result,
                    normalizedKey,
                    normalizedImsi,
                    "MTMSI"
            );
        }

        return Collections.unmodifiableMap(result);
    }

    /**
     * IMSI lấy từ reference mapping cũng phải được validate.
     *
     * <p>Mapping sai là lỗi cấu hình/reference data, không phải lỗi
     * của BronzeEvent hiện tại. Vì vậy constructor fail ngay thay vì
     * âm thầm đưa event vào invalid-identity.</p>
     */
    private static String normalizeMappedImsi(
            String rawImsi
    ) {
        return IdentityNormalizer
                .normalizeImsi(rawImsi)
                .orElseThrow(
                        () -> new IllegalArgumentException(
                                "Identity mapping contains "
                                        + "an invalid IMSI"
                        )
                );
    }

    /**
     * Phát hiện hai key sau chuẩn hóa cùng trỏ đến hai IMSI khác nhau.
     *
     * <p>Ví dụ "+8490..." và "8490..." trở thành cùng một key.
     * Nếu chúng trỏ đến hai IMSI khác nhau thì phải dừng cấu hình,
     * không được chọn ngẫu nhiên.</p>
     */
    private static void putWithoutConflict(
            Map<String, String> destination,
            String key,
            String imsi,
            String aliasType
    ) {
        String previous = destination.putIfAbsent(
                key,
                imsi
        );

        if (previous != null && !previous.equals(imsi)) {
            throw new IllegalArgumentException(
                    aliasType
                            + " mapping contains conflicting entries"
            );
        }
    }
}