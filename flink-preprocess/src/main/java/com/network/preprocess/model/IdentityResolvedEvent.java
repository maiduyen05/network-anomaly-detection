package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Event trung gian sau khi xác định UE identity.
 *
 * Chưa chuẩn hóa event_id, event_result và xây nhóm display, quality.
 *
 * @param ueKey subscriber key dùng cho keyBy ở bước sau
 * @param imsi IMSI đã được chuẩn hóa
 * @param resolutionSource cách Silver xác định được IMSI
 * @param bronzeEvent toàn bộ BronzeEvent ban đầu
 */
public record IdentityResolvedEvent(
        String ueKey,
        String imsi,
        IdentityResolutionSource resolutionSource,
        BronzeEvent bronzeEvent
) implements Serializable {

    /**
     * Compact constructor khóa invariant của pipeline:
     *
     * <ul>
     *     <li>ueKey không được null.</li>
     *     <li>imsi không được null.</li>
     *     <li>ueKey phải chính là IMSI đã resolve.</li>
     *     <li>BronzeEvent phải được giữ đầy đủ.</li>
     * </ul>
     */
    public IdentityResolvedEvent {
        Objects.requireNonNull(
                ueKey,
                "ueKey must not be null"
        );

        Objects.requireNonNull(
                imsi,
                "imsi must not be null"
        );

        Objects.requireNonNull(
                resolutionSource,
                "resolutionSource must not be null"
        );

        Objects.requireNonNull(
                bronzeEvent,
                "bronzeEvent must not be null"
        );

        if (!ueKey.equals(imsi)) {
            throw new IllegalArgumentException(
                    "ueKey must be the resolved IMSI"
            );
        }
    }
}