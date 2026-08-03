package com.network.preprocess.silver.identity;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.IdentityResolutionSource;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.InvalidIdentityReason;
import com.network.preprocess.model.InvalidIdentityRecord;
import com.network.preprocess.silver.IdentityResolutionResult;
import com.network.preprocess.util.DeterministicId;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Resolve BronzeEvent về một IMSI duy nhất.
 *
 * <p>Thứ tự xử lý:</p>
 *
 * <ol>
 *     <li>Nếu IMSI có giá trị: validate và dùng trực tiếp.</li>
 *     <li>Nếu IMSI rỗng: thử mapping bằng MSISDN.</li>
 *     <li>Nếu chưa được: thử mapping bằng MTMSI.</li>
 *     <li>Nếu vẫn không được: tạo InvalidIdentityRecord.</li>
 * </ol>
 *
 * <p>IMEISV không bao giờ được dùng làm ueKey.</p>
 *
 * <p>Class không phụ thuộc Flink API nên business logic có thể
 * được unit test trực tiếp.</p>
 */
public final class UeIdentityResolver
        implements Serializable {

    private static final String INVALID_SCHEMA_VERSION =
            "invalid-identity-v1";

    private final UeIdentityMappingLookup mappingLookup;

    public UeIdentityResolver(
            UeIdentityMappingLookup mappingLookup
    ) {
        this.mappingLookup = Objects.requireNonNull(
                mappingLookup,
                "mappingLookup must not be null"
        );
    }

    /**
     * Resolve một BronzeEvent.
     *
     * @param event BronzeEvent đầu vào
     * @param processingTimeMillis thời gian xử lý của Silver
     * @return resolved event hoặc invalid identity record
     */
    public IdentityResolutionResult resolve(
            BronzeEvent event,
            long processingTimeMillis
    ) {
        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        /*
         * Bronze contract phải luôn có rawRecordId.
         *
         * Nếu field này mất thì đó là lỗi contract/code giữa Bronze
         * và Silver, không phải lỗi identity để route sang side output.
         */
        Objects.requireNonNull(
                event.rawRecordId(),
                "BronzeEvent.rawRecordId must not be null"
        );

        /*
         * Quy tắc quan trọng:
         *
         * Chỉ fallback sang alias khi IMSI rỗng.
         * Nếu IMSI có dữ liệu nhưng sai format, route invalid ngay.
         */
        if (IdentityNormalizer.hasText(event.imsi())) {
            Optional<String> normalizedImsi =
                    IdentityNormalizer.normalizeImsi(
                            event.imsi()
                    );

            if (normalizedImsi.isEmpty()) {
                return invalid(
                        event,
                        InvalidIdentityReason
                                .INVALID_DIRECT_IMSI,
                        processingTimeMillis
                );
            }

            return resolved(
                    event,
                    normalizedImsi.get(),
                    IdentityResolutionSource.DIRECT_IMSI
            );
        }

        boolean hasAnyAlias =
                IdentityNormalizer.hasText(event.msisdn())
                        || IdentityNormalizer.hasText(
                                event.mtmsi()
                        );

        boolean hasAnyValidAlias = false;

        /*
         * Ưu tiên mapping MSISDN trước MTMSI.
         */
        Optional<String> normalizedMsisdn =
                IdentityNormalizer.normalizeMsisdn(
                        event.msisdn()
                );

        if (normalizedMsisdn.isPresent()) {
            hasAnyValidAlias = true;

            Optional<String> mappedImsi =
                    requireValidLookupResult(
                            mappingLookup.findImsiByMsisdn(
                                    normalizedMsisdn.get()
                            ),
                            "MSISDN"
                    );

            if (mappedImsi.isPresent()) {
                return resolved(
                        event,
                        mappedImsi.get(),
                        IdentityResolutionSource
                                .MSISDN_MAPPING
                );
            }
        }

        /*
         * MSISDN không hợp lệ hoặc không tìm thấy mapping:
         * tiếp tục thử MTMSI.
         */
        Optional<String> normalizedMtmsi =
                IdentityNormalizer.normalizeMtmsi(
                        event.mtmsi()
                );

        if (normalizedMtmsi.isPresent()) {
            hasAnyValidAlias = true;

            Optional<String> mappedImsi =
                    requireValidLookupResult(
                            mappingLookup.findImsiByMtmsi(
                                    normalizedMtmsi.get()
                            ),
                            "MTMSI"
                    );

            if (mappedImsi.isPresent()) {
                return resolved(
                        event,
                        mappedImsi.get(),
                        IdentityResolutionSource
                                .MTMSI_MAPPING
                );
            }
        }

        /*
         * Không có IMSI, MSISDN và MTMSI.
         *
         * Dù IMEISV tồn tại, event vẫn invalid vì IMEISV chỉ
         * định danh thiết bị, không định danh thuê bao.
         */
        if (!hasAnyAlias) {
            return invalid(
                    event,
                    InvalidIdentityReason
                            .MISSING_IMSI_AND_ALIASES,
                    processingTimeMillis
            );
        }

        /*
         * Có alias nhưng tất cả đều sai định dạng.
         */
        if (!hasAnyValidAlias) {
            return invalid(
                    event,
                    InvalidIdentityReason
                            .INVALID_IDENTITY_ALIASES,
                    processingTimeMillis
            );
        }

        /*
         * Có ít nhất một alias hợp lệ nhưng không lookup được IMSI.
         */
        return invalid(
                event,
                InvalidIdentityReason
                        .IDENTITY_MAPPING_NOT_FOUND,
                processingTimeMillis
        );
    }

    private IdentityResolutionResult resolved(
            BronzeEvent event,
            String normalizedImsi,
            IdentityResolutionSource source
    ) {
        /*
         * ueKey ở pipeline hiện tại chính là IMSI chuẩn hóa.
         */
        IdentityResolvedEvent resolvedEvent =
                new IdentityResolvedEvent(
                        normalizedImsi,
                        normalizedImsi,
                        source,
                        event
                );

        return IdentityResolutionResult.resolved(
                resolvedEvent
        );
    }

    /**
     * Kiểm tra contract của mapping provider.
     *
     * <p>Nếu provider trả null hoặc IMSI sai thì đây là lỗi reference
     * data/code. Job phải fail để được phát hiện, không được biến thành
     * lỗi của event hiện tại.</p>
     */
    private Optional<String> requireValidLookupResult(
            Optional<String> lookupResult,
            String aliasType
    ) {
        Objects.requireNonNull(
                lookupResult,
                aliasType + " lookup must not return null"
        );

        if (lookupResult.isEmpty()) {
            return Optional.empty();
        }

        String normalizedImsi =
                IdentityNormalizer
                        .normalizeImsi(lookupResult.get())
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        aliasType
                                                + " lookup returned "
                                                + "an invalid IMSI"
                                )
                        );

        return Optional.of(normalizedImsi);
    }

    private IdentityResolutionResult invalid(
            BronzeEvent event,
            InvalidIdentityReason reason,
            long processingTimeMillis
    ) {
        /*
         * ID không chứa failedAt.
         *
         * Cùng một raw record và cùng nguyên nhân luôn tạo cùng ID,
         * kể cả khi Flink restart và xử lý lại record.
         */
        String invalidIdentityId =
                DeterministicId.sha256(
                        "invalid-identity:"
                                + event.rawRecordId()
                                + ":"
                                + reason.name()
                );

        String failedAt =
                Instant.ofEpochMilli(
                        processingTimeMillis
                ).toString();

        InvalidIdentityRecord invalidRecord =
                new InvalidIdentityRecord(
                        INVALID_SCHEMA_VERSION,
                        invalidIdentityId,
                        event.rawRecordId(),
                        reason,
                        safeMessage(reason),
                        failedAt,
                        event
                );

        return IdentityResolutionResult.invalid(
                invalidRecord
        );
    }

    /**
     * Message chỉ mô tả loại lỗi, không sao chép IMSI/MSISDN/MTMSI.
     */
    private String safeMessage(
            InvalidIdentityReason reason
    ) {
        return switch (reason) {
            case INVALID_DIRECT_IMSI ->
                    "Direct IMSI has invalid format";

            case MISSING_IMSI_AND_ALIASES ->
                    "IMSI, MSISDN and MTMSI are missing";

            case INVALID_IDENTITY_ALIASES ->
                    "Identity aliases have invalid format";

            case IDENTITY_MAPPING_NOT_FOUND ->
                    "No IMSI mapping was found for identity aliases";
        };
    }
}