package com.network.preprocess.silver;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.IdentityResolutionSource;
import com.network.preprocess.model.InvalidIdentityReason;
import com.network.preprocess.silver.identity.MapBackedUeIdentityMappingLookup;
import com.network.preprocess.silver.identity.UeIdentityResolver;
import com.network.preprocess.testsupport.BronzeEventFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class UeIdentityResolverTest {

    private static final long PROCESSING_TIME =
            1_722_672_001_000L;

    private static final String IMSI_DIRECT =
            "452010123456789";

    private static final String IMSI_FROM_MSISDN =
            "452010987654321";

    private static final String IMSI_FROM_MTMSI =
            "452010111222333";

    private UeIdentityResolver resolver;

    @BeforeEach
    void setUp() {
        Map<String, String> msisdnMappings =
                Map.of(
                        "84901234567",
                        IMSI_FROM_MSISDN
                );

        Map<String, String> mtmsiMappings =
                Map.of(
                        "A1B2C3D4",
                        IMSI_FROM_MTMSI
                );

        resolver = new UeIdentityResolver(
                new MapBackedUeIdentityMappingLookup(
                        msisdnMappings,
                        mtmsiMappings
                )
        );
    }

    @Test
    void shouldUseDirectImsiWhenItIsValid() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        " 452010123456789 ",
                        "+84 90-123-4567",
                        "A1B2C3D4",
                        "3567890123456789"
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertTrue(result.isResolved());

        assertEquals(
                IMSI_DIRECT,
                result.getResolvedEvent().imsi()
        );

        assertEquals(
                IMSI_DIRECT,
                result.getResolvedEvent().ueKey()
        );

        assertEquals(
                IdentityResolutionSource.DIRECT_IMSI,
                result.getResolvedEvent()
                        .resolutionSource()
        );

        /*
         * BronzeEvent gốc phải được giữ để checkpoint sau
         * xây display metadata.
         */
        assertSame(
                event,
                result.getResolvedEvent()
                        .bronzeEvent()
        );
    }

    @Test
    void shouldPreferDirectImsiOverAllMappings() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        IMSI_DIRECT,
                        "84901234567",
                        "A1B2C3D4",
                        null
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertTrue(result.isResolved());

        /*
         * Dù MSISDN và MTMSI đều có mapping khác,
         * IMSI trực tiếp vẫn phải thắng.
         */
        assertEquals(
                IMSI_DIRECT,
                result.getResolvedEvent().imsi()
        );

        assertEquals(
                IdentityResolutionSource.DIRECT_IMSI,
                result.getResolvedEvent()
                        .resolutionSource()
        );
    }

    @Test
    void shouldResolveMissingImsiFromMsisdn() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        "+84 90-123-4567",
                        null,
                        null
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertTrue(result.isResolved());

        assertEquals(
                IMSI_FROM_MSISDN,
                result.getResolvedEvent().imsi()
        );

        assertEquals(
                IdentityResolutionSource.MSISDN_MAPPING,
                result.getResolvedEvent()
                        .resolutionSource()
        );
    }

    @Test
    void shouldFallbackToMtmsiWhenMsisdnMappingDoesNotExist() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        "84909999999",
                        "0xa1b2c3d4",
                        null
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertTrue(result.isResolved());

        assertEquals(
                IMSI_FROM_MTMSI,
                result.getResolvedEvent().imsi()
        );

        assertEquals(
                IdentityResolutionSource.MTMSI_MAPPING,
                result.getResolvedEvent()
                        .resolutionSource()
        );
    }

    @Test
    void shouldNotFallbackWhenDirectImsiHasInvalidFormat() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        "INVALID-IMSI",
                        "84901234567",
                        "A1B2C3D4",
                        null
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertFalse(result.isResolved());

        /*
         * MSISDN có mapping nhưng không được dùng,
         * vì IMSI hiện hữu nhưng bị lỗi.
         */
        assertEquals(
                InvalidIdentityReason.INVALID_DIRECT_IMSI,
                result.getInvalidRecord().reason()
        );
    }

    @Test
    void shouldRejectEventContainingOnlyImeisv() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        null,
                        null,
                        "3567890123456789"
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertFalse(result.isResolved());

        assertEquals(
                InvalidIdentityReason
                        .MISSING_IMSI_AND_ALIASES,
                result.getInvalidRecord().reason()
        );
    }

    @Test
    void shouldRejectInvalidAliases() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        "not-a-phone-number",
                        "invalid@mtmsi",
                        null
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertFalse(result.isResolved());

        assertEquals(
                InvalidIdentityReason
                        .INVALID_IDENTITY_ALIASES,
                result.getInvalidRecord().reason()
        );
    }

    @Test
    void shouldRouteValidButUnknownAliasesToMappingNotFound() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        "84909999999",
                        "FFFFFFFF",
                        null
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        assertFalse(result.isResolved());

        assertEquals(
                InvalidIdentityReason
                        .IDENTITY_MAPPING_NOT_FOUND,
                result.getInvalidRecord().reason()
        );
    }

    @Test
    void shouldCreateDeterministicInvalidIdentityId() {
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        null,
                        null,
                        "3567890123456789"
                );

        IdentityResolutionResult first =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        IdentityResolutionResult second =
                resolver.resolve(
                        event,
                        PROCESSING_TIME + 60_000L
                );

        /*
         * failedAt thay đổi nhưng ID không được thay đổi.
         */
        assertEquals(
                first.getInvalidRecord()
                        .invalidIdentityId(),
                second.getInvalidRecord()
                        .invalidIdentityId()
        );

        assertNotEquals(
                first.getInvalidRecord().failedAt(),
                second.getInvalidRecord().failedAt()
        );
    }

    @Test
    void invalidRecordToStringShouldNotExposeIdentity() {
        String sensitiveImsi = "INVALID-IMSI-VALUE";

        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        sensitiveImsi,
                        "84901234567",
                        null,
                        null
                );

        IdentityResolutionResult result =
                resolver.resolve(
                        event,
                        PROCESSING_TIME
                );

        String loggedValue =
                result.getInvalidRecord().toString();

        assertFalse(
                loggedValue.contains(sensitiveImsi)
        );

        assertFalse(
                loggedValue.contains("84901234567")
        );

        assertTrue(
                loggedValue.contains(
                        "originalEvent=<redacted>"
                )
        );
    }

    @Test
    void shouldFailFastWhenMappingContainsInvalidImsi() {
        /*
         * Mapping hỏng là lỗi reference data/configuration,
         * không phải lỗi của một event cụ thể.
         */
        assertThrows(
                IllegalArgumentException.class,
                () -> new MapBackedUeIdentityMappingLookup(
                        Map.of(
                                "84901234567",
                                "INVALID-MAPPED-IMSI"
                        ),
                        Map.of()
                )
        );
    }
}