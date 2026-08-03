package com.network.preprocess.silver;

import com.network.preprocess.silver.identity.IdentityNormalizer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IdentityNormalizerTest {

    @Test
    void shouldKeepValidImsiAsString() {
        /*
         * Không parse IMSI thành long.
         * Chuỗi định danh phải được giữ nguyên.
         */
        assertEquals(
                "452010123456789",
                IdentityNormalizer
                        .normalizeImsi(
                                " 452010123456789 "
                        )
                        .orElseThrow()
        );
    }

    @Test
    void shouldRejectImsiContainingNonDigitCharacters() {
        assertTrue(
                IdentityNormalizer
                        .normalizeImsi(
                                "45201ABC3456789"
                        )
                        .isEmpty()
        );
    }

    @Test
    void shouldNormalizeFormattedMsisdn() {
        assertEquals(
                "84901234567",
                IdentityNormalizer
                        .normalizeMsisdn(
                                "+84 90-123-4567"
                        )
                        .orElseThrow()
        );
    }

    @Test
    void shouldNormalizeHexMtmsi() {
        assertEquals(
                "A1B2C3D4",
                IdentityNormalizer
                        .normalizeMtmsi(
                                "0xa1b2c3d4"
                        )
                        .orElseThrow()
        );
    }

    @Test
    void shouldRejectMtmsiOutsideAcceptedFormat() {
        assertTrue(
                IdentityNormalizer
                        .normalizeMtmsi(
                                "MTMSI@INVALID"
                        )
                        .isEmpty()
        );
    }
}