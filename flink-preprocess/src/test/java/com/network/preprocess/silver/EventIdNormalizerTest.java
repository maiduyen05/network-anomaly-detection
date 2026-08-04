package com.network.preprocess.silver;

import com.network.preprocess.silver.event.EventIdNormalizer;

//JUnit 5: framework để viết và chạy unit test cho Java 
import org.junit.jupiter.api.Test;

// assertEquals: kiểm tra gt thực tế có bằng gt mong đợi không 
import static org.junit.jupiter.api.Assertions.assertEquals;

// assertTrue: kiểm tra một điều kiện có đúng không
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventIdNormalizerTest {

    @Test
    void shouldNormalizeCaseAndWhitespace() {
        assertEquals(
                "l_service_request",
                EventIdNormalizer
                        .normalizeLookupKey(
                                " L_SERVICE_REQUEST "
                        )
                        .orElseThrow()
        );
    }

    @Test
    void shouldNormalizeDifferentSeparators() {
        assertEquals(
                "l_service_request",
                EventIdNormalizer
                        .normalizeLookupKey(
                                "L-Service Request"
                        )
                        .orElseThrow()
        );
    }

    @Test
    void shouldRejectBlankEventId() {
        assertTrue(
                EventIdNormalizer
                        .normalizeLookupKey("   ")
                        .isEmpty()
        );
    }

    @Test
    void shouldRejectValueContainingOnlySeparators() {
        assertTrue(
                EventIdNormalizer
                        .normalizeLookupKey("---___")
                        .isEmpty()
        );
    }
}