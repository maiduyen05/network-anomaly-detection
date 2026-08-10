package com.network.preprocess.silver;

import com.network.preprocess.model.EventDefinition;
import com.network.preprocess.model.EventResult;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.UnsupportedEventReason;
import com.network.preprocess.silver.event.MapBackedEventCatalog;
import com.network.preprocess.silver.event.SilverEventTransformer;
import com.network.preprocess.testsupport.IdentityResolvedEventFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SilverEventTransformerTest {

    private static final long PROCESSING_TIME =
            1_722_672_001_000L;

    private SilverEventTransformer transformer;

    @BeforeEach
    void setUp() {
        EventDefinition serviceRequest =
                new EventDefinition(
                        "l_service_request",
                        "Service Request"
                );

        transformer =
                new SilverEventTransformer(
                        new MapBackedEventCatalog(
                                Map.of(
                                        "l_service_request",
                                        serviceRequest
                                )
                        )
                );
    }

    @Test
    void shouldCreateSilverEventForSupportedEvent() {
        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "l_service_request",
                        "success"
                );

        SilverTransformationResult result =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        assertTrue(result.isSupported());

        assertEquals(
                "452010123456789",
                result.getSilverEvent().ueKey()
        );

        assertEquals(
                "l_service_request",
                result.getSilverEvent().eventId()
        );

        assertEquals(
                EventResult.SUCCESS,
                result.getSilverEvent().eventResult()
        );

        assertEquals(
                "Service Request",
                result.getSilverEvent()
                        .display()
                        .eventName()
        );

        assertTrue(
                result.getSilverEvent()
                        .quality()
                        .eventResultRecognized()
        );
    }

    @Test
    void shouldNormalizeSupportedEventAlias() {
        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        " L_SERVICE_REQUEST ",
                        " SUCCESS "
                );

        SilverTransformationResult result =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        assertTrue(result.isSupported());

        assertEquals(
                "l_service_request",
                result.getSilverEvent().eventId()
        );

        assertTrue(
                result.getSilverEvent()
                        .quality()
                        .eventIdChanged()
        );

        assertTrue(
                result.getSilverEvent()
                        .quality()
                        .eventResultChanged()
        );

        assertTrue(
                result.getSilverEvent()
                        .quality()
                        .warnings()
                        .contains("EVENT_ID_NORMALIZED")
        );

        assertTrue(
                result.getSilverEvent()
                        .quality()
                        .warnings()
                        .contains("EVENT_RESULT_NORMALIZED")
        );
    }

        /**
         * EVENT_RESULT có giá trị nhưng không nằm trong
         * vocabulary của model phải được route sang
         * unsupported-event.
         *
         * <p>
         * Model v1 chỉ hỗ trợ:
         * </p>
         *
         * <ul>
         *     <li>reject</li>
         *     <li>success</li>
         * </ul>
         *
         * <p>
         * Không tạo EventResult.UNKNOWN vì Gold không có
         * vocabulary ID tương ứng để encode.
         * </p>
         */
        @Test
        void shouldRouteUnknownEventResultToUnsupported() {

        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "l_service_request",
                        "vendor-result-15"
                );

        SilverTransformationResult result =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        /*
        * Event ID hợp lệ nhưng EVENT_RESULT không thuộc
        * feature contract nên không được tạo SilverEvent.
        */
        assertFalse(
                result.isSupported()
        );

        /*
        * EVENT_RESULT có giá trị nhưng không được model hỗ trợ.
        */
        assertEquals(
                UnsupportedEventReason.UNSUPPORTED_EVENT_RESULT,
                result.getUnsupportedEvent().reason()
        );
        }

        /**
         * EVENT_RESULT bị thiếu phải được phân biệt
         * với EVENT_RESULT có giá trị nhưng không được hỗ trợ.
         */
        @Test
        void shouldRouteMissingEventResultToUnsupported() {

        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "l_service_request",
                        null
                );

        SilverTransformationResult result =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        assertFalse(
                result.isSupported()
        );

        assertEquals(
                UnsupportedEventReason.MISSING_EVENT_RESULT,
                result.getUnsupportedEvent().reason()
        );
        }

    @Test
    void shouldRouteUnknownEventIdToUnsupported() {
        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "vendor_unknown_event",
                        "success"
                );

        SilverTransformationResult result =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        assertFalse(result.isSupported());

        assertEquals(
                UnsupportedEventReason.UNSUPPORTED_EVENT_ID,
                result.getUnsupportedEvent().reason()
        );
    }

    @Test
    void shouldRouteMissingEventIdToUnsupported() {
        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        null,
                        "success"
                );

        SilverTransformationResult result =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        assertFalse(result.isSupported());

        assertEquals(
                UnsupportedEventReason.MISSING_EVENT_ID,
                result.getUnsupportedEvent().reason()
        );
    }

    @Test
    void shouldCreateDeterministicSilverEventId() {
        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "l_service_request",
                        "success"
                );

        SilverTransformationResult first =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        SilverTransformationResult second =
                transformer.transform(
                        input,
                        PROCESSING_TIME + 60_000L
                );

        assertEquals(
                first.getSilverEvent().silverEventId(),
                second.getSilverEvent().silverEventId()
        );
    }

    @Test
    void unsupportedRecordToStringShouldNotExposeIdentity() {
        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "vendor_unknown_event",
                        "success"
                );

        SilverTransformationResult result =
                transformer.transform(
                        input,
                        PROCESSING_TIME
                );

        String loggedValue =
                result.getUnsupportedEvent().toString();

        assertFalse(
                loggedValue.contains("452010123456789")
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
    void shouldRejectNonNormalizedCanonicalId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new MapBackedEventCatalog(
                        Map.of(
                                "service-request",
                                new EventDefinition(
                                        "SERVICE-REQUEST",
                                        "Service Request"
                                )
                        )
                )
        );
    }
}