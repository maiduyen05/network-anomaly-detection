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

    @Test
    void shouldKeepSupportedEventWithUnknownResult() {
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

        assertTrue(result.isSupported());

        assertEquals(
                EventResult.UNKNOWN,
                result.getSilverEvent().eventResult()
        );

        assertFalse(
                result.getSilverEvent()
                        .quality()
                        .eventResultRecognized()
        );

        assertTrue(
                result.getSilverEvent()
                        .quality()
                        .warnings()
                        .contains(
                                "EVENT_RESULT_UNRECOGNIZED"
                        )
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