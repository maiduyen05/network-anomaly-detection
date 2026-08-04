package com.network.preprocess.silver;

import com.network.preprocess.model.EventDefinition;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.UnsupportedEventReason;
import com.network.preprocess.model.UnsupportedEventRecord;
import com.network.preprocess.silver.event.MapBackedEventCatalog;
import com.network.preprocess.silver.event.SilverEventTransformer;
import com.network.preprocess.testsupport.IdentityResolvedEventFixtures;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class SilverEventProcessFunctionTest {

    private OneInputStreamOperatorTestHarness<
            IdentityResolvedEvent,
            SilverEvent
            > harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void shouldEmitSupportedEventToMainOutput()
            throws Exception {

        SilverEventProcessFunction function =
                createFunction();

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(function);

        harness.setProcessingTime(
                1_722_672_001_000L
        );

        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "l_service_request",
                        "success"
                );

        harness.processElement(
                input,
                100L
        );

        List<SilverEvent> output =
                harness.extractOutputValues();

        assertEquals(1, output.size());

        assertEquals(
                "l_service_request",
                output.get(0).eventId()
        );

        assertEquals(
                "452010123456789",
                output.get(0).ueKey()
        );
    }

    @Test
    void shouldEmitUnsupportedEventToSideOutput()
            throws Exception {

        SilverEventProcessFunction function =
                createFunction();

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(function);

        harness.setProcessingTime(
                1_722_672_001_000L
        );

        IdentityResolvedEvent input =
                IdentityResolvedEventFixtures.event(
                        "vendor_unknown_event",
                        "success"
                );

        harness.processElement(
                input,
                100L
        );

        assertTrue(
                harness.extractOutputValues().isEmpty()
        );

        ConcurrentLinkedQueue<
                StreamRecord<UnsupportedEventRecord>
                > sideOutput =
                harness.getSideOutput(
                        SilverEventProcessFunction
                                .UNSUPPORTED_EVENT_TAG
                );

        assertNotNull(sideOutput);
        assertEquals(1, sideOutput.size());

        UnsupportedEventRecord record =
                sideOutput.peek().getValue();

        assertEquals(
                UnsupportedEventReason.UNSUPPORTED_EVENT_ID,
                record.reason()
        );

        assertEquals(
                "raw-record-001",
                record.rawRecordId()
        );

        assertEquals(
                "2024-08-03T08:00:01Z",
                record.failedAt()
        );
    }

    private SilverEventProcessFunction createFunction() {
        EventDefinition serviceRequest =
                new EventDefinition(
                        "l_service_request",
                        "Service Request"
                );

        SilverEventTransformer transformer =
                new SilverEventTransformer(
                        new MapBackedEventCatalog(
                                Map.of(
                                        "l_service_request",
                                        serviceRequest
                                )
                        )
                );

        return new SilverEventProcessFunction(
                transformer
        );
    }
}