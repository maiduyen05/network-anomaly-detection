package com.network.preprocess.silver;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.InvalidIdentityReason;
import com.network.preprocess.model.InvalidIdentityRecord;
import com.network.preprocess.silver.identity.MapBackedUeIdentityMappingLookup;
import com.network.preprocess.silver.identity.UeIdentityResolver;
import com.network.preprocess.testsupport.BronzeEventFixtures;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class SilverIdentityProcessFunctionTest {

    private OneInputStreamOperatorTestHarness<
            BronzeEvent,
            IdentityResolvedEvent
            > harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void shouldEmitResolvedEventToMainOutput()
            throws Exception {

        UeIdentityResolver resolver =
                new UeIdentityResolver(
                        new MapBackedUeIdentityMappingLookup(
                                Map.of(),
                                Map.of()
                        )
                );

        SilverIdentityProcessFunction function =
                new SilverIdentityProcessFunction(
                        resolver
                );

        /*
         * Factory tạo test harness phù hợp cho ProcessFunction.
         * Harness cho phép điều khiển processing time và đọc side output.
         */
        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(function);

        harness.setProcessingTime(
                1_722_672_001_000L
        );

        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        "452010123456789",
                        null,
                        null,
                        null
                );

        harness.processElement(
                event,
                100L
        );

        List<IdentityResolvedEvent> mainOutput =
                harness.extractOutputValues();

        assertEquals(
                1,
                mainOutput.size()
        );

        assertEquals(
                "452010123456789",
                mainOutput.get(0).ueKey()
        );
    }

    @Test
    void shouldEmitUnresolvedEventToInvalidIdentitySideOutput()
            throws Exception {

        UeIdentityResolver resolver =
                new UeIdentityResolver(
                        new MapBackedUeIdentityMappingLookup(
                                Map.of(),
                                Map.of()
                        )
                );

        SilverIdentityProcessFunction function =
                new SilverIdentityProcessFunction(
                        resolver
                );

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(function);

        harness.setProcessingTime(
                1_722_672_001_000L
        );

        /*
         * Chỉ có IMEISV nên không thể xác định subscriber.
         */
        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        null,
                        null,
                        "3567890123456789"
                );

        harness.processElement(
                event,
                100L
        );

        /*
         * Record invalid không được xuất ra main output.
         */
        assertTrue(
                harness.extractOutputValues().isEmpty()
        );

        ConcurrentLinkedQueue<
                StreamRecord<InvalidIdentityRecord>
                > sideOutput =
                harness.getSideOutput(
                        SilverIdentityProcessFunction
                                .INVALID_IDENTITY_TAG
                );

        assertNotNull(sideOutput);
        assertEquals(1, sideOutput.size());

        InvalidIdentityRecord invalidRecord =
                sideOutput.peek().getValue();

        assertEquals(
                InvalidIdentityReason
                        .MISSING_IMSI_AND_ALIASES,
                invalidRecord.reason()
        );

        assertEquals(
                "raw-record-001",
                invalidRecord.rawRecordId()
        );

        assertEquals(
                "2024-08-03T08:00:01Z",
                invalidRecord.failedAt()
        );
    }

    @Test
    void shouldResolveMsisdnMappingInsideProcessFunction()
            throws Exception {

        UeIdentityResolver resolver =
                new UeIdentityResolver(
                        new MapBackedUeIdentityMappingLookup(
                                Map.of(
                                        "84901234567",
                                        "452010987654321"
                                ),
                                Map.of()
                        )
                );

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(
                                new SilverIdentityProcessFunction(
                                        resolver
                                )
                        );

        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        null,
                        "+84 90-123-4567",
                        null,
                        null
                );

        harness.processElement(
                event,
                100L
        );

        List<IdentityResolvedEvent> output =
                harness.extractOutputValues();

        assertEquals(1, output.size());

        assertEquals(
                "452010987654321",
                output.get(0).imsi()
        );
    }
}