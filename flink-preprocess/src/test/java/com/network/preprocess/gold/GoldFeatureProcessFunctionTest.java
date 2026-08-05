package com.network.preprocess.gold;

import com.network.preprocess.gold.feature
        .GoldFeatureEncodingException;
import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceSample;
import com.network.preprocess.model.GoldSequenceWindow;
import com.network.preprocess.model.InvalidGoldFeatureRecord;
import org.apache.flink.streaming.runtime.streamrecord
        .StreamRecord;
import org.apache.flink.streaming.util
        .OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util
        .ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class GoldFeatureProcessFunctionTest {

    private OneInputStreamOperatorTestHarness<
            GoldSequenceWindow,
            GoldSequenceSample
            > harness;

    @AfterEach
    void tearDown() throws Exception {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void shouldEmitModelReadySampleToMainOutput()
            throws Exception {

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(
                                new GoldFeatureProcessFunction(
                                        "invalid-gold-feature-v1"
                                )
                        );

        GoldSequenceWindow window =
                createWindow(
                        "l_attach"
                );

        harness.processElement(
                window,
                100L
        );

        List<GoldSequenceSample> output =
                harness.extractOutputValues();

        assertEquals(
                1,
                output.size()
        );

        GoldSequenceSample sample =
                output.get(0);

        assertEquals(
                "sample-001",
                sample.getSampleId()
        );

        assertEquals(
                "gold-sequence-v1",
                sample.getSchemaVersion()
        );

        assertEquals(
                "gold-ue-sequence-feature-v1",
                sample.getFeatureVersion()
        );

        /*
         * Kiểm tra đúng shape của hai tensor.
         */
        assertEquals(
                32,
                sample.getModelInput()
                        .getXCat()
                        .length
        );

        assertEquals(
                4,
                sample.getModelInput()
                        .getXCat()[0]
                        .length
        );

        assertEquals(
                32,
                sample.getModelInput()
                        .getXNum()
                        .length
        );

        assertEquals(
                2,
                sample.getModelInput()
                        .getXNum()[0]
                        .length
        );

        /*
         * l_attach=1, success=1, cause ""=0, sub-cause ""=0.
         */
        assertArrayEquals(
                new long[]{1L, 1L, 0L, 0L},
                sample.getModelInput()
                        .getXCat()[0]
        );

        /*
         * Evidence phải giữ đủ 32 event.
         */
        assertEquals(
                32,
                sample.getEvidence()
                        .getEvents()
                        .size()
        );

        /*
         * Window hợp lệ không được tạo side output lỗi.
         */
        ConcurrentLinkedQueue<
                StreamRecord<InvalidGoldFeatureRecord>
                > sideOutput =
                harness.getSideOutput(
                        GoldFeatureProcessFunction
                                .INVALID_FEATURE_TAG
                );

        assertNull(sideOutput);
    }

    @Test
    void shouldRouteUnknownCategoryToSideOutput()
            throws Exception {

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(
                                new GoldFeatureProcessFunction(
                                        "invalid-gold-feature-v1"
                                )
                        );

        /*
         * Đặt processing time cố định để failedAt deterministic.
         */
        harness.setProcessingTime(
                1_722_672_001_000L
        );

        GoldSequenceWindow window =
                createWindow(
                        "l_new_event"
                );

        harness.processElement(
                window,
                100L
        );

        /*
         * Window lỗi không được đi vào main output.
         */
        assertTrue(
                harness.extractOutputValues().isEmpty()
        );

        ConcurrentLinkedQueue<
                StreamRecord<InvalidGoldFeatureRecord>
                > sideOutput =
                harness.getSideOutput(
                        GoldFeatureProcessFunction
                                .INVALID_FEATURE_TAG
                );

        assertNotNull(sideOutput);
        assertEquals(
                1,
                sideOutput.size()
        );

        InvalidGoldFeatureRecord invalidRecord =
                sideOutput
                        .peek()
                        .getValue();

        assertEquals(
                "sample-001",
                invalidRecord.getSampleId()
        );

        assertEquals(
                "event_code",
                invalidRecord.getFeatureName()
        );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.UNKNOWN_CATEGORY,
                invalidRecord.getReason()
        );

        assertEquals(
                "l_new_event",
                invalidRecord.getRejectedValue()
        );

        assertEquals(
                "invalid-gold-feature-v1",
                invalidRecord.getSchemaVersion()
        );

        assertNotNull(
                invalidRecord.getRejectedWindow()
        );
    }

    /**
     * Tạo window đúng 32 event.
     *
     * @param firstEventId EVENT_ID của event đầu tiên
     */
    private static GoldSequenceWindow createWindow(
            String firstEventId
    ) {
        Instant startTime =
                Instant.parse(
                        "2026-07-08T10:00:00Z"
                );

        List<GoldSequenceEvent> events =
                new ArrayList<>();

        for (int index = 0;
             index < 32;
             index++) {

            String eventId =
                    index == 0
                            ? firstEventId
                            : "l_attach";

            GoldSequenceEvent event =
                    new GoldSequenceEvent();

            event.setUeKey(
                    "452040000000001"
            );

            event.setImsi(
                    "452040000000001"
            );

            event.setEventId(
                    eventId
            );

            event.setEventResult(
                    "success"
            );

            /*
             * Chuỗi rỗng là category hợp lệ, ID 0.
             */
            event.setNormalizedCauseCode("");
            event.setSubCauseCode("");

            event.setDurationMs(
                    100L
            );

            event.setRequestRetries(
                    0
            );

            event.setEventTimeEpochMs(
                    startTime
                            .plusSeconds(index)
                            .toEpochMilli()
            );

            event.setFeatureSourceFields(
                    new LinkedHashMap<>()
            );

            event.setDisplayFields(
                    new LinkedHashMap<>()
            );

            event.setQualityFields(
                    new LinkedHashMap<>()
            );

            event.setSourceOrderKey(
                    "raw-record-" + index
            );

            events.add(
                    event
            );
        }

        return new GoldSequenceWindow(
                "gold-sequence-v1",
                "gold-ue-sequence-feature-v1",
                "sample-001",
                "452040000000001",
                "452040000000001",
                startTime,
                startTime.plusSeconds(31),
                32,
                8,
                events
        );
    }
}