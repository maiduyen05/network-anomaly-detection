package com.network.preprocess.gold;

import com.network.preprocess.config.GoldFeatureContract;
import com.network.preprocess.config.GoldJobConfig;
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Kiểm thử GoldFeatureProcessFunction.
 *
 * <p>
 * ProcessFunction có hai nhiệm vụ:
 * </p>
 *
 * <ul>
 *     <li>
 *         Window hợp lệ -> GoldSequenceSample ở main output.
 *     </li>
 *     <li>
 *         Window vi phạm feature contract ->
 *         InvalidGoldFeatureRecord ở side output.
 *     </li>
 * </ul>
 */
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


    /**
     * Window hợp lệ phải tạo GoldSequenceSample.
     */
    @Test
    void shouldEmitModelReadySampleToMainOutput()
            throws Exception {

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(
                                createProcessFunction()
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


        /*
         * =========================================================
         * SAMPLE METADATA
         * =========================================================
         */

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
         * =========================================================
         * MODEL SHAPE
         * =========================================================
         */

        assertEquals(
                32,
                sample
                        .getModelInput()
                        .getXCat()
                        .length
        );

        assertEquals(
                4,
                sample
                        .getModelInput()
                        .getXCat()[0]
                        .length
        );

        assertEquals(
                32,
                sample
                        .getModelInput()
                        .getXNum()
                        .length
        );

        assertEquals(
                2,
                sample
                        .getModelInput()
                        .getXNum()[0]
                        .length
        );


        /*
         * l_attach = 1
         * success  = 1
         * cause "" = 0
         * sub ""   = 0
         */
        assertArrayEquals(
                new long[]{
                        1L,
                        1L,
                        0L,
                        0L
                },
                sample
                        .getModelInput()
                        .getXCat()[0]
        );


        /*
         * Evidence vẫn giữ đúng 32 event
         * đã tạo nên model input.
         */
        assertEquals(
                32,
                sample
                        .getEvidence()
                        .getEvents()
                        .size()
        );


        /*
         * Window hợp lệ không tạo invalid-feature.
         */
        ConcurrentLinkedQueue<
                StreamRecord<InvalidGoldFeatureRecord>
                > sideOutput =
                harness.getSideOutput(
                        GoldFeatureProcessFunction
                                .INVALID_FEATURE_TAG
                );

        assertNull(
                sideOutput
        );
    }


    /**
     * Category không tồn tại trong contract
     * phải được route sang invalid-feature side output.
     */
    @Test
    void shouldRouteUnknownCategoryToSideOutput()
            throws Exception {

        harness =
                ProcessFunctionTestHarnesses
                        .forProcessFunction(
                                createProcessFunction()
                        );

        /*
         * Đặt processing time cố định
         * để failedAt deterministic.
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
         * Invalid window không được đi vào main output.
         */
        assertTrue(
                harness
                        .extractOutputValues()
                        .isEmpty()
        );


        /*
         * Phải có đúng một invalid record.
         */
        ConcurrentLinkedQueue<
                StreamRecord<InvalidGoldFeatureRecord>
                > sideOutput =
                harness.getSideOutput(
                        GoldFeatureProcessFunction
                                .INVALID_FEATURE_TAG
                );

        assertNotNull(
                sideOutput
        );

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
                        .Reason
                        .UNKNOWN_CATEGORY,
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
     * Tạo ProcessFunction bằng chính feature contract
     * đang được application.yaml sử dụng.
     *
     * <p>
     * Constructor production hiện tại là:
     * </p>
     *
     * <pre>
     * GoldFeatureProcessFunction(
     *     invalidSchemaVersion,
     *     featureContract
     * )
     * </pre>
     */
    private static GoldFeatureProcessFunction
    createProcessFunction() {

        GoldFeatureContract contract =
                GoldJobConfig
                        .loadFromClasspath(
                                "application.yaml"
                        )
                        .featureContract();

        return new GoldFeatureProcessFunction(
                "invalid-gold-feature-v1",
                contract
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

        for (
                int index = 0;
                index < 32;
                index++
        ) {

            String eventId =
                    index == 0
                            ? firstEventId
                            : "l_attach";

            GoldSequenceEvent event =
                    new GoldSequenceEvent();


            /*
             * =====================================================
             * IDENTITY
             * =====================================================
             */

            event.setUeKey(
                    "452040000000001"
            );

            event.setImsi(
                    "452040000000001"
            );


            /*
             * =====================================================
             * CATEGORICAL SOURCES
             * =====================================================
             */

            event.setEventId(
                    eventId
            );

            event.setEventResult(
                    "success"
            );

            /*
             * Empty string là category hợp lệ.
             */
            event.setNormalizedCauseCode(
                    ""
            );

            event.setSubCauseCode(
                    ""
            );


            /*
             * =====================================================
             * NUMERIC SOURCES
             * =====================================================
             */

            event.setDurationMs(
                    100L
            );

            event.setRequestRetries(
                    0
            );


            /*
             * =====================================================
             * EVENT TIME
             * =====================================================
             */

            event.setEventTimeEpochMs(
                    startTime
                            .plusSeconds(
                                    index
                            )
                            .toEpochMilli()
            );


            /*
             * =====================================================
             * EVIDENCE / FEATURE SOURCE MAPS
             * =====================================================
             */

            event.setFeatureSourceFields(
                    new LinkedHashMap<>()
            );

            event.setDisplayFields(
                    new LinkedHashMap<>()
            );

            event.setQualityFields(
                    new LinkedHashMap<>()
            );


            /*
             * Deterministic tie-breaker.
             */
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
                startTime.plusSeconds(
                        31
                ),
                32,
                8,
                events
        );
    }
}