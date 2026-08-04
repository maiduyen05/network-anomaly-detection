package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceWindow;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators
        .KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord
        .StreamRecord;
import org.apache.flink.streaming.util
        .KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoldSequenceProcessFunctionTest {

    private static final Instant BASE_TIME =
            Instant.parse("2026-07-08T10:00:00Z");

    /**
     * Chưa đủ 32 event thì không được phát sample.
     */
    @Test
    void shouldNotEmitWindowWhenOnlyThirtyOneEventsExist()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            processRange(
                    harness,
                    1,
                    31
            );

            advanceWatermark(
                    harness,
                    31
            );

            assertTrue(
                    mainOutput(harness).isEmpty()
            );

        } finally {
            harness.close();
        }
    }

    /**
     * Đúng 32 event thì phát đúng một window.
     */
    @Test
    void shouldEmitOneWindowWhenThirtyTwoEventsExist()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            processRange(
                    harness,
                    1,
                    32
            );

            advanceWatermark(
                    harness,
                    32
            );

            List<GoldSequenceWindow> output =
                    mainOutput(harness);

            assertEquals(
                    1,
                    output.size()
            );

            GoldSequenceWindow window =
                    output.get(0);

            assertEquals(
                    32,
                    window.events().size()
            );

            assertEquals(
                    8,
                    window.stride()
            );

            assertEquals(
                    "raw-record-1",
                    window.events()
                            .get(0)
                            .sourceOrderKey()
            );

            assertEquals(
                    "raw-record-32",
                    window.events()
                            .get(31)
                            .sourceOrderKey()
            );

        } finally {
            harness.close();
        }
    }

    /**
     * Với 40 event và stride 8 phải phát:
     *
     * <pre>
     * window 1 = event 1..32
     * window 2 = event 9..40
     * </pre>
     */
    @Test
    void shouldEmitSecondWindowAfterEightMoreEvents()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            processRange(
                    harness,
                    1,
                    40
            );

            advanceWatermark(
                    harness,
                    40
            );

            List<GoldSequenceWindow> output =
                    mainOutput(harness);

            assertEquals(
                    2,
                    output.size()
            );

            GoldSequenceWindow firstWindow =
                    output.get(0);

            GoldSequenceWindow secondWindow =
                    output.get(1);

            assertEquals(
                    "raw-record-1",
                    firstWindow.events()
                            .get(0)
                            .sourceOrderKey()
            );

            assertEquals(
                    "raw-record-32",
                    firstWindow.events()
                            .get(31)
                            .sourceOrderKey()
            );

            assertEquals(
                    "raw-record-9",
                    secondWindow.events()
                            .get(0)
                            .sourceOrderKey()
            );

            assertEquals(
                    "raw-record-40",
                    secondWindow.events()
                            .get(31)
                            .sourceOrderKey()
            );

        } finally {
            harness.close();
        }
    }

    /**
     * Arrival order có thể khác event-time order.
     *
     * <p>Test gửi event theo thứ tự ngược, nhưng output vẫn phải
     * được sắp từ event 1 đến event 32.</p>
     */
    @Test
    void shouldOrderEventsByEventTime()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            for (int index = 32;
                 index >= 1;
                 index--) {

                processEvent(
                        harness,
                        event(index)
                );
            }

            advanceWatermark(
                    harness,
                    32
            );

            List<GoldSequenceWindow> output =
                    mainOutput(harness);

            assertEquals(
                    1,
                    output.size()
            );

            List<GoldSequenceEvent> events =
                    output.get(0).events();

            assertEquals(
                    "raw-record-1",
                    events.get(0).sourceOrderKey()
            );

            assertEquals(
                    "raw-record-32",
                    events.get(31).sourceOrderKey()
            );

            for (int index = 1;
                 index < events.size();
                 index++) {

                assertTrue(
                        !events.get(index)
                                .eventTime()
                                .isBefore(
                                        events.get(index - 1)
                                                .eventTime()
                                )
                );
            }

        } finally {
            harness.close();
        }
    }

    /**
     * Event có eventTime nhỏ hơn watermark phải đi side output.
     */
    @Test
    void shouldRouteTooLateEventToSideOutput()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            /*
             * Đưa watermark tới giây thứ 10.
             */
            advanceWatermark(
                    harness,
                    10
            );

            /*
             * Event ở giây thứ 5 đến sau watermark nên đã quá trễ.
             */
            processEvent(
                    harness,
                    event(5)
            );

            ConcurrentLinkedQueue<
                    StreamRecord<GoldSequenceEvent>>
                    lateOutput =
                    harness.getSideOutput(
                            GoldSequenceProcessFunction
                                    .TOO_LATE_EVENT_TAG
                    );

            assertEquals(
                    1,
                    lateOutput.size()
            );

            assertEquals(
                    "raw-record-5",
                    lateOutput
                            .peek()
                            .getValue()
                            .sourceOrderKey()
            );

        } finally {
            harness.close();
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<
            String,
            GoldSequenceEvent,
            GoldSequenceWindow> createHarness()
            throws Exception {

        GoldSequenceProcessFunction function =
                new GoldSequenceProcessFunction(
                        32,
                        8,
                        86_400_000L,
                        "gold-sequence-v1",
                        "ue-seq-v1"
                );

        KeyedProcessOperator<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> operator =
                new KeyedProcessOperator<>(
                        function
                );

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        GoldSequenceEvent::ueKey,
                        Types.STRING
                );

        harness.open();

        return harness;
    }

    private static void processRange(
            KeyedOneInputStreamOperatorTestHarness<
                    String,
                    GoldSequenceEvent,
                    GoldSequenceWindow> harness,
            int firstIndex,
            int lastIndex
    ) throws Exception {

        for (int index = firstIndex;
             index <= lastIndex;
             index++) {

            processEvent(
                    harness,
                    event(index)
            );
        }
    }

    private static void processEvent(
            KeyedOneInputStreamOperatorTestHarness<
                    String,
                    GoldSequenceEvent,
                    GoldSequenceWindow> harness,
            GoldSequenceEvent event
    ) throws Exception {

        harness.processElement(
                new StreamRecord<>(
                        event,
                        event.eventTime().toEpochMilli()
                )
        );
    }

    private static void advanceWatermark(
            KeyedOneInputStreamOperatorTestHarness<
                    String,
                    GoldSequenceEvent,
                    GoldSequenceWindow> harness,
            int second
    ) throws Exception {

        harness.processWatermark(
                new Watermark(
                        BASE_TIME
                                .plusSeconds(second)
                                .toEpochMilli()
                )
        );
    }

    /**
     * Chỉ lấy StreamRecord chứa GoldSequenceWindow.
     *
     * <p>Queue output còn có thể chứa Watermark, vì vậy không được
     * cast trực tiếp tất cả phần tử.</p>
     */
    private static List<GoldSequenceWindow> mainOutput(
            KeyedOneInputStreamOperatorTestHarness<
                    String,
                    GoldSequenceEvent,
                    GoldSequenceWindow> harness
    ) {
        List<GoldSequenceWindow> result =
                new ArrayList<>();

        for (Object outputItem
                : harness.getOutput()) {

            if (outputItem
                    instanceof StreamRecord<?> record
                    && record.getValue()
                    instanceof GoldSequenceWindow window) {

                result.add(window);
            }
        }

        return result;
    }

    private static GoldSequenceEvent event(
            int index
    ) {
        return new GoldSequenceEvent(
                "452040000000001",
                "452040000000001",
                "l_service_request",
                3,
                "success",
                1,
                100L + index,
                BASE_TIME.plusSeconds(index),
                Map.of(
                        "REQUEST_RETRIES",
                        Integer.toString(index % 3),
                        "PAGING_ATTEMPTS",
                        Integer.toString(index % 2)
                ),
                Map.of(
                        "EVENT_ID",
                        "l_service_request",
                        "EVENT_RESULT",
                        "success",
                        "IMSI",
                        "452040000000001",
                        "TAC",
                        "1001",
                        "ECI",
                        "20001"
                ),
                Map.of(
                        "supported",
                        "true",
                        "event_time_quality",
                        "source"
                ),
                "raw-record-" + index
        );
    }
}