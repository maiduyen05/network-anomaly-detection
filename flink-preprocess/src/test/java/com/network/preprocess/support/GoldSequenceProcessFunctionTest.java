package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceWindow;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test cho Gold sequence ordering.
 *
 * <p>
 * Các test cố ý không dựa vào Kafka/source watermark nữa.
 * Gold ordering được quyết định theo timeline của từng ueKey.
 * </p>
 */
class GoldSequenceProcessFunctionTest {

    private static final Instant BASE_TIME =
            Instant.parse("2026-07-08T10:00:00Z");

    private static final long TEST_REORDER_MS =
            30_000L;

    private static final long TEST_IDLE_FLUSH_MS =
            60_000L;

    @Test
    void shouldNotEmitWindowWhenOnlyThirtyOneEventsExist()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            processRange(harness, 1, 31);

            /*
             * UE idle -> flush tail. Vẫn chỉ có 31 event,
             * nên chưa đủ tạo sequence length 32.
             */
            flushIdle(harness);

            assertTrue(mainOutput(harness).isEmpty());

        } finally {
            harness.close();
        }
    }

    @Test
    void shouldEmitOneWindowWhenThirtyTwoEventsExist()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            processRange(harness, 1, 32);
            flushIdle(harness);

            List<GoldSequenceWindow> output =
                    mainOutput(harness);

            assertEquals(1, output.size());

            GoldSequenceWindow window =
                    output.get(0);

            assertEquals(32, window.events().size());
            assertEquals(8, window.stride());
            assertEquals(
                    "raw-record-1",
                    window.events().get(0).sourceOrderKey()
            );
            assertEquals(
                    "raw-record-32",
                    window.events().get(31).sourceOrderKey()
            );

        } finally {
            harness.close();
        }
    }

    @Test
    void shouldEmitSecondWindowAfterEightMoreEvents()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            processRange(harness, 1, 40);
            flushIdle(harness);

            List<GoldSequenceWindow> output =
                    mainOutput(harness);

            assertEquals(2, output.size());

            assertEquals(
                    "raw-record-1",
                    output.get(0).events().get(0).sourceOrderKey()
            );
            assertEquals(
                    "raw-record-32",
                    output.get(0).events().get(31).sourceOrderKey()
            );
            assertEquals(
                    "raw-record-9",
                    output.get(1).events().get(0).sourceOrderKey()
            );
            assertEquals(
                    "raw-record-40",
                    output.get(1).events().get(31).sourceOrderKey()
            );

        } finally {
            harness.close();
        }
    }

    /**
     * Arrival order có thể khác event-time order.
     * Output vẫn phải deterministic theo eventTime + sourceOrderKey.
     */
    @Test
    void shouldOrderEventsByEventTimePerUe()
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

            flushIdle(harness);

            List<GoldSequenceWindow> output =
                    mainOutput(harness);

            assertEquals(1, output.size());

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
     * Một UE có timestamp tiến xa không được làm event của UE khác late.
     * Đây là regression test cho lỗi production đã quan sát:
     * Silver Kafka partition chứa nhiều UE với timeline khác nhau.
     */
    @Test
    void shouldKeepUeTimelinesIndependent()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            GoldSequenceEvent farAheadUeA =
                    eventForUe(
                            "ue-A",
                            1,
                            BASE_TIME.plusSeconds(600)
                    );

            GoldSequenceEvent olderButValidUeB =
                    eventForUe(
                            "ue-B",
                            2,
                            BASE_TIME.plusSeconds(10)
                    );

            processEvent(harness, farAheadUeA);
            processEvent(harness, olderButValidUeB);

            ConcurrentLinkedQueue<
                    StreamRecord<GoldSequenceEvent>> lateOutput =
                    harness.getSideOutput(
                            GoldSequenceProcessFunction
                                    .TOO_LATE_EVENT_TAG
                    );

            /*
             * UE-B không được bị loại chỉ vì UE-A đang ở +10 phút.
             */
            assertTrue(
                    lateOutput == null
                            || lateOutput.isEmpty()
            );

        } finally {
            harness.close();
        }
    }

    /**
     * Event chỉ được xem là late khi chính UE đã finalize qua timestamp đó.
     */
    @Test
    void shouldRouteEventLateForSameUeAfterIdleFinalization()
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> harness =
                createHarness();

        try {
            /*
             * UE hiện tại đã thấy event ở giây 100.
             */
            processEvent(
                    harness,
                    eventForUe(
                            "ue-A",
                            100,
                            BASE_TIME.plusSeconds(100)
                    )
            );

            /*
             * Sau 60 giây processing-time không có event mới,
             * Gold flush toàn bộ tail và finalize UE tới giây 100.
             */
            flushIdle(harness);

            /*
             * Event giây 50 đến sau khi cùng UE đã finalized tới 100.
             * Đây mới là too-late thực sự.
             */
            GoldSequenceEvent lateEvent =
                    eventForUe(
                            "ue-A",
                            50,
                            BASE_TIME.plusSeconds(50)
                    );

            processEvent(
                    harness,
                    lateEvent
            );

            ConcurrentLinkedQueue<
                    StreamRecord<GoldSequenceEvent>> lateOutput =
                    harness.getSideOutput(
                            GoldSequenceProcessFunction
                                    .TOO_LATE_EVENT_TAG
                    );

            assertEquals(1, lateOutput.size());
            assertEquals(
                    lateEvent.sourceOrderKey(),
                    lateOutput.peek()
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
                        "gold-ue-sequence-feature-v2",
                        TEST_REORDER_MS,
                        TEST_IDLE_FLUSH_MS
                );

        KeyedProcessOperator<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> operator =
                new KeyedProcessOperator<>(function);

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
        harness.setProcessingTime(0L);

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

    /**
     * Tất cả event trong test được nhận tại processing-time 0,
     * nên timer idle mới nhất fire tại 60_000 ms.
     */
    private static void flushIdle(
            KeyedOneInputStreamOperatorTestHarness<
                    String,
                    GoldSequenceEvent,
                    GoldSequenceWindow> harness
    ) throws Exception {

        harness.setProcessingTime(
                TEST_IDLE_FLUSH_MS
        );
    }

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
        return eventForUe(
                "452040000000001",
                index,
                BASE_TIME.plusSeconds(index)
        );
    }

    private static GoldSequenceEvent eventForUe(
            String ueKey,
            int index,
            Instant eventTime
    ) {
        return new GoldSequenceEvent(
                ueKey,
                ueKey,
                "l_service_request",
                8,
                "success",
                1,
                "",
                "",
                100L + index,
                index % 3,
                eventTime,
                Map.of(
                        "CAUSE_CODE", "",
                        "SUB_CAUSE_CODE", "",
                        "REQUEST_RETRIES",
                        Integer.toString(index % 3)
                ),
                Map.of(
                        "EVENT_ID", "l_service_request",
                        "EVENT_RESULT", "success"
                ),
                Map.of(
                        "supported", "true"
                ),
                "raw-record-" + index
        );
    }
}
