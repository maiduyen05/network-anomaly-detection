package com.network.preprocess.silver;

import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.silver.time.SilverLateEventProcessFunction;
import com.network.preprocess.testsupport.SilverEventFixtures;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SilverLateEventProcessFunctionTest {

    @Test
    void shouldKeepEventInsideThresholdAndRouteTooLateEvent()
            throws Exception {

        try (KeyedOneInputStreamOperatorTestHarness<
                String,
                SilverEvent,
                SilverEvent> harness = createHarness()) {

            /*
             * Giả sử timestamp lớn nhất từng thấy là 100000 ms
             * và out-of-orderness là 30000 ms.
             *
             * Watermark tương ứng:
             *
             * 100000 - 30000 - 1 = 69999
             */
            harness.processWatermark(
                    new Watermark(69_999L)
            );

            SilverEvent insideThreshold =
                    SilverEventFixtures.event(
                            100L,
                            "1970-01-01T00:01:10Z"
                    );

            /*
             * Timestamp 70000 > watermark 69999.
             * Event vẫn nằm trong giới hạn được chấp nhận.
             */
            harness.processElement(
                    new StreamRecord<>(
                            insideThreshold,
                            70_000L
                    )
            );

            SilverEvent tooLate =
                    SilverEventFixtures.event(
                            101L,
                            "1970-01-01T00:01:09.999Z"
                    );

            /*
             * Timestamp 69999 <= watermark 69999.
             * Event phải đi vào late-event side output.
             */
            harness.processElement(
                    new StreamRecord<>(
                            tooLate,
                            69_999L
                    )
            );

            List<SilverEvent> mainOutput =
                    extractMainOutput(harness);

            ConcurrentLinkedQueue<
                    StreamRecord<SilverEvent>> lateOutput =
                    harness.getSideOutput(
                            SilverLateEventProcessFunction
                                    .LATE_EVENT_TAG
                    );

            assertEquals(1, mainOutput.size());
            assertEquals(
                    insideThreshold,
                    mainOutput.get(0)
            );

            assertEquals(1, lateOutput.size());
            assertEquals(
                    tooLate,
                    lateOutput.peek().getValue()
            );
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<
            String,
            SilverEvent,
            SilverEvent> createHarness() throws Exception {

        SilverLateEventProcessFunction function =
                new SilverLateEventProcessFunction();

        KeyedProcessOperator<
                String,
                SilverEvent,
                SilverEvent> operator =
                new KeyedProcessOperator<>(function);

        KeySelector<SilverEvent, String> ueKeySelector =
                SilverEvent::ueKey;

        KeyedOneInputStreamOperatorTestHarness<
                String,
                SilverEvent,
                SilverEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        ueKeySelector,
                        Types.STRING
                );

        harness.open();

        return harness;
    }

    @SuppressWarnings("unchecked")
    private List<SilverEvent> extractMainOutput(
            KeyedOneInputStreamOperatorTestHarness<
                    String,
                    SilverEvent,
                    SilverEvent> harness
    ) {
        return harness
                .getOutput()
                .stream()
                .filter(StreamRecord.class::isInstance)
                .map(element ->
                        ((StreamRecord<SilverEvent>) element)
                                .getValue()
                )
                .toList();
    }
}