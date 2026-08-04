package com.network.preprocess.silver;

import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.silver.dedup.SilverDedupKeySelector;
import com.network.preprocess.silver.dedup.SilverDeduplicateProcessFunction;
import com.network.preprocess.testsupport.SilverEventFixtures;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SilverDeduplicateProcessFunctionTest {

    @Test
    void shouldEmitDuplicateSourceOffsetOnlyOnce()
            throws Exception {

        try (KeyedOneInputStreamOperatorTestHarness<
                String,
                SilverEvent,
                SilverEvent> harness = createHarness()) {

            SilverEvent original =
                    SilverEventFixtures.event(
                            100L,
                            "2026-07-08T10:15:30Z"
                    );

            SilverEvent replayed =
                    SilverEventFixtures.event(
                            100L,
                            "2026-07-08T10:15:30Z"
                    );

            harness.processElement(
                    new StreamRecord<>(original)
            );

            harness.processElement(
                    new StreamRecord<>(replayed)
            );

            List<SilverEvent> outputs =
                    extractOutputValues(harness);

            /*
             * Record đầu tiên được output.
             * Record replay cùng source offset bị loại.
             */
            assertEquals(1, outputs.size());
            assertEquals(original, outputs.get(0));
        }
    }

    @Test
    void shouldKeepDifferentOffsetsOfSameUe()
            throws Exception {

        try (KeyedOneInputStreamOperatorTestHarness<
                String,
                SilverEvent,
                SilverEvent> harness = createHarness()) {

            /*
             * Hai event có cùng IMSI/ueKey nhưng offset khác nhau.
             * Đây là hai event hợp lệ, không phải duplicate.
             */
            SilverEvent first =
                    SilverEventFixtures.event(
                            100L,
                            "2026-07-08T10:15:30Z"
                    );

            SilverEvent second =
                    SilverEventFixtures.event(
                            101L,
                            "2026-07-08T10:15:31Z"
                    );

            harness.processElement(
                    new StreamRecord<>(first)
            );

            harness.processElement(
                    new StreamRecord<>(second)
            );

            List<SilverEvent> outputs =
                    extractOutputValues(harness);

            assertEquals(2, outputs.size());
        }
    }

    private KeyedOneInputStreamOperatorTestHarness<
            String,
            SilverEvent,
            SilverEvent> createHarness() throws Exception {

        SilverDeduplicateProcessFunction function =
                new SilverDeduplicateProcessFunction(
                        86_400_000L
                );

        KeyedProcessOperator<
                String,
                SilverEvent,
                SilverEvent> operator =
                new KeyedProcessOperator<>(function);

        KeyedOneInputStreamOperatorTestHarness<
                String,
                SilverEvent,
                SilverEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        new SilverDedupKeySelector(),
                        Types.STRING
                );

        harness.open();

        return harness;
    }

    /**
     * Chỉ lấy StreamRecord khỏi output queue.
     *
     * <p>Output queue của Flink test harness còn có thể chứa
     * Watermark hoặc các StreamElement khác.</p>
     */
    @SuppressWarnings("unchecked")
    private List<SilverEvent> extractOutputValues(
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