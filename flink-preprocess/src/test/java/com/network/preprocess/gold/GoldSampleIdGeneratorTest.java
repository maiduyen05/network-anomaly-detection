package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class GoldSampleIdGeneratorTest {

    @Test
    void shouldCreateSameIdForSameSequence() {
        List<GoldSequenceEvent> events =
                createEvents(1, 32);

        String firstId =
                GoldSampleIdGenerator.generate(
                        "452040000000001",
                        events.get(0).eventTime(),
                        events.get(31).eventTime(),
                        events
                );

        String secondId =
                GoldSampleIdGenerator.generate(
                        "452040000000001",
                        events.get(0).eventTime(),
                        events.get(31).eventTime(),
                        events
                );

        /*
         * Cùng sequence phải tạo cùng ID.
         */
        assertEquals(
                firstId,
                secondId
        );

        /*
         * SHA-256 dạng hexadecimal có đúng 64 ký tự.
         */
        assertEquals(
                64,
                firstId.length()
        );
    }

    @Test
    void shouldCreateDifferentIdForDifferentSequence() {
        List<GoldSequenceEvent> firstSequence =
                createEvents(1, 32);

        List<GoldSequenceEvent> secondSequence =
                createEvents(9, 40);

        String firstId =
                GoldSampleIdGenerator.generate(
                        "452040000000001",
                        firstSequence.get(0).eventTime(),
                        firstSequence.get(31).eventTime(),
                        firstSequence
                );

        String secondId =
                GoldSampleIdGenerator.generate(
                        "452040000000001",
                        secondSequence.get(0).eventTime(),
                        secondSequence.get(31).eventTime(),
                        secondSequence
                );

        assertNotEquals(
                firstId,
                secondId
        );
    }

    private static List<GoldSequenceEvent> createEvents(
            int firstIndex,
            int lastIndex
    ) {
        List<GoldSequenceEvent> events =
                new ArrayList<>();

        for (int index = firstIndex;
             index <= lastIndex;
             index++) {

            events.add(
                    event(index)
            );
        }

        return events;
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
                Instant.parse("2026-07-08T10:00:00Z")
                        .plusSeconds(index),
                Map.of(
                        "REQUEST_RETRIES",
                        Integer.toString(index % 3)
                ),
                Map.of(
                        "IMSI",
                        "452040000000001",
                        "TAC",
                        "1001"
                ),
                Map.of(
                        "supported",
                        "true"
                ),
                "raw-record-" + index
        );
    }
}