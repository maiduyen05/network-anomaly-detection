package com.network.preprocess.silver;

import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.silver.time.SilverEventTimestampExtractor;
import com.network.preprocess.testsupport.SilverEventFixtures;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SilverEventTimestampExtractorTest {

    private final SilverEventTimestampExtractor extractor =
            new SilverEventTimestampExtractor();

    @Test
    void shouldExtractUtcEventTimeAsEpochMillis() {
        String eventTime =
                "2026-07-08T10:15:30.123Z";

        SilverEvent event =
                SilverEventFixtures.event(
                        100L,
                        eventTime
                );

        long actual =
                extractor.extractTimestamp(event);

        long expected =
                Instant.parse(eventTime).toEpochMilli();

        assertEquals(expected, actual);
    }

    @Test
    void shouldRejectInvalidEventTimeContract() {
        SilverEvent event =
                SilverEventFixtures.event(
                        100L,
                        "not-a-timestamp"
                );

        assertThrows(
                IllegalArgumentException.class,
                () -> extractor.extractTimestamp(event)
        );
    }
}