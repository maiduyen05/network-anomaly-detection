package com.network.preprocess.silver.time;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.testsupport.BronzeEventFixtures;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BronzeWatermarkStrategyFactoryTest {

    @Test
    void shouldExtractEventTimeFromBronzeEvent() {

        BronzeEvent event =
                BronzeEventFixtures.eventWithIdentity(
                        "452010123456789",
                        null,
                        null,
                        null
                );

        long actual =
                BronzeWatermarkStrategyFactory
                        .extractEventTimeMillis(
                                event
                        );

        long expected =
                Instant
                        .parse(
                                "2026-08-03T08:00:00Z"
                        )
                        .toEpochMilli();

        assertEquals(
                expected,
                actual
        );
    }
}