package com.network.preprocess.config;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GoldJobConfigTest {

    @Test
    void shouldLoadGoldConfigurationFromApplicationYaml() {
        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        assertEquals(
                "flink-gold-v1",
                config.jobName()
        );

        assertEquals(
                "silver.ue.event",
                config.inputTopic()
        );

        assertEquals(
                "gold.ue.sequence",
                config.outputTopic()
        );

        assertEquals(
                "gold-too-late-event",
                config.tooLateEventTopic()
        );

        assertEquals(
                "invalid-gold-feature",
                config.invalidFeatureTopic()
        );

        assertEquals(
                "gold-sequence-v1",
                config.outputSchemaVersion()
        );

        assertEquals(
                "invalid-gold-feature-v1",
                config.invalidFeatureSchemaVersion()
        );

        assertEquals(
                "gold-ue-sequence-feature-v1",
                config.featureVersion()
        );

        assertEquals(
                32,
                config.sequenceLength()
        );

        assertEquals(
                8,
                config.sequenceStride()
        );

        assertEquals(
                30_000L,
                config.watermarkMaxOutOfOrdernessMs()
        );

        assertEquals(
                60_000L,
                config.watermarkIdlenessMs()
        );

        assertEquals(
                86_400_000L,
                config.stateTtlMs()
        );
    }

    @Test
    void shouldUseDifferentTransactionalPrefixForEveryGoldSink() {
        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        Set<String> prefixes =
                Set.of(
                        config.outputTransactionalIdPrefix(),
                        config.tooLateEventTransactionalIdPrefix(),
                        config.invalidFeatureTransactionalIdPrefix()
                );

        /*
         * Set có đúng ba phần tử nghĩa là ba prefix khác nhau.
         */
        assertEquals(
                3,
                prefixes.size()
        );
    }
}