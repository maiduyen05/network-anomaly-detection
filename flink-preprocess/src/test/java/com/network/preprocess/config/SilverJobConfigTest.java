package com.network.preprocess.config;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SilverJobConfigTest {

    @Test
    void shouldLoadSilverConfigurationFromApplicationYaml() {
        SilverJobConfig config =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        assertEquals(
                "flink-silver-v1",
                config.jobName()
        );

        assertEquals(
                "bronze.ue.event",
                config.inputTopic()
        );

        assertEquals(
                "silver.ue.event",
                config.outputTopic()
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

        assertTrue(
                config.eventDefinitionsByAlias()
                        .containsKey("l_service_request")
        );

        assertTrue(
                config.eventDefinitionsByAlias()
                        .containsKey("l_pdn_disconnect")
        );
    }

    @Test
    void shouldUseDifferentTransactionalPrefixForEverySink() {
        SilverJobConfig config =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        Set<String> prefixes =
                Set.of(
                        config.outputTransactionalIdPrefix(),
                        config.invalidIdentityTransactionalIdPrefix(),
                        config.unsupportedEventTransactionalIdPrefix(),
                        config.lateEventTransactionalIdPrefix()
                );

        /*
         * Set chỉ còn bốn phần tử nếu cả bốn prefix khác nhau.
         */
        assertEquals(
                4,
                prefixes.size()
        );
    }
}