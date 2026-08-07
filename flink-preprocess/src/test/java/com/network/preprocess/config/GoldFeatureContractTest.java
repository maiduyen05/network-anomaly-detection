package com.network.preprocess.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.network.preprocess.config.GoldJobConfig;

class GoldFeatureContractTest {

    /**
     * Contract v1 hợp lệ phải load đúng toàn bộ shape model.
     */
    @Test
    void shouldLoadCurrentGoldFeatureContract()
            throws Exception {

        String yaml = """
                feature-contract:
                  feature-version: gold-ue-sequence-feature-v1

                  sequence:
                    length: 32
                    stride: 8
                    padding-side: LEFT
                    emit-partial-windows: false

                  categorical:
                    dtype: INT64
                    feature-count: 4
                    unknown-policy: REJECT
                    missing-policy: REJECT

                    features:
                      - index: 0
                        name: event_code
                        source: eventId
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          l_attach: 1
                          l_service_request: 8

                      - index: 1
                        name: event_result_code
                        source: eventResult
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          reject: 0
                          success: 1

                      - index: 2
                        name: normalized_cause_code
                        source: rawFields.CAUSE_CODE
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          "": 0
                          "10": 1

                      - index: 3
                        name: sub_cause_code
                        source: rawFields.SUB_CAUSE_CODE
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          "": 0
                          "11": 2

                  numeric:
                    dtype: FLOAT32
                    feature-count: 2
                    missing-value: -1.0

                    normalized-valid-range:
                      min: 0.0
                      max: 1.0

                    features:
                      - index: 0
                        name: duration_ms
                        source: durationMs
                        raw-clip-min: 0
                        raw-clip-max: 600000
                        transform: log1p_minmax

                      - index: 1
                        name: request_retries
                        source: requestRetries
                        raw-clip-min: 0
                        raw-clip-max: 10
                        transform: clipped_minmax
                """;

        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory()
                );

        JsonNode root =
                mapper.readTree(
                        yaml
                );

        GoldFeatureContract contract =
                GoldFeatureContract.fromRoot(
                        root
                );

        assertEquals(
                "gold-ue-sequence-feature-v1",
                contract.featureVersion()
        );

        assertEquals(
                32,
                contract.sequenceLength()
        );

        assertEquals(
                8,
                contract.sequenceStride()
        );

        assertFalse(
                contract.emitPartialWindows()
        );

        assertEquals(
                4,
                contract.categoricalFeatureCount()
        );

        assertEquals(
                2,
                contract.numericFeatureCount()
        );

        assertEquals(
                -1.0F,
                contract.numericMissingValue()
        );

        /*
         * Empty string PHẢI còn tồn tại.
         *
         * Đây là category hợp lệ của cause code,
         * không phải missing value.
         */
        assertEquals(
                0,
                contract
                        .categoricalFeatures()
                        .get(2)
                        .vocabulary()
                        .get("")
        );
    }


    /**
     * Model hiện tại chưa hỗ trợ partial window.
     */
    @Test
    void shouldRejectPartialWindows()
            throws Exception {

        String yaml = minimalContract(
                32,
                8,
                true
        );

        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory()
                );

        JsonNode root =
                mapper.readTree(
                        yaml
                );

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        GoldFeatureContract.fromRoot(
                                root
                        )
        );
    }


    /**
     * Model v1 chỉ nhận sequence 32.
     */
    @Test
    void shouldRejectDifferentSequenceLength()
            throws Exception {

        String yaml = minimalContract(
                64,
                8,
                false
        );

        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory()
                );

        JsonNode root =
                mapper.readTree(
                        yaml
                );

        assertThrows(
                IllegalStateException.class,
                () ->
                        GoldFeatureContract.fromRoot(
                                root
                        )
        );
    }


    /**
     * YAML tối thiểu phục vụ các negative test.
     */
    private static String minimalContract(
            int sequenceLength,
            int stride,
            boolean partial
    ) {

        return """
                feature-contract:
                  feature-version: gold-ue-sequence-feature-v1

                  sequence:
                    length: %d
                    stride: %d
                    padding-side: LEFT
                    emit-partial-windows: %s

                  categorical:
                    dtype: INT64
                    feature-count: 4
                    unknown-policy: REJECT
                    missing-policy: REJECT

                    features:
                      - index: 0
                        name: event_code
                        source: eventId
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          l_attach: 1

                      - index: 1
                        name: event_result_code
                        source: eventResult
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          success: 1

                      - index: 2
                        name: normalized_cause_code
                        source: rawFields.CAUSE_CODE
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          "": 0

                      - index: 3
                        name: sub_cause_code
                        source: rawFields.SUB_CAUSE_CODE
                        transform: fixed_vocabulary_lookup
                        vocabulary:
                          "": 0

                  numeric:
                    dtype: FLOAT32
                    feature-count: 2
                    missing-value: -1.0

                    normalized-valid-range:
                      min: 0.0
                      max: 1.0

                    features:
                      - index: 0
                        name: duration_ms
                        source: durationMs
                        raw-clip-min: 0
                        raw-clip-max: 600000
                        transform: log1p_minmax

                      - index: 1
                        name: request_retries
                        source: requestRetries
                        raw-clip-min: 0
                        raw-clip-max: 10
                        transform: clipped_minmax
                """
                .formatted(
                        sequenceLength,
                        stride,
                        partial
                );
    }
    }