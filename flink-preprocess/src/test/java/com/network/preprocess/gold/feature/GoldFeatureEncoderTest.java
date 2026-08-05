package com.network.preprocess.gold.feature;

import com.network.preprocess.model.GoldModelInput;
import com.network.preprocess.model.GoldSequenceEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GoldFeatureEncoderTest {

    private static final float DELTA = 0.000001F;

    @Test
    void shouldCreateExactModelTensorShapesAndValues() {
        List<GoldSequenceEvent> sequence =
                new ArrayList<>();

        /*
         * Timestep 0:
         *
         * l_attach → 1
         * reject   → 0
         * "" cause → 1
         * "" sub   → 1
         * duration 0 → 0.0
         * retries 0  → 0.0
         */
        sequence.add(
                event(
                        "l_attach",
                        "reject",
                        "",
                        "",
                        0L,
                        0
                )
        );

        /*
         * Timestep 1:
         *
         * l_service_request → 8
         * success           → 1
         * cause 10          → 2
         * sub cause 107     → 2
         * max duration      → 1.0
         * max retries       → 1.0
         */
        sequence.add(
                event(
                        "l_service_request",
                        "success",
                        "10",
                        "107",
                        600_000L,
                        10
                )
        );

        /*
         * Thêm 30 event hợp lệ để sequence đủ 32.
         */
        while (sequence.size() < 32) {
            sequence.add(
                    event(
                            "l_tau",
                            "success",
                            "9",
                            "413",
                            1_000L,
                            5
                    )
            );
        }

        GoldFeatureEncoder encoder =
                new GoldFeatureEncoder();

        GoldModelInput modelInput =
                encoder.encode(sequence);

        long[][] xCat = modelInput.getXCat();
        float[][] xNum = modelInput.getXNum();

        assertEquals(32, xCat.length);
        assertEquals(4, xCat[0].length);

        assertEquals(32, xNum.length);
        assertEquals(2, xNum[0].length);

        assertArrayEquals(
                new long[]{1L, 0L, 1L, 1L},
                xCat[0]
        );

        assertArrayEquals(
                new long[]{8L, 1L, 2L, 2L},
                xCat[1]
        );

        assertArrayEquals(
                new float[]{0.0F, 0.0F},
                xNum[0],
                DELTA
        );

        assertArrayEquals(
                new float[]{1.0F, 1.0F},
                xNum[1],
                DELTA
        );

        /*
         * Kiểm tra thêm event l_tau:
         *
         * l_tau → 9
         * success → 1
         * cause "9" → 4
         * sub cause "413" → 7
         */
        assertArrayEquals(
                new long[]{9L, 1L, 4L, 7L},
                xCat[2]
        );

        assertEquals(
                0.5F,
                xNum[2][1],
                DELTA
        );
    }

    @Test
    void shouldRejectSequenceThatDoesNotContainExactly32Events() {
        List<GoldSequenceEvent> sequence =
                new ArrayList<>();

        sequence.add(
                event(
                        "l_attach",
                        "success",
                        "",
                        "",
                        100L,
                        0
                )
        );

        GoldFeatureEncoder encoder =
                new GoldFeatureEncoder();

        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () -> encoder.encode(sequence)
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.INVALID_SEQUENCE_LENGTH,
                exception.getReason()
        );
    }

    @Test
    void shouldRejectUnknownCategoryInsideSequence() {
        List<GoldSequenceEvent> sequence =
                new ArrayList<>();

        while (sequence.size() < 32) {
            sequence.add(
                    event(
                            "l_attach",
                            "success",
                            "",
                            "",
                            100L,
                            0
                    )
            );
        }

        /*
         * Thay event đầu tiên bằng category không tồn tại
         * trong vocabulary cố định.
         */
        sequence.get(0).setEventId("l_new_event");

        GoldFeatureEncoder encoder =
                new GoldFeatureEncoder();

        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () -> encoder.encode(sequence)
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason.UNKNOWN_CATEGORY,
                exception.getReason()
        );

        assertEquals(
                "event_code",
                exception.getFeatureName()
        );
    }

    private static GoldSequenceEvent event(
            String eventId,
            String eventResult,
            String normalizedCauseCode,
            String subCauseCode,
            Long durationMs,
            Integer requestRetries
    ) {
        GoldSequenceEvent event =
                new GoldSequenceEvent();

        event.setEventId(eventId);
        event.setEventResult(eventResult);
        event.setNormalizedCauseCode(
                normalizedCauseCode
        );
        event.setSubCauseCode(subCauseCode);
        event.setDurationMs(durationMs);
        event.setRequestRetries(requestRetries);

        return event;
    }
}