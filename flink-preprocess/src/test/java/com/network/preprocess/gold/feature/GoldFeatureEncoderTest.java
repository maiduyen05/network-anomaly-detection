package com.network.preprocess.gold.feature;

import com.network.preprocess.config.GoldJobConfig;
import com.network.preprocess.model.GoldModelInput;
import com.network.preprocess.model.GoldSequenceEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Kiểm thử GoldFeatureEncoder.
 * Phần enconder được lấy từ GoldFeatureContract trong application.yaml.
 */
class GoldFeatureEncoderTest {

    private static final float DELTA =
            0.000001F;

    /**
     * Kiểm tra encoder tạo đúng:
     *
     * <pre>
     * x_cat[32][4]
     * x_num[32][2]
     * </pre>
     *
     * và giữ nguyên mapping của model v1.
     */
    @Test
    void shouldCreateExactModelTensorShapesAndValues() {

        List<GoldSequenceEvent> sequence =
                new ArrayList<>();

        /*
         * Timestep 0:
         *
         * l_attach -> 1
         * reject   -> 0
         * "" cause -> 0
         * "" sub   -> 0
         *
         * duration 0 -> 0.0
         * retries 0  -> 0.0
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
         * l_service_request -> 8
         * success           -> 1
         * cause 10          -> 1
         * sub cause 107     -> 1
         *
         * duration 600000 -> 1.0
         * retries 10      -> 1.0
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
         * Thêm 30 event hợp lệ
         * để sequence có đúng 32 event.
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

        /*
         * Encoder phải được tạo bằng feature contract thật.
         */
        GoldFeatureEncoder encoder =
                encoder();

        GoldModelInput modelInput =
                encoder.encode(
                        sequence
                );

        long[][] xCat =
                modelInput.getXCat();

        float[][] xNum =
                modelInput.getXNum();


        /*
         * =========================================================
         * SHAPE
         * =========================================================
         */

        assertEquals(
                32,
                xCat.length
        );

        assertEquals(
                4,
                xCat[0].length
        );

        assertEquals(
                32,
                xNum.length
        );

        assertEquals(
                2,
                xNum[0].length
        );


        /*
         * =========================================================
         * TIMESTEP 0
         * =========================================================
         */

        assertArrayEquals(
                new long[]{
                        1L,
                        0L,
                        0L,
                        0L
                },
                xCat[0]
        );

        assertArrayEquals(
                new float[]{
                        0.0F,
                        0.0F
                },
                xNum[0],
                DELTA
        );


        /*
         * =========================================================
         * TIMESTEP 1
         * =========================================================
         */

        assertArrayEquals(
                new long[]{
                        8L,
                        1L,
                        1L,
                        1L
                },
                xCat[1]
        );

        assertArrayEquals(
                new float[]{
                        1.0F,
                        1.0F
                },
                xNum[1],
                DELTA
        );


        /*
         * =========================================================
         * TIMESTEP 2
         * =========================================================
         *
         * l_tau  -> 9
         * success -> 1
         * cause 9 -> 3
         * sub 413 -> 6
         *
         * retries 5 / 10 -> 0.5
         */

        assertArrayEquals(
                new long[]{
                        9L,
                        1L,
                        3L,
                        6L
                },
                xCat[2]
        );

        assertEquals(
                0.5F,
                xNum[2][1],
                DELTA
        );
    }


    /**
     * Contract hiện tại yêu cầu đúng 32 event.
     */
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
                encoder();

        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                encoder.encode(
                                        sequence
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .INVALID_SEQUENCE_LENGTH,
                exception.getReason()
        );
    }


    /**
     * Category ngoài vocabulary phải bị reject.
     *
     * <p>
     * Encoder không được tự tạo ID mới trong runtime.
     * </p>
     */
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
         * EVENT_ID này không tồn tại
         * trong feature contract v1.
         */
        sequence
                .get(0)
                .setEventId(
                        "l_new_event"
                );

        GoldFeatureEncoder encoder =
                encoder();

        GoldFeatureEncodingException exception =
                assertThrows(
                        GoldFeatureEncodingException.class,
                        () ->
                                encoder.encode(
                                        sequence
                                )
                );

        assertEquals(
                GoldFeatureEncodingException
                        .Reason
                        .UNKNOWN_CATEGORY,
                exception.getReason()
        );

        assertEquals(
                "event_code",
                exception.getFeatureName()
        );
    }


    /**
     * Tạo event tối thiểu phục vụ feature encoder test.
     */
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

        event.setEventId(
                eventId
        );

        event.setEventResult(
                eventResult
        );

        event.setNormalizedCauseCode(
                normalizedCauseCode
        );

        event.setSubCauseCode(
                subCauseCode
        );

        event.setDurationMs(
                durationMs
        );

        event.setRequestRetries(
                requestRetries
        );

        return event;
    }


    /**
     * Tạo encoder từ chính feature contract
     * đang được Gold Job sử dụng.
     *
     * <p>
     * Test không tạo một vocabulary riêng,
     * tránh tình trạng:
     * </p>
     *
     * <pre>
     * test contract != runtime contract
     * </pre>
     */
    private static GoldFeatureEncoder encoder() {

        GoldJobConfig config =
                GoldJobConfig
                        .loadFromClasspath(
                                "application.yaml"
                        );

        return new GoldFeatureEncoder(
                config.featureContract()
        );
    }
}