package com.network.preprocess.gold;

import com.network.preprocess.gold.feature.GoldFeatureEncoder;
import com.network.preprocess.model.GoldEvidence;
import com.network.preprocess.model.GoldModelInput;
import com.network.preprocess.model.GoldSequenceSample;
import com.network.preprocess.model.GoldSequenceWindow;
import com.network.preprocess.config.GoldFeatureContract;

import java.io.Serializable;
import java.util.Objects;

/**
 * Chuyển GoldSequenceWindow thành GoldSequenceSample model-ready.
 *
 * <p>Window chịu trách nhiệm:</p>
 *
 * <ul>
 *     <li>Giữ đúng 32 event.</li>
 *     <li>Giữ thứ tự event time.</li>
 *     <li>Giữ metadata và sample ID.</li>
 * </ul>
 *
 * <p>Factory chịu trách nhiệm:</p>
 *
 * <ul>
 *     <li>Encode 32 event thành tensor.</li>
 *     <li>Đóng gói model_input.</li>
 *     <li>Đóng gói evidence.</li>
 * </ul>
 */
public final class GoldSequenceSampleFactory
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final GoldFeatureEncoder featureEncoder;

        /**
         * Factory phải nhận cùng feature contract
         * mà Gold Job đang sử dụng.
         */
        public GoldSequenceSampleFactory(
                GoldFeatureContract featureContract
        ) {

        this.featureEncoder =
                new GoldFeatureEncoder(
                        Objects.requireNonNull(
                                featureContract,
                                "featureContract must not be null"
                        )
                );
        }

    public GoldSequenceSample create(
            GoldSequenceWindow window
    ) {
        Objects.requireNonNull(
                window,
                "window must not be null"
        );

        /*
         * Có thể phát sinh GoldFeatureEncodingException nếu:
         *
         * - category bị thiếu;
         * - category không thuộc vocabulary;
         * - sequence không đúng 32 event.
         *
         * Factory không bắt lỗi tại đây.
         * ProcessFunction phía sau sẽ route lỗi sang side output.
         */
        GoldModelInput modelInput =
                featureEncoder.encode(
                        window.events()
                );

        /*
         * Evidence giữ nguyên 32 event đã được dùng để tạo tensor.
         */
        GoldEvidence evidence =
                new GoldEvidence(
                        window.events()
                );

        return new GoldSequenceSample(
                window.schemaVersion(),
                window.featureVersion(),
                window.sampleId(),
                window.ueKey(),
                window.imsi(),

                /*
                 * Instant chỉ được chuyển thành String tại biên output.
                 */
                window.windowStartEventTime().toString(),
                window.windowEndEventTime().toString(),

                window.sequenceLength(),
                window.stride(),

                modelInput,
                evidence
        );
    }
}