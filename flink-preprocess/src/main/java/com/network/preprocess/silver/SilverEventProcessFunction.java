package com.network.preprocess.silver;

import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.UnsupportedEventRecord;
import com.network.preprocess.silver.event.SilverEventTransformer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * Flink operator tạo SilverEvent.
 *
 * <p>Output:</p>
 *
 * <ul>
 *     <li>Main output: SilverEvent hợp lệ.</li>
 *     <li>Side output: UnsupportedEventRecord.</li>
 * </ul>
 *
 * <p>Operator chưa sử dụng keyed state, timer hoặc watermark.</p>
 */
public final class SilverEventProcessFunction
        extends ProcessFunction<
                IdentityResolvedEvent,
                SilverEvent
        > {

    /**
     * Anonymous subclass giữ type information của
     * UnsupportedEventRecord tại runtime.
     */
    public static final OutputTag<UnsupportedEventRecord>
            UNSUPPORTED_EVENT_TAG =
            new OutputTag<UnsupportedEventRecord>(
                    "silver-unsupported-event"
            ) {
            };

    private final SilverEventTransformer transformer;

    public SilverEventProcessFunction(
            SilverEventTransformer transformer
    ) {
        this.transformer = Objects.requireNonNull(
                transformer,
                "transformer must not be null"
        );
    }

    @Override
    public void processElement(
            IdentityResolvedEvent value,
            Context context,
            Collector<SilverEvent> out
    ) {
        /*
         * Timestamp của stream chưa được gán ở Checkpoint 8,
         * vì vậy failedAt dùng processing time.
         */
        long processingTimeMillis =
                context.timerService()
                        .currentProcessingTime();

        SilverTransformationResult result =
                transformer.transform(
                        value,
                        processingTimeMillis
                );

        if (result.isSupported()) {
            out.collect(
                    result.getSilverEvent()
            );
            return;
        }

        context.output(
                UNSUPPORTED_EVENT_TAG,
                result.getUnsupportedEvent()
        );
    }
}