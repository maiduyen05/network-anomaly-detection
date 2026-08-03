package com.network.preprocess.silver;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.IdentityResolvedEvent;
import com.network.preprocess.model.InvalidIdentityRecord;
import com.network.preprocess.silver.identity.UeIdentityResolver;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * Flink operator thực hiện bước resolve identity của Silver.
 *
 * <p>Output:</p>
 *
 * <ul>
 *     <li>Main output: IdentityResolvedEvent.</li>
 *     <li>Side output: InvalidIdentityRecord.</li>
 * </ul>
 *
 * <p>Operator này chưa dùng keyed state, timer, watermark hoặc
 * deduplication.</p>
 */
public final class SilverIdentityProcessFunction
        extends ProcessFunction<
                BronzeEvent,
                IdentityResolvedEvent
        > {

    /**
     * Anonymous subclass giữ generic type information tại runtime.
     *
     * <p>Không viết:</p>
     *
     * <pre>
     * new OutputTag&lt;&gt;("invalid-identity");
     * </pre>
     *
     * <p>Vì Flink cần biết kiểu InvalidIdentityRecord.</p>
     */
    public static final OutputTag<InvalidIdentityRecord>
            INVALID_IDENTITY_TAG =
            new OutputTag<InvalidIdentityRecord>(
                    "silver-invalid-identity"
            ) {
            };

    private final UeIdentityResolver resolver;

    public SilverIdentityProcessFunction(
            UeIdentityResolver resolver
    ) {
        this.resolver = Objects.requireNonNull(
                resolver,
                "resolver must not be null"
        );
    }

    @Override
    public void processElement(
            BronzeEvent value,
            Context context,
            Collector<IdentityResolvedEvent> out
    ) {
        /*
         * Đây là processing time tại Silver.
         *
         * Không dùng context.timestamp() vì timestamp của stream
         * chưa được gán ở Checkpoint 7.
         */
        long processingTimeMillis =
                context.timerService()
                        .currentProcessingTime();

        IdentityResolutionResult result =
                resolver.resolve(
                        value,
                        processingTimeMillis
                );

        if (result.isResolved()) {
            out.collect(
                    result.getResolvedEvent()
            );
            return;
        }

        /*
         * Record không biến mất.
         * Nó được route sang side output để ghi vào
         * topic invalid-identity ở bước lắp SilverJob.
         */
        context.output(
                INVALID_IDENTITY_TAG,
                result.getInvalidRecord()
        );
    }
}