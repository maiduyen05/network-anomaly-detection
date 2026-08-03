package com.network.preprocess.bronze;

import com.network.preprocess.model.BronzeDlqRecord;
import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.KafkaRawRecord;
import org.apache.flink.streaming.api.functions
        .ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Điều phối luồng xử lý trong Flink, nhận record từ kafka, gọi transformer, quyết định record đi đâu
 *
 * <ul>
 *     <li>Main output: BronzeEvent hợp lệ.</li>
 *     <li>Side output: BronzeDlqRecord.</li>
 * </ul>
 */
public final class BronzeProcessFunction
        extends ProcessFunction<KafkaRawRecord, BronzeEvent> {

    /**
     * Anonymous subclass giữ generic type tại runtime.
     */
    public static final OutputTag<BronzeDlqRecord> DLQ_TAG =
            new OutputTag<BronzeDlqRecord>(
                    "bronze-dlq"
            ) {
            };

    private final BronzeTransformer transformer;

    public BronzeProcessFunction(
            BronzeTransformer transformer
    ) {
        this.transformer = transformer;
    }

    @Override
    public void processElement(
            KafkaRawRecord value,
            Context context,
            Collector<BronzeEvent> out
    ) {
        /*
         * ingestTime là processing time của Bronze,
         * không phải event time trong log.
         */
        long ingestTimeMillis =
                context.timerService()
                        .currentProcessingTime();

        BronzeTransformResult result =
                transformer.transform(
                        value,
                        ingestTimeMillis
                );

        if (result.isValid()) {
            out.collect(result.getEvent());
            return;
        }

        context.output(
                DLQ_TAG,
                result.getDlqRecord()
        );
    }
}