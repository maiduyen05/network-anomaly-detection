package com.network.preprocess.silver.time;

import com.network.preprocess.model.SilverEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Tách event đúng hạn và event đến quá trễ.
 *
 * <p>Operator này chạy sau khi stream đã được:</p>
 *
 * <ol>
 *     <li>Gắn timestamp.</li>
 *     <li>Gắn watermark.</li>
 *     <li>keyBy theo ueKey.</li>
 * </ol>
 */
public final class SilverLateEventProcessFunction
        extends KeyedProcessFunction<String, SilverEvent, SilverEvent> {

    /**
     * Side output chứa SilverEvent đến quá trễ.
     *
     * <p>Checkpoint sau sẽ nối side output này với Kafka topic
     * cấu hình bởi silver.late-event-topic.</p>
     */
    public static final OutputTag<SilverEvent> LATE_EVENT_TAG =
            new OutputTag<>(
                    "late-ue-event",
                    TypeInformation.of(SilverEvent.class)
            );

    /**
     * Kiểm tra timestamp của từng event so với watermark hiện tại.
     */
    @Override
    public void processElement(
            SilverEvent event,
            Context context,
            Collector<SilverEvent> output
    ) {
        /*
         * Timestamp này đã được SilverWatermarkStrategyFactory
         * gắn vào StreamRecord từ event.eventTime().
         */
        Long eventTimestamp = context.timestamp();

        /*
         * Watermark thể hiện tiến độ event time mà operator
         * cho rằng stream đã đi qua.
         */
        long currentWatermark =
                context.timerService().currentWatermark();

        /*
         * Trước khi Flink phát watermark đầu tiên,
         * currentWatermark là Long.MIN_VALUE.
         *
         * Trong giai đoạn đó chưa thể kết luận event bị trễ.
         */
        boolean watermarkStarted =
                currentWatermark != Long.MIN_VALUE;

        /*
         * Event có timestamp nhỏ hơn hoặc bằng watermark
         * được xem là đến quá trễ.
         */
        boolean isLate =
                eventTimestamp != null
                        && watermarkStarted
                        && eventTimestamp <= currentWatermark;

        if (isLate) {
            context.output(LATE_EVENT_TAG, event);
            return;
        }

        /*
         * Event đúng hạn hoặc vẫn nằm trong khoảng out-of-order
         * được tiếp tục đưa sang main output.
         */
        output.collect(event);
    }
}