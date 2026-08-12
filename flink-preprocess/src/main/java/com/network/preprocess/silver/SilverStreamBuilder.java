package com.network.preprocess.silver;

import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.silver.dedup.SilverDedupKeySelector;
import com.network.preprocess.silver.dedup.SilverDeduplicateProcessFunction;
import com.network.preprocess.silver.time.SilverLateEventProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

/**
 * Ghép phần dedup và late-event routing của Silver.
 *
 * <p>
 * Input của builder phải là SilverEvent đã:
 * </p>
 *
 * <ul>
 *     <li>Resolve được IMSI/ueKey.</li>
 *     <li>Chuẩn hóa EVENT_ID.</li>
 *     <li>Chuẩn hóa EVENT_RESULT.</li>
 *     <li>Chuẩn hóa DURATION.</li>
 *     <li>Loại unsupported event khỏi main stream.</li>
 * </ul>
 *
 * <p>
 * Timestamp và watermark đã được tạo tại Bronze Kafka source
 * của SilverJob và được propagate qua các operator.
 * Builder không được assign watermark lại sau shuffle.
 * </p>
 */
public final class SilverStreamBuilder {

    private SilverStreamBuilder() {
    }

    /**
     * Kết quả gồm hai stream riêng:
     *
     * @param onTimeEvents event hợp lệ để ghi silver.ue.event
     * @param lateEvents event quá trễ để ghi late-ue-event
     */
    public record Result(
            DataStream<SilverEvent> onTimeEvents,
            DataStream<SilverEvent> lateEvents
    ) {
    }

    /**
     * Xây phần stateful processing của Silver.
     */
    public static Result build(
            DataStream<SilverEvent> normalizedEvents,
            long stateTtlMs
    ) {

        Objects.requireNonNull(
                normalizedEvents,
                "normalizedEvents must not be null"
        );

        /*
         * =========================================================
         * BƯỚC 1
         * DEDUPLICATE THEO KAFKA SOURCE COORDINATES
         * =========================================================
         *
         * Timestamp và watermark đã tồn tại trên stream
         * trước khi đi vào keyBy này.
         *
         * Không assign watermark sau keyBy vì keyBy gây shuffle.
         */
        SingleOutputStreamOperator<SilverEvent> deduplicated =
                normalizedEvents
                        .keyBy(
                                new SilverDedupKeySelector()
                        )
                        .process(
                                new SilverDeduplicateProcessFunction(
                                        stateTtlMs
                                )
                        )
                        .name(
                                "silver-deduplicate-source-offset"
                        )
                        .uid(
                                "silver-deduplicate-source-offset-v1"
                        );

        /*
         * =========================================================
         * BƯỚC 2
         * ROUTE THEO UE
         * =========================================================
         *
         * Event của cùng UE phải về cùng subtask.
         *
         * Watermark nhận từ Kafka source được propagate
         * xuyên qua dedup operator tới đây.
         */
        SingleOutputStreamOperator<SilverEvent> routed =
                deduplicated
                        .keyBy(
                                SilverEvent::ueKey
                        )
                        .process(
                                new SilverLateEventProcessFunction()
                        )
                        .name(
                                "silver-late-event-router"
                        )
                        .uid(
                                "silver-late-event-router-v1"
                        );

        /*
         * Main output chứa event chưa quá watermark.
         */
        DataStream<SilverEvent> onTimeEvents =
                routed;

        /*
         * Side output chứa event đã quá watermark.
         */
        DataStream<SilverEvent> lateEvents =
                routed.getSideOutput(
                        SilverLateEventProcessFunction
                                .LATE_EVENT_TAG
                );

        return new Result(
                onTimeEvents,
                lateEvents
        );
    }
}