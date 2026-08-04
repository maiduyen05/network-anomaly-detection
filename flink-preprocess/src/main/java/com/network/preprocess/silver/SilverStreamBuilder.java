package com.network.preprocess.silver;

import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.silver.dedup.SilverDedupKeySelector;
import com.network.preprocess.silver.dedup.SilverDeduplicateProcessFunction;
import com.network.preprocess.silver.time.SilverLateEventProcessFunction;
import com.network.preprocess.silver.time.SilverWatermarkStrategyFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

/**
 * Ghép phần dedup, watermark và late-event routing của Silver.
 *
 * <p>Input của builder phải là SilverEvent đã:</p>
 *
 * <ul>
 *     <li>Resolve được IMSI/ueKey.</li>
 *     <li>Chuẩn hóa EVENT_ID.</li>
 *     <li>Chuẩn hóa EVENT_RESULT.</li>
 *     <li>Chuẩn hóa DURATION.</li>
 *     <li>Loại unsupported event khỏi main stream.</li>
 * </ul>
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
            long stateTtlMs,
            long maxOutOfOrdernessMs,
            long idleTimeoutMs
    ) {
        Objects.requireNonNull(
                normalizedEvents,
                "normalizedEvents must not be null"
        );

        /*
         * BƯỚC 1: Chia stream theo Kafka source metadata.
         *
         * Hai bản sao của cùng Kafka record chắc chắn được đưa
         * về cùng một Flink keyed state.
         */
        SingleOutputStreamOperator<SilverEvent> deduplicated =
                normalizedEvents
                        .keyBy(new SilverDedupKeySelector())
                        .process(
                                new SilverDeduplicateProcessFunction(
                                        stateTtlMs
                                )
                        )
                        .name("silver-deduplicate-source-offset")
                        /*
                         * UID phải ổn định để Flink có thể map state
                         * khi restore từ savepoint/checkpoint.
                         */
                        .uid("silver-deduplicate-source-offset-v1");

        /*
         * BƯỚC 2: Gắn event timestamp và watermark.
         */
        SingleOutputStreamOperator<SilverEvent> withWatermarks =
                deduplicated
                        .assignTimestampsAndWatermarks(
                                SilverWatermarkStrategyFactory.create(
                                        maxOutOfOrdernessMs,
                                        idleTimeoutMs
                                )
                        )
                        .name("silver-event-time-watermark")
                        .uid("silver-event-time-watermark-v1");

        /*
         * BƯỚC 3: Chia lại stream theo UE.
         *
         * Từ đây các event của cùng một UE được đưa về cùng subtask,
         * chuẩn bị cho Gold sắp xếp và tạo sequence.
         */
        SingleOutputStreamOperator<SilverEvent> routed =
                withWatermarks
                        .keyBy(SilverEvent::ueKey)
                        .process(
                                new SilverLateEventProcessFunction()
                        )
                        .name("silver-late-event-router")
                        .uid("silver-late-event-router-v1");

        /*
         * Main output chỉ chứa event chưa quá watermark.
         */
        DataStream<SilverEvent> onTimeEvents = routed;

        /*
         * Side output chứa event quá trễ.
         */
        DataStream<SilverEvent> lateEvents =
                routed.getSideOutput(
                        SilverLateEventProcessFunction.LATE_EVENT_TAG
                );

        return new Result(
                onTimeEvents,
                lateEvents
        );
    }
}