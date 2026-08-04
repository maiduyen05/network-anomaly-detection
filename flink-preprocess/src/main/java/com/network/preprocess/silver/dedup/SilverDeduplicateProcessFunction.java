package com.network.preprocess.silver.dedup;

import com.network.preprocess.model.SilverEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Loại bỏ SilverEvent bị lặp theo Kafka source metadata.
 *
 * <p>Operator này phải được gọi sau:</p>
 *
 * <pre>
 * stream.keyBy(new SilverDedupKeySelector())
 * </pre>
 *
 * <p>Mỗi dedup key có một ValueState riêng:</p>
 *
 * <ul>
 *     <li>State chưa có: đây là event đầu tiên, cho đi tiếp.</li>
 *     <li>State bằng true: event đã xuất hiện, không output lần nữa.</li>
 * </ul>
 *
 * <p>State có TTL để tránh tăng vô hạn khi job chạy lâu dài.</p>
 */
public final class SilverDeduplicateProcessFunction
        extends KeyedProcessFunction<String, SilverEvent, SilverEvent> {

    /**
     * Thời gian giữ một dedup key trong Flink state.
     */
    private final long stateTtlMs;

    /**
     * State đánh dấu current key đã được xử lý.
     *
     * <p>Đặt transient vì Flink sẽ tự khởi tạo state khi open operator.
     * Không serialize trực tiếp object ValueState vào job graph.</p>
     */
    private transient ValueState<Boolean> seenState;

    /**
     * @param stateTtlMs TTL của dedup state, đơn vị millisecond
     */
    public SilverDeduplicateProcessFunction(long stateTtlMs) {
        if (stateTtlMs <= 0) {
            throw new IllegalArgumentException(
                    "stateTtlMs must be greater than zero"
            );
        }

        this.stateTtlMs = stateTtlMs;
    }

    /**
     * Khởi tạo managed state khi Flink mở operator.
     */
    @Override
    public void open(OpenContext openContext) throws Exception {
        /*
         * TTL được cập nhật khi key được tạo hoặc ghi lại.
         *
         * NeverReturnExpired bảo đảm state hết hạn không bị trả về
         * như một dedup key còn hợp lệ.
         */
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofMillis(stateTtlMs))
                .setUpdateType(
                        StateTtlConfig.UpdateType.OnCreateAndWrite
                )
                .setStateVisibility(
                        StateTtlConfig.StateVisibility.NeverReturnExpired
                )
                .build();

        /*
         * Mỗi key chỉ cần lưu true/không tồn tại.
         * Không cần lưu lại toàn bộ SilverEvent trong state.
         */
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>(
                        "silver-seen-source-offset",
                        Boolean.class
                );

        descriptor.enableTimeToLive(ttlConfig);

        seenState = getRuntimeContext().getState(descriptor);
    }

    /**
     * Kiểm tra một SilverEvent có bị lặp hay không.
     */
    @Override
    public void processElement(
            SilverEvent event,
            Context context,
            Collector<SilverEvent> output
    ) throws Exception {

        Boolean alreadySeen = seenState.value();

        /*
         * Nếu state đã là true, event này đã được xử lý.
         * Không collect nghĩa là event bị loại khỏi main stream.
         */
        if (Boolean.TRUE.equals(alreadySeen)) {
            return;
        }

        /*
         * Đánh dấu trước khi output.
         *
         * State này nằm trong checkpoint của Flink. Khi job restore,
         * dedup state cũng được restore cùng Kafka source offset.
         */
        seenState.update(Boolean.TRUE);

        output.collect(event);
    }
}