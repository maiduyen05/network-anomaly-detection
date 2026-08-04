package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceWindow;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Gom event theo từng UE thành sliding sequence.
 *
 * <p>Operator này bắt buộc phải được gọi sau:</p>
 *
 * <pre>
 * stream.keyBy(GoldSequenceEvent::ueKey)
 * </pre>
 *
 * <p>Thuật toán:</p>
 *
 * <ol>
 *     <li>Giữ event mới trong pending state.</li>
 *     <li>Đăng ký event-time timer tại eventTime.</li>
 *     <li>Khi watermark vượt qua eventTime, event được xem là an toàn để sort.</li>
 *     <li>Đưa event đã sort vào sliding-window state.</li>
 *     <li>Khi đủ 32 event, tạo một GoldSequenceWindow.</li>
 *     <li>Xóa 8 event đầu để thực hiện stride 8.</li>
 * </ol>
 */
public final class GoldSequenceProcessFunction
        extends KeyedProcessFunction<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> {

    /**
     * Event đến sau khi watermark đã đi qua eventTime không được
     * chèn ngược vào sequence đã phát trước đó.
     */
    public static final OutputTag<GoldSequenceEvent>
            TOO_LATE_EVENT_TAG =
            new OutputTag<GoldSequenceEvent>(
                    "gold-too-late-events"
            ) {
            };

    private final int sequenceLength;
    private final int stride;
    private final long stateTtlMs;
    private final GoldSequenceWindowFactory windowFactory;

    /**
     * Event đang chờ watermark xác nhận.
     */
    private transient ListState<GoldSequenceEvent>
            pendingEventState;

    /**
     * Event đã có thứ tự, đang chờ đủ sequenceLength.
     */
    private transient ListState<GoldSequenceEvent>
            slidingWindowState;

    public GoldSequenceProcessFunction(
            int sequenceLength,
            int stride,
            long stateTtlMs,
            String schemaVersion,
            String featureVersion
    ) {
        if (sequenceLength <= 0) {
            throw new IllegalArgumentException(
                    "sequenceLength must be positive"
            );
        }

        if (stride <= 0 || stride > sequenceLength) {
            throw new IllegalArgumentException(
                    "stride must be between 1 and sequenceLength"
            );
        }

        if (stateTtlMs <= 0) {
            throw new IllegalArgumentException(
                    "stateTtlMs must be positive"
            );
        }

        this.sequenceLength = sequenceLength;
        this.stride = stride;
        this.stateTtlMs = stateTtlMs;

        this.windowFactory =
                new GoldSequenceWindowFactory(
                        sequenceLength,
                        stride,
                        schemaVersion,
                        featureVersion
                );
    }

    /**
     * Khởi tạo Flink managed state.
     *
     * <p>Không dùng ArrayList làm field thông thường để lưu event,
     * vì field Java thông thường không được Flink checkpoint.</p>
     */
    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig =
                StateTtlConfig
                        .newBuilder(
                                Duration.ofMillis(stateTtlMs)
                        )
                        /*
                         * TTL được làm mới khi state được tạo
                         * hoặc được ghi lại.
                         */
                        .setUpdateType(
                                StateTtlConfig.UpdateType
                                        .OnCreateAndWrite
                        )
                        /*
                         * Không trả dữ liệu đã hết hạn cho operator.
                         */
                        .setStateVisibility(
                                StateTtlConfig.StateVisibility
                                        .NeverReturnExpired
                        )
                        .build();

        ListStateDescriptor<GoldSequenceEvent>
                pendingDescriptor =
                new ListStateDescriptor<>(
                        "gold-pending-event-time-buffer",
                        TypeInformation.of(
                                GoldSequenceEvent.class
                        )
                );

        pendingDescriptor.enableTimeToLive(
                ttlConfig
        );

        pendingEventState =
                getRuntimeContext().getListState(
                        pendingDescriptor
                );

        ListStateDescriptor<GoldSequenceEvent>
                slidingDescriptor =
                new ListStateDescriptor<>(
                        "gold-sliding-sequence-buffer",
                        TypeInformation.of(
                                GoldSequenceEvent.class
                        )
                );

        slidingDescriptor.enableTimeToLive(
                ttlConfig
        );

        slidingWindowState =
                getRuntimeContext().getListState(
                        slidingDescriptor
                );
    }

    /**
     * Nhận một event mới.
     */
    @Override
    public void processElement(
            GoldSequenceEvent event,
            Context context,
            Collector<GoldSequenceWindow> output
    ) throws Exception {
        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        /*
         * Bảo vệ topology.
         *
         * Nếu stream keyBy nhầm field, operator phải fail rõ ràng
         * thay vì trộn state của UE.
         */
        if (!event.ueKey().equals(
                context.getCurrentKey()
        )) {
            throw new IllegalStateException(
                    "Current keyed context does not match event ueKey"
            );
        }

        long eventTimestamp =
                event.eventTime().toEpochMilli();

        long currentWatermark =
                context
                        .timerService()
                        .currentWatermark();

        /*
         * Khi watermark đã vượt eventTime, các window trước đó có thể
         * đã được phát. Không thể chèn event này ngược vào output cũ.
         */
        if (currentWatermark != Long.MIN_VALUE
                && eventTimestamp <= currentWatermark) {

            context.output(
                    TOO_LATE_EVENT_TAG,
                    event
            );

            return;
        }

        /*
         * Chưa đưa event thẳng vào sliding window.
         * Nó phải chờ watermark để chắc chắn không còn event cũ hơn
         * nằm trong ngưỡng out-of-order.
         */
        pendingEventState.add(
                event
        );

        /*
         * Event-time timer được kích hoạt khi watermark đạt timestamp này.
         */
        context
                .timerService()
                .registerEventTimeTimer(
                        eventTimestamp
                );
    }

    /**
     * Được Flink gọi khi watermark đi qua timestamp đã đăng ký.
     */
    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<GoldSequenceWindow> output
    ) throws Exception {
        List<GoldSequenceEvent> dueEvents =
                new ArrayList<>();

        List<GoldSequenceEvent> stillPendingEvents =
                new ArrayList<>();

        /*
         * Tách pending state thành:
         *
         * - dueEvents: đã an toàn để xử lý;
         * - stillPendingEvents: vẫn nằm phía trước watermark.
         */
        for (GoldSequenceEvent event
                : pendingEventState.get()) {

            if (event.eventTime().toEpochMilli()
                    <= timestamp) {

                dueEvents.add(event);

            } else {
                stillPendingEvents.add(event);
            }
        }

        if (stillPendingEvents.isEmpty()) {
            pendingEventState.clear();
        } else {
            pendingEventState.update(
                    stillPendingEvents
            );
        }

        /*
         * Sort deterministic:
         *
         * 1. eventTime;
         * 2. sourceOrderKey.
         */
        dueEvents.sort(
                GoldSequenceEvent.EVENT_TIME_ORDER
        );

        List<GoldSequenceEvent> sequenceBuffer =
                new ArrayList<>();

        for (GoldSequenceEvent event
                : slidingWindowState.get()) {

            sequenceBuffer.add(event);
        }

        sequenceBuffer.addAll(
                dueEvents
        );

        /*
         * Bình thường mỗi lần chỉ đủ tạo tối đa một window.
         * Dùng while để vẫn đúng khi watermark nhảy xa và giải phóng
         * nhiều event pending trong một lần.
         */
        while (sequenceBuffer.size()
                >= sequenceLength) {

            List<GoldSequenceEvent> windowEvents =
                    new ArrayList<>(
                            sequenceBuffer.subList(
                                    0,
                                    sequenceLength
                            )
                    );

            output.collect(
                    windowFactory.create(
                            windowEvents
                    )
            );

            /*
             * Thực hiện stride.
             *
             * Với length=32, stride=8:
             * - phát event 1..32;
             * - xóa event 1..8;
             * - giữ event 9..32;
             * - chờ thêm event 33..40.
             */
            sequenceBuffer
                    .subList(0, stride)
                    .clear();
        }

        if (sequenceBuffer.isEmpty()) {
            slidingWindowState.clear();
        } else {
            slidingWindowState.update(
                    sequenceBuffer
            );
        }
    }
}