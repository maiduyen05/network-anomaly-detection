package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceWindow;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
 * Gom event theo từng UE thành sliding sequence dùng chung cho
 * training replay và online serving.
 *
 * <p>Operator này bắt buộc phải được gọi sau:</p>
 *
 * <pre>
 * stream.keyBy(GoldSequenceEvent::ueKey)
 * </pre>
 *
 * <p>
 * Điểm quan trọng: quyết định ordering/late được thực hiện theo
 * timeline của CHÍNH UE hiện tại, không dùng watermark chung của
 * Kafka partition. Một Silver Kafka partition có thể chứa rất nhiều UE
 * với event-time khác nhau; nếu dùng partition watermark để quyết định
 * late thì một UE có timestamp tiến xa có thể làm event hợp lệ của UE
 * khác bị loại nhầm.
 * </p>
 *
 * <p>Thuật toán:</p>
 *
 * <ol>
 *     <li>Giữ max event-time đã thấy cho từng ueKey.</li>
 *     <li>Giữ event mới trong reorder buffer của chính UE đó.</li>
 *     <li>
 *         safeThrough = maxSeenEventTime - perUeMaxOutOfOrderness.
 *         Event có eventTime <= safeThrough được sort và finalize.
 *     </li>
 *     <li>Event finalized được đưa vào sliding-window state.</li>
 *     <li>Khi đủ sequenceLength event, tạo GoldSequenceWindow.</li>
 *     <li>Xóa stride event đầu và tiếp tục chờ window kế tiếp.</li>
 *     <li>
 *         Nếu UE im lặng đủ lâu theo processing time, flush phần reorder
 *         buffer còn lại. Cơ chế này giúp historical replay kết thúc được
 *         phần tail và đồng thời dùng được cho online serving.
 *     </li>
 * </ol>
 */
public final class GoldSequenceProcessFunction
        extends KeyedProcessFunction<
                String,
                GoldSequenceEvent,
                GoldSequenceWindow> {

    /**
     * Mặc định giữ cùng tolerance 30 giây với cấu hình Gold hiện tại.
     *
     * <p>
     * Đây là tolerance theo TỪNG UE, không phải theo toàn Kafka partition.
     * </p>
     */
    public static final long DEFAULT_PER_UE_MAX_OUT_OF_ORDERNESS_MS =
            30_000L;

    /**
     * UE im lặng 60 giây thì flush phần reorder buffer còn lại.
     */
    public static final long DEFAULT_PER_UE_IDLE_FLUSH_MS =
            60_000L;

    /**
     * Event thực sự đến quá muộn so với timeline đã finalized
     * của CHÍNH UE đó.
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
    private final long perUeMaxOutOfOrdernessMs;
    private final long perUeIdleFlushMs;
    private final GoldSequenceWindowFactory windowFactory;

    /**
     * Event chưa được finalize, đang chờ reorder theo chính ueKey.
     */
    private transient ListState<GoldSequenceEvent>
            pendingEventState;

    /**
     * Event đã được order/finalize, đang chờ đủ sequenceLength.
     */
    private transient ListState<GoldSequenceEvent>
            slidingWindowState;

    /**
     * Event-time lớn nhất từng thấy của chính UE hiện tại.
     */
    private transient ValueState<Long>
            maxSeenEventTimeState;

    /**
     * Mốc event-time đã được cam kết là finalized của chính UE.
     *
     * <p>
     * Event đến sau với timestamp <= mốc này không thể chèn ngược vào
     * sequence đã phát nên mới được xem là too-late.
     * </p>
     */
    private transient ValueState<Long>
            finalizedThroughState;

    /**
     * Processing-time timer gần nhất dùng để flush UE khi idle.
     */
    private transient ValueState<Long>
            idleFlushTimerState;

    /**
     * Constructor compatibility với topology hiện tại.
     *
     * <p>
     * Giữ mặc định 30 giây reorder tolerance và 60 giây idle flush.
     * </p>
     */
    public GoldSequenceProcessFunction(
            int sequenceLength,
            int stride,
            long stateTtlMs,
            String schemaVersion,
            String featureVersion
    ) {
        this(
                sequenceLength,
                stride,
                stateTtlMs,
                schemaVersion,
                featureVersion,
                DEFAULT_PER_UE_MAX_OUT_OF_ORDERNESS_MS,
                DEFAULT_PER_UE_IDLE_FLUSH_MS
        );
    }

    /**
     * Constructor đầy đủ để có thể cấu hình riêng tolerance/idle flush
     * trong checkpoint tiếp theo mà không đổi thuật toán.
     */
    public GoldSequenceProcessFunction(
            int sequenceLength,
            int stride,
            long stateTtlMs,
            String schemaVersion,
            String featureVersion,
            long perUeMaxOutOfOrdernessMs,
            long perUeIdleFlushMs
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

        if (perUeMaxOutOfOrdernessMs < 0) {
            throw new IllegalArgumentException(
                    "perUeMaxOutOfOrdernessMs must not be negative"
            );
        }

        if (perUeIdleFlushMs <= 0) {
            throw new IllegalArgumentException(
                    "perUeIdleFlushMs must be positive"
            );
        }

        this.sequenceLength = sequenceLength;
        this.stride = stride;
        this.stateTtlMs = stateTtlMs;
        this.perUeMaxOutOfOrdernessMs =
                perUeMaxOutOfOrdernessMs;
        this.perUeIdleFlushMs =
                perUeIdleFlushMs;

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
     * <p>
     * Tất cả state đều là keyed state, vì vậy sau keyBy(ueKey)
     * mỗi UE có reorder buffer và timeline độc lập.
     * </p>
     */
    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig =
                StateTtlConfig
                        .newBuilder(
                                Duration.ofMillis(stateTtlMs)
                        )
                        .setUpdateType(
                                StateTtlConfig.UpdateType
                                        .OnCreateAndWrite
                        )
                        .setStateVisibility(
                                StateTtlConfig.StateVisibility
                                        .NeverReturnExpired
                        )
                        .build();

        ListStateDescriptor<GoldSequenceEvent>
                pendingDescriptor =
                new ListStateDescriptor<>(
                        "gold-per-ue-reorder-buffer",
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

        ValueStateDescriptor<Long>
                maxSeenDescriptor =
                new ValueStateDescriptor<>(
                        "gold-per-ue-max-seen-event-time",
                        Long.class
                );

        maxSeenDescriptor.enableTimeToLive(
                ttlConfig
        );

        maxSeenEventTimeState =
                getRuntimeContext().getState(
                        maxSeenDescriptor
                );

        ValueStateDescriptor<Long>
                finalizedThroughDescriptor =
                new ValueStateDescriptor<>(
                        "gold-per-ue-finalized-through",
                        Long.class
                );

        finalizedThroughDescriptor.enableTimeToLive(
                ttlConfig
        );

        finalizedThroughState =
                getRuntimeContext().getState(
                        finalizedThroughDescriptor
                );

        ValueStateDescriptor<Long>
                idleTimerDescriptor =
                new ValueStateDescriptor<>(
                        "gold-per-ue-idle-flush-timer",
                        Long.class
                );

        idleTimerDescriptor.enableTimeToLive(
                ttlConfig
        );

        idleFlushTimerState =
                getRuntimeContext().getState(
                        idleTimerDescriptor
                );
    }

    /**
     * Nhận một event mới của UE hiện tại.
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
         * Nếu upstream keyBy nhầm field thì fail rõ ràng,
         * không được trộn state của nhiều UE.
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

        Long finalizedThrough =
                finalizedThroughState.value();

        /*
         * Late được quyết định theo timeline của CHÍNH UE.
         *
         * Không đọc currentWatermark() ở đây.
         * Watermark của Kafka partition có thể bị đẩy bởi UE khác và
         * không đại diện cho tiến độ event-time của UE hiện tại.
         */
        if (finalizedThrough != null
                && eventTimestamp <= finalizedThrough) {

            context.output(
                    TOO_LATE_EVENT_TAG,
                    event
            );

            return;
        }

        pendingEventState.add(
                event
        );

        Long previousMaxSeen =
                maxSeenEventTimeState.value();

        long maxSeenEventTime =
                previousMaxSeen == null
                        ? eventTimestamp
                        : Math.max(
                                previousMaxSeen,
                                eventTimestamp
                        );

        /*
         * Ghi lại mỗi lần để refresh TTL khi UE vẫn active,
         * kể cả event hiện tại là out-of-order và không tạo max mới.
         */
        maxSeenEventTimeState.update(
                maxSeenEventTime
        );

        long safeThrough =
                maxSeenEventTime
                        - perUeMaxOutOfOrdernessMs;

        /*
         * Finalize phần timeline đủ cũ so với max event-time
         * của chính UE này.
         */
        releasePendingUpTo(
                safeThrough,
                output
        );

        advanceFinalizedThrough(
                safeThrough
        );

        /*
         * Mỗi event reset idle timer của chính UE.
         *
         * Training replay: khi producer hết dữ liệu, timer giúp flush tail.
         * Online serving: UE im lặng đủ lâu cũng được flush tail để tránh
         * giữ event vô thời hạn.
         */
        scheduleIdleFlush(
                context
        );
    }

    /**
     * Chỉ dùng processing-time timer cho idle flush.
     *
     * <p>
     * Không dùng event-time timer/global watermark để reorder nữa.
     * </p>
     */
    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext context,
            Collector<GoldSequenceWindow> output
    ) throws Exception {
        Long expectedIdleTimer =
                idleFlushTimerState.value();

        /*
         * Timer cũ có thể vẫn fire sau khi key đã được reschedule.
         * Chỉ timer mới nhất mới được phép flush state.
         */
        if (expectedIdleTimer == null
                || timestamp != expectedIdleTimer) {
            return;
        }

        flushAllPending(
                output
        );

        Long maxSeenEventTime =
                maxSeenEventTimeState.value();

        if (maxSeenEventTime != null) {
            /*
             * Sau idle flush, toàn bộ timeline <= maxSeen đã được commit.
             * Event cũ hơn tới sau thời điểm này là too-late thật sự
             * đối với chính UE này.
             */
            advanceFinalizedThrough(
                    maxSeenEventTime
            );
        }

        idleFlushTimerState.clear();
    }

    /**
     * Finalize các pending event có eventTime <= safeThrough.
     */
    private void releasePendingUpTo(
            long safeThrough,
            Collector<GoldSequenceWindow> output
    ) throws Exception {
        List<GoldSequenceEvent> dueEvents =
                new ArrayList<>();

        List<GoldSequenceEvent> stillPendingEvents =
                new ArrayList<>();

        for (GoldSequenceEvent pendingEvent
                : pendingEventState.get()) {

            if (pendingEvent
                    .eventTime()
                    .toEpochMilli()
                    <= safeThrough) {

                dueEvents.add(
                        pendingEvent
                );

            } else {
                stillPendingEvents.add(
                        pendingEvent
                );
            }
        }

        if (stillPendingEvents.isEmpty()) {
            pendingEventState.clear();
        } else {
            pendingEventState.update(
                    stillPendingEvents
            );
        }

        appendFinalizedEvents(
                dueEvents,
                output
        );
    }

    /**
     * Flush toàn bộ reorder buffer của UE khi UE idle.
     */
    private void flushAllPending(
            Collector<GoldSequenceWindow> output
    ) throws Exception {
        List<GoldSequenceEvent> pendingEvents =
                new ArrayList<>();

        for (GoldSequenceEvent pendingEvent
                : pendingEventState.get()) {
            pendingEvents.add(
                    pendingEvent
            );
        }

        pendingEventState.clear();

        appendFinalizedEvents(
                pendingEvents,
                output
        );
    }

    /**
     * Sort deterministic rồi nối event vào sliding sequence buffer.
     */
    private void appendFinalizedEvents(
            List<GoldSequenceEvent> finalizedEvents,
            Collector<GoldSequenceWindow> output
    ) throws Exception {
        if (finalizedEvents.isEmpty()) {
            return;
        }

        /*
         * Deterministic order:
         * 1. eventTime;
         * 2. sourceOrderKey.
         */
        finalizedEvents.sort(
                GoldSequenceEvent.EVENT_TIME_ORDER
        );

        List<GoldSequenceEvent> sequenceBuffer =
                new ArrayList<>();

        for (GoldSequenceEvent existingEvent
                : slidingWindowState.get()) {
            sequenceBuffer.add(
                    existingEvent
            );
        }

        sequenceBuffer.addAll(
                finalizedEvents
        );

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
             * length=32, stride=8:
             * window 1 = 1..32
             * giữ 9..32
             * chờ thêm 33..40
             * window 2 = 9..40
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

    /**
     * Tiến mốc finalized của UE theo chiều tăng duy nhất.
     */
    private void advanceFinalizedThrough(
            long candidateTimestamp
    ) throws Exception {
        Long currentFinalizedThrough =
                finalizedThroughState.value();

        if (currentFinalizedThrough == null
                || candidateTimestamp
                > currentFinalizedThrough) {

            finalizedThroughState.update(
                    candidateTimestamp
            );
        }
    }

    /**
     * Reset processing-time idle timer của UE hiện tại.
     */
    private void scheduleIdleFlush(
            Context context
    ) throws Exception {
        Long previousTimer =
                idleFlushTimerState.value();

        if (previousTimer != null) {
            context
                    .timerService()
                    .deleteProcessingTimeTimer(
                            previousTimer
                    );
        }

        long nextTimer =
                context
                        .timerService()
                        .currentProcessingTime()
                        + perUeIdleFlushMs;

        context
                .timerService()
                .registerProcessingTimeTimer(
                        nextTimer
                );

        idleFlushTimerState.update(
                nextTimer
        );
    }
}
