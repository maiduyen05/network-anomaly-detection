package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceWindow;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Chuyển đúng sequenceLength event đã sắp xếp thành GoldSequenceWindow.
 */
public final class GoldSequenceWindowFactory
        implements Serializable {

    private final int sequenceLength;
    private final int stride;
    private final String schemaVersion;
    private final String featureVersion;

    public GoldSequenceWindowFactory(
            int sequenceLength,
            int stride,
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

        this.sequenceLength = sequenceLength;
        this.stride = stride;
        this.schemaVersion = requiredText(
                schemaVersion,
                "schemaVersion"
        );
        this.featureVersion = requiredText(
                featureVersion,
                "featureVersion"
        );
    }

    /**
     * Tạo một sequence window.
     *
     * @param orderedEvents danh sách đã được sắp xếp theo eventTime
     */
    public GoldSequenceWindow create(
            List<GoldSequenceEvent> orderedEvents
    ) {
        Objects.requireNonNull(
                orderedEvents,
                "orderedEvents must not be null"
        );

        if (orderedEvents.size() != sequenceLength) {
            throw new IllegalArgumentException(
                    "Expected "
                            + sequenceLength
                            + " events but received "
                            + orderedEvents.size()
            );
        }

        /*
         * Tạo bản copy để không làm thay đổi ListState bên ngoài.
         */
        List<GoldSequenceEvent> events =
                new ArrayList<>(orderedEvents);

        /*
         * Kiểm tra operator đã sắp đúng thứ tự.
         *
         * Factory không âm thầm sort lại vì nếu operator có lỗi,
         * test phải phát hiện thay vì che giấu lỗi đó.
         */
        for (int index = 1;
             index < events.size();
             index++) {

            GoldSequenceEvent previous =
                    events.get(index - 1);

            GoldSequenceEvent current =
                    events.get(index);

            if (GoldSequenceEvent.EVENT_TIME_ORDER.compare(
                    previous,
                    current
            ) > 0) {
                throw new IllegalArgumentException(
                        "Events must be ordered by eventTime"
                );
            }
        }

        GoldSequenceEvent firstEvent =
                events.get(0);

        GoldSequenceEvent lastEvent =
                events.get(events.size() - 1);

        /*
         * Một Gold window không được trộn event của nhiều UE.
         */
        for (GoldSequenceEvent event : events) {
            if (!firstEvent.ueKey().equals(event.ueKey())) {
                throw new IllegalArgumentException(
                        "All events must belong to the same ueKey"
                );
            }

            if (!firstEvent.imsi().equals(event.imsi())) {
                throw new IllegalArgumentException(
                        "All events must belong to the same IMSI"
                );
            }
        }

        String sampleId =
                GoldSampleIdGenerator.generate(
                        firstEvent.ueKey(),
                        firstEvent.eventTime(),
                        lastEvent.eventTime(),
                        events
                );

        return new GoldSequenceWindow(
                schemaVersion,
                featureVersion,
                sampleId,
                firstEvent.ueKey(),
                firstEvent.imsi(),
                firstEvent.eventTime(),
                lastEvent.eventTime(),
                sequenceLength,
                stride,
                events
        );
    }

    private static String requiredText(
            String value,
            String fieldName
    ) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    fieldName + " must not be blank"
            );
        }

        return value.trim();
    }
}