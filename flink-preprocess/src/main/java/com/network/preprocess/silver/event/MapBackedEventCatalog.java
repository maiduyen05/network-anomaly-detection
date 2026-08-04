package com.network.preprocess.silver.event;

import com.network.preprocess.model.EventDefinition;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Event catalog bất biến dùng cho unit test và local development.
 *
 * <p>Map đầu vào có dạng:</p>
 *
 * <pre>
 * alias -> EventDefinition
 * </pre>
 *
 * <p>Nhiều alias có thể cùng trỏ về một canonical event.</p>
 */
public final class MapBackedEventCatalog
        implements EventCatalog {

    private final Map<String, EventDefinition> eventsByAlias;

    public MapBackedEventCatalog(
            Map<String, EventDefinition> source
    ) {
        if (source == null || source.isEmpty()) {
            this.eventsByAlias = Collections.emptyMap();
            return;
        }

        Map<String, EventDefinition> result =
                new LinkedHashMap<>();

        for (Map.Entry<String, EventDefinition> entry
                : source.entrySet()) {

            EventDefinition definition =
                    Objects.requireNonNull(
                            entry.getValue(),
                            "Event definition must not be null"
                    );

            String normalizedAlias =
                    EventIdNormalizer
                            .normalizeLookupKey(entry.getKey())
                            .orElseThrow(
                                    () -> new IllegalArgumentException(
                                            "Event catalog contains "
                                                    + "an invalid alias"
                                    )
                            );

            /*
             * Canonical ID cũng phải ở sẵn dạng chuẩn.
             * Không âm thầm sửa canonical ID vì đây là contract
             * được ghi ra Kafka.
             */
            String normalizedCanonical =
                    EventIdNormalizer
                            .normalizeLookupKey(
                                    definition.canonicalEventId()
                            )
                            .orElseThrow(
                                    () -> new IllegalArgumentException(
                                            "Event definition contains "
                                                    + "an invalid canonical ID"
                                    )
                            );

            if (!normalizedCanonical.equals(
                    definition.canonicalEventId()
            )) {
                throw new IllegalArgumentException(
                        "Canonical event ID must already be normalized: "
                                + definition.canonicalEventId()
                );
            }

            EventDefinition previous =
                    result.putIfAbsent(
                            normalizedAlias,
                            definition
                    );

            /*
             * Hai alias sau chuẩn hóa trở thành cùng một key
             * nhưng trỏ tới hai event khác nhau là lỗi catalog.
             */
            if (previous != null
                    && !previous.equals(definition)) {

                throw new IllegalArgumentException(
                        "Event catalog contains conflicting aliases: "
                                + normalizedAlias
                );
            }
        }

        this.eventsByAlias =
                Collections.unmodifiableMap(result);
    }

    @Override
    public Optional<EventDefinition> findByEventId(
            String normalizedEventId
    ) {
        Objects.requireNonNull(
                normalizedEventId,
                "normalizedEventId must not be null"
        );

        return Optional.ofNullable(
                eventsByAlias.get(normalizedEventId)
        );
    }
}