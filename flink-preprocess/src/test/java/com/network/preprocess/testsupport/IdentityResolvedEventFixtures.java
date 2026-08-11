package com.network.preprocess.testsupport;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.BronzeSourceMetadata;
import com.network.preprocess.model.IdentityResolutionSource;
import com.network.preprocess.model.IdentityResolvedEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory tạo IdentityResolvedEvent cho test Checkpoint 8.
 */
public final class IdentityResolvedEventFixtures {

    private IdentityResolvedEventFixtures() {
    }

    public static IdentityResolvedEvent event(
            String eventId,
            String eventResult
    ) {
        String imsi =
                "452010123456789";

        Map<String, String> rawFields =
                new LinkedHashMap<>();

        rawFields.put(
                "EVENT_ID",
                eventId == null ? "" : eventId
        );

        rawFields.put(
                "EVENT_RESULT",
                eventResult == null ? "" : eventResult
        );

        rawFields.put(
                "SUB_TYPE",
                "normal"
        );

        rawFields.put(
                "IMSI",
                imsi
        );

        BronzeSourceMetadata source =
                new BronzeSourceMetadata(
                        "ue-log-20260803.csv",
                        10L,
                        "raw.ue.log.line",
                        2,
                        100L,
                        "2026-08-03T08:00:00Z",
                        "2026-08-03T08:00:01Z"
                );

        BronzeEvent bronzeEvent =
                new BronzeEvent(
                        "bronze-v1",
                        "raw-record-001",

                        eventId,
                        eventResult,
                        120L,
                        1,
                        2,

                        "2026-08-03T08:00:00Z",
                        "source",

                        "84901234567",
                        imsi,
                        "A1B2C3D4",
                        "3567890123456789",
                        "100",
                        "01",
                        "0",
                        "msc-01",
                        "1001",
                        "123456",
                        "sgw-01",
                        "pgw-01",

                        rawFields,
                        source
                );

        return new IdentityResolvedEvent(
                imsi,
                imsi,
                IdentityResolutionSource.DIRECT_IMSI,
                bronzeEvent
        );
    }
}