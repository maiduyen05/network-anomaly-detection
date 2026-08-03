package com.network.preprocess.testsupport;

import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.BronzeSourceMetadata;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory tạo BronzeEvent cho unit test.
 *
 * <p>Dùng một factory chung giúp test chỉ tập trung thay đổi
 * identity cần kiểm tra.</p>
 */
public final class BronzeEventFixtures {

    private BronzeEventFixtures() {
    }

    public static BronzeEvent eventWithIdentity(
            String imsi,
            String msisdn,
            String mtmsi,
            String imeisv
    ) {
        Map<String, String> rawFields =
                new LinkedHashMap<>();

        rawFields.put(
                "EVENT_ID",
                "l_service_request"
        );

        rawFields.put(
                "IMSI",
                imsi == null ? "" : imsi
        );

        rawFields.put(
                "MSISDN",
                msisdn == null ? "" : msisdn
        );

        rawFields.put(
                "MTMSI",
                mtmsi == null ? "" : mtmsi
        );

        rawFields.put(
                "IMEISV",
                imeisv == null ? "" : imeisv
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

        return new BronzeEvent(
                "bronze-v1",
                "raw-record-001",

                "l_service_request",
                "success",
                120L,
                1,
                2,

                "2026-08-03T08:00:00Z",
                "source",

                msisdn,
                imsi,
                mtmsi,
                imeisv,
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
    }
}