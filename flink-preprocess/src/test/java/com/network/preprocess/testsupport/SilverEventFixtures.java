package com.network.preprocess.testsupport;

import com.network.preprocess.model.BronzeSourceMetadata;
import com.network.preprocess.model.EventResult;
import com.network.preprocess.model.IdentityResolutionSource;
import com.network.preprocess.model.SilverDisplay;
import com.network.preprocess.model.SilverEvent;
import com.network.preprocess.model.SilverQuality;

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Cung cấp các SilverEvent mẫu dùng chung cho unit test.
 *
 * <p>Fixture giúp các test chỉ cần truyền những giá trị có liên quan
 * trực tiếp đến nội dung cần kiểm tra, chẳng hạn Kafka offset
 * và event time.</p>
 */
public final class SilverEventFixtures {

    /**
     * Không cho tạo object SilverEventFixtures vì class này
     * chỉ chứa phương thức static hỗ trợ test.
     */
    private SilverEventFixtures() {
    }

    /**
     * Tạo một SilverEvent hợp lệ dùng trong unit test.
     *
     * @param sourceOffset Kafka source offset của event
     * @param eventTime    thời điểm xảy ra event dạng ISO-8601 UTC
     * @return SilverEvent có đầy đủ các field bắt buộc
     */
    public static SilverEvent event(
            long sourceOffset,
            String eventTime
    ) {
        return new SilverEvent(
                // Phiên bản schema của tầng Silver.
                "silver-v1",

                // ID của Silver event.
                "silver-event-" + sourceOffset,

                // ID record gốc tại Bronze.
                "raw-record-" + sourceOffset,

                // ueKey ổn định sau khi resolve identity.
                "452040000000001",

                // IMSI đã được resolve.
                "452040000000001",

                // EVENT_ID đã chuẩn hóa.
                "l_service_request",

                // EVENT_RESULT đã chuẩn hóa.
                EventResult.SUCCESS,

                // DURATION, đơn vị millisecond.
                120L,

                // REQUEST_RETRIES là Integer.
                0,

                /*
                 * SUB_TYPE trong SilverEvent cũng là Integer.
                 *
                 * Trước đây fixture truyền "normal", gây lỗi:
                 * String cannot be converted to Integer.
                 */
                0,

                // EVENT_TIME dạng ISO-8601 UTC.
                eventTime,

                // Phía báo cáo sự kiện.
                "left",

                // MSISDN.
                "84900000001",

                // MTMSI.
                "0x1234",

                // IMEISV.
                "3567890123456701",

                // MMEGI.
                "10",

                // MMEC.
                "01",

                // TAC.
                "1001",

                // ECI.
                "20001",

                // SGW.
                "SGW01",

                // SGSN.
                "SGSN01",

                /*
                 * SilverEvent bắt buộc display khác null.
                 */
                new SilverDisplay(
                        "Service Request",
                        "Success"
                ),

                /*
                 * SilverEvent bắt buộc quality khác null.
                 */
                new SilverQuality(
                        IdentityResolutionSource.DIRECT_IMSI,
                        false, // EVENT_ID không bị thay đổi trong fixture.
                        false, // EVENT_RESULT không bị thay đổi.
                        true,  // EVENT_RESULT được nhận diện.
                        new ArrayList<>()
                ),

                /*
                 * Dùng LinkedHashMap mutable để Flink/Kryo có thể
                 * copy object trong test harness.
                 */
                new LinkedHashMap<>(),

                // Metadata của Kafka record gốc.
                new BronzeSourceMetadata(
                        "test-log-file",
                        sourceOffset + 1,
                        "raw.ue.log.line",
                        0,
                        sourceOffset,
                        "2026-07-08T10:15:30Z",
                        "2026-07-08T10:15:31Z"
                )
        );
    }
}