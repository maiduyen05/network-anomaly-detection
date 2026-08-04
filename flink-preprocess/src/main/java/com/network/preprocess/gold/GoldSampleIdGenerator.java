package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Sinh sampleId ổn định cho Gold sequence.
 *
 * <p>Cùng UE, cùng window và cùng chuỗi event luôn tạo cùng sampleId,
 * kể cả khi Flink restart và xử lý lại dữ liệu từ checkpoint.</p>
 */
public final class GoldSampleIdGenerator {

    private GoldSampleIdGenerator() {
    }

    public static String generate(
            String ueKey,
            Instant windowStart,
            Instant windowEnd,
            List<GoldSequenceEvent> events
    ) {
        Objects.requireNonNull(ueKey, "ueKey must not be null");
        Objects.requireNonNull(
                windowStart,
                "windowStart must not be null"
        );
        Objects.requireNonNull(
                windowEnd,
                "windowEnd must not be null"
        );
        Objects.requireNonNull(
                events,
                "events must not be null"
        );

        /*
         * Chuỗi canonical dùng làm input cho SHA-256.
         *
         * Không dùng thời gian xử lý hiện tại hoặc UUID ngẫu nhiên,
         * vì các giá trị đó sẽ thay đổi sau mỗi lần replay.
         */
        StringBuilder canonicalValue =
                new StringBuilder()
                        .append(ueKey)
                        .append('|')
                        .append(windowStart)
                        .append('|')
                        .append(windowEnd);

        for (GoldSequenceEvent event : events) {
            /*
             * eventId thể hiện chuỗi hành vi nghiệp vụ.
             * sourceOrderKey giúp phân biệt các record giống eventId
             * và cùng eventTime.
             */
            canonicalValue
                    .append('|')
                    .append(event.eventId())
                    .append('@')
                    .append(event.sourceOrderKey());
        }

        try {
            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");

            byte[] hash =
                    digest.digest(
                            canonicalValue
                                    .toString()
                                    .getBytes(StandardCharsets.UTF_8)
                    );

            return HexFormat.of().formatHex(hash);

        } catch (NoSuchAlgorithmException exception) {
            /*
             * SHA-256 là thuật toán bắt buộc của Java runtime.
             * Nếu runtime không có, đây là lỗi môi trường nghiêm trọng.
             */
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    exception
            );
        }
    }
}