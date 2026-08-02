package com.network.producer.kafka;

import com.network.producer.model.RawNetworkEvent;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Xác định Kafka key cho một RawNetworkEvent --> đưa các event cùng UE vào cùng 1 kafka partition để kafka giữ thứ tự trong phạm vi UE
 * Thứ tự ưu tiên:
 *    + IMSI.
 *    + MSISDN
 *    + rawRecordId nếu không lấy được identity
 *
 * Trong đó, IMSI/MSISDN được hash trước khi dùng làm key để tránh
 * đưa định danh thuê bao nguyên bản vào Kafka metadata.</p>
 */
public final class KafkaMessageKeyResolver {

    /**
     * Dấu phân cách field trong raw log.
     */
    private static final String FIELD_DELIMITER =
            ";";

    /**
     * MSISDN là trường thứ 6 trong layout 52 trường.
     */
    private static final int MSISDN_INDEX = 5;

    /**
     * IMSI là trường thứ 7 trong layout 52 trường.
     */
    private static final int IMSI_INDEX = 6;

    /**
     * Tạo Kafka key cho raw event.
     * @param event raw event
     * @return key ổn định dùng cho ProducerRecord
     */
    public String resolve(RawNetworkEvent event) {

        Objects.requireNonNull(
                event,
                "event must not be null"
        );

        /*
         * split(..., -1) giữ cả các field rỗng.
         *
         * Ví dụ:
         * "a;b;;"
         * vẫn được tách thành 4 field.
         */
        String[] fields = event
                .rawPayload()
                .split(FIELD_DELIMITER, -1);

        // Ưu tiên IMSI nếu raw payload có đủ field.
        String imsi = getField(
                fields,
                IMSI_INDEX
        );

        if (!imsi.isBlank()) {
            return sha256(
                    "imsi:" + imsi
            );
        }

        // Nếu thiếu IMSI, thử dùng MSISDN.
        String msisdn = getField(
                fields,
                MSISDN_INDEX
        );

        if (!msisdn.isBlank()) {
            return sha256(
                    "msisdn:" + msisdn
            );
        }

        /*
         * Message lỗi hoặc thiếu identity vẫn phải được gửi.
         *
         * Dùng rawRecordId làm fallback giúp record có key ổn định
         * mà không làm producer crash.
         */
        return event.rawRecordId();
    }

    /**
     * Lấy một field an toàn.
     *
     * @return chuỗi rỗng nếu index không tồn tại hoặc field null
     */
    private static String getField(
            String[] fields,
            int index
    ) {
        if (
                index < 0
                        || index >= fields.length
                        || fields[index] == null
        ) {
            return "";
        }

        return fields[index].trim();
    }

    /**
     * Tính SHA-256 dạng hexadecimal viết thường.
     */
    private static String sha256(String input) {

        try {
            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");

            byte[] hashBytes = digest.digest(
                    input.getBytes(StandardCharsets.UTF_8)
            );

            return HexFormat.of().formatHex(
                    hashBytes
            );

        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    exception
            );
        }
    }
}