package com.network.preprocess.parser;

import com.network.preprocess.model.BronzeErrorCode;

import java.util.Objects;

/**
 * Exception biểu diễn lỗi dữ liệu đã dự đoán trước ở tầng Bronze.
 *
 * <p>Ví dụ:</p>
 *
 * <ul>
 *     <li>Envelope thiếu field bắt buộc.</li>
 *     <li>Raw payload không đủ 52 field.</li>
 *     <li>EVENT_TIME sai định dạng.</li>
 *     <li>DURATION là số âm.</li>
 * </ul>
 *
 * <p>BronzeTransformer bắt exception này và chuyển record sang DLQ.</p>
 *
 * <p>Không dùng exception này cho:</p>
 *
 * <ul>
 *     <li>NullPointerException do lỗi code.</li>
 *     <li>Lỗi kết nối Kafka.</li>
 *     <li>Lỗi Flink runtime.</li>
 *     <li>Lỗi checkpoint.</li>
 * </ul>
 */
public final class BronzeDataException extends Exception {

    /*
     * Mã lỗi dùng để phân loại DLQ record.
     */
    private final BronzeErrorCode errorCode;

    /**
     * Tạo lỗi dữ liệu Bronze.
     *
     * @param errorCode mã lỗi cố định
     * @param safeMessage message an toàn, không chứa raw payload/identity
     */
    public BronzeDataException(
            BronzeErrorCode errorCode,
            String safeMessage
    ) {
        /*
         * Gửi safeMessage lên constructor của Exception.
         * Sau đó có thể đọc lại bằng exception.getMessage().
         */
        super(safeMessage);

        /*
         * errorCode null là lỗi lập trình nên không được chấp nhận.
         */
        this.errorCode = Objects.requireNonNull(
                errorCode,
                "errorCode must not be null"
        );
    }

    /**
     * Trả về mã lỗi để BronzeTransformer ghi vào DLQ record.
     */
    public BronzeErrorCode getErrorCode() {
        return errorCode;
    }
}