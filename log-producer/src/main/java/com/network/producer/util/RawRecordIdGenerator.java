package com.network.producer.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Tạo ID (định danh 1 record cụ thể) từng dòng log nguồn theo công thức: 
 * SHA-256(sourceFile + ":" + lineNumber + ":" + rawData)
 * Không đưa ingestAt vào công thức vì mỗi lần producer đọc lại tạo ra 1 thời gian khác nhau 
 * SHA-256 tạo ra chuỗi 64 ký tự hexadecimal viết thường. --> không gây ra trùng lặp ID, producer không bị đọc lại cùng 1 dòng khi lỗi
 * Tại sao lại dùng SHA-256?
 * Cùng 1 sự kiện ở 1 vị trí luôn tạo ra 1 ID duy nhất (khác 1 chữ ID đã khác hoàn toàn), khác với UUID tạo lại từ 1 dòng vẫn sinh ID mới 
 * Lưu ý: đổi tên file sẽ làm ID thay đổi, nên srcfile  không nên là đường dẫn cứng
 */

public final class RawRecordIdGenerator {

    private static final String HASH_ALGORITHM = "SHA-256";
    private static final String SEPARATOR = ":";

    private RawRecordIdGenerator() {
    }

    /**
     * Tạo raw record ID.
     *
     * @param sourceFile tên file nguồn
     * @param lineNumber số dòng, bắt đầu từ 1
     * @param rawData nội dung dòng nguồn
     * @return SHA-256 dạng 64 ký tự hexadecimal
     */
    public static String generate(
            String sourceFile,
            long lineNumber,
            String rawData
    ) {
        Objects.requireNonNull(
                sourceFile,
                "sourceFile must not be null"
        );

        Objects.requireNonNull(
                rawData,
                "rawData must not be null"
        );

        if (lineNumber < 1) {
            throw new IllegalArgumentException(
                    "lineNumber must be greater than or equal to 1"
            );
        }

        String input = sourceFile
                + SEPARATOR
                + lineNumber
                + SEPARATOR
                + rawData;

        try {
            MessageDigest digest =
                    MessageDigest.getInstance(HASH_ALGORITHM);

            byte[] hash = digest.digest(
                    input.getBytes(StandardCharsets.UTF_8)
            );

            return HexFormat.of().formatHex(hash);

        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    exception
            );
        }
    }
}