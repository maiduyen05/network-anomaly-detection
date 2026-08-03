package com.network.preprocess.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Tạo SHA-256 ID ổn định.
 */
public final class DeterministicId {

    private DeterministicId() {
        /*
         * Utility class không được khởi tạo.
         */
    }

    public static String sha256(String input) {
        try {
            MessageDigest digest =
                    MessageDigest.getInstance("SHA-256");

            byte[] hash = digest.digest(
                    input.getBytes(StandardCharsets.UTF_8)
            );

            StringBuilder hex = new StringBuilder(
                    hash.length * 2
            );

            for (byte value : hash) {
                hex.append(
                        String.format("%02x", value)
                );
            }

            return hex.toString();

        } catch (NoSuchAlgorithmException exception) {
            /*
             * SHA-256 bắt buộc tồn tại trong Java runtime.
             * Nếu không tồn tại thì đây là lỗi môi trường,
             * không phải lỗi dữ liệu để đưa sang DLQ.
             */
            throw new IllegalStateException(
                    "SHA-256 is not available",
                    exception
            );
        }
    }
}