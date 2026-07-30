package com.network.producer.factory;

import com.network.producer.model.RawNetworkEvent;
import com.network.producer.model.SourceLine;
import com.network.producer.util.RawRecordIdGenerator;

import java.util.Objects;

/**
 * Chuyển một SourceLine thành RawNetworkEvent.
 *
 * <p>SourceLine là dữ liệu vừa được FileLogReader đọc
 * từ một dòng trong file nguồn.</p>
 *
 * <p>RawNetworkEvent là raw envelope chuẩn bị được
 * serialize thành JSON và gửi vào Kafka.</p>
 *
 * <p>Class này không đọc file, không tạo JSON
 * và không kết nối Kafka. Nó chỉ chuyển đổi model.</p>
 */
public final class RawNetworkEventFactory {

    /**
     * Phiên bản cấu trúc của raw JSON envelope.
     *
     * <p>Phiên bản này mô tả năm field:</p>
     *
     * <pre>
     * raw_record_id
     * schema_version
     * source_file
     * source_line
     * raw_payload
     * </pre>
     *
     * <p>Nó không phải phiên bản của layout 52 cột.</p>
     */
    public static final String SCHEMA_VERSION =
            "raw-envelope-v1";

    /**
     * Tạo RawNetworkEvent từ SourceLine.
     *
     * @param sourceLine dòng vừa được đọc từ file
     * @return raw event sẵn sàng serialize thành JSON
     */
    public RawNetworkEvent create(SourceLine sourceLine) {

        /*
         * Không thể tạo event nếu không có SourceLine.
         */
        Objects.requireNonNull(
                sourceLine,
                "sourceLine must not be null"
        );

        /*
         * sourceFile phải tồn tại vì nó được dùng để:
         *
         * 1. Truy vết message về file nguồn.
         * 2. Tạo rawRecordId.
         * 3. Xác định vị trí record gốc.
         */
        if (
                sourceLine.sourceFile() == null
                        || sourceLine.sourceFile().isBlank()
        ) {
            throw new IllegalArgumentException(
                    "sourceFile must not be blank"
            );
        }

        /*
         * Dòng đầu tiên trong file có số thứ tự 1.
         *
         * Vì vậy 0 và số âm là không hợp lệ.
         */
        if (sourceLine.lineNumber() < 1) {
            throw new IllegalArgumentException(
                    "lineNumber must be greater than or equal to 1"
            );
        }

        /*
         * rawData không được null.
         *
         * Tuy nhiên chuỗi rỗng "" vẫn được chấp nhận.
         * Bronze Job sau này sẽ phát hiện dòng rỗng
         * và route nó sang DLQ.
         */
        if (sourceLine.rawData() == null) {
            throw new IllegalArgumentException(
                    "rawData must not be null"
            );
        }

        /*
         * Tạo ID ổn định cho dòng nguồn.
         *
         * Công thức:
         *
         * SHA-256(
         *     sourceFile
         *     + ":"
         *     + lineNumber
         *     + ":"
         *     + rawData
         * )
         */
        String rawRecordId =
                RawRecordIdGenerator.generate(
                        sourceLine.sourceFile(),
                        sourceLine.lineNumber(),
                        sourceLine.rawData()
                );

        /*
         * Chuyển dữ liệu từ SourceLine sang RawNetworkEvent.
         *
         * Mapping:
         *
         * SourceLine.lineNumber
         *     -> RawNetworkEvent.sourceLine
         *
         * SourceLine.rawData
         *     -> RawNetworkEvent.rawPayload
         */
        return new RawNetworkEvent(
                rawRecordId,
                SCHEMA_VERSION,
                sourceLine.sourceFile(),
                sourceLine.lineNumber(),
                sourceLine.rawData()
        );
    }
}