package com.network.preprocess.validation;

import com.network.preprocess.model.BronzeErrorCode;
import com.network.preprocess.model.RawNetworkEvent;
import com.network.preprocess.parser.BronzeDataException;

import java.io.Serializable;

/**
 * Kiểm tra contract của raw JSON envelope.
 *
 * <p>Class này chạy sau JsonEventParser. Điều đó có nghĩa JSON đã đúng
 * cú pháp, nhưng nội dung bên trong vẫn có thể bị thiếu hoặc sai.</p>
 *
 * <p>Ví dụ:</p>
 *
 * <ul>
 *     <li>Thiếu raw_record_id.</li>
 *     <li>Thiếu schema_version.</li>
 *     <li>schema_version không được hỗ trợ.</li>
 *     <li>Thiếu source_file.</li>
 *     <li>source_line nhỏ hơn 1.</li>
 * </ul>
 */
public final class SchemaValidator implements Serializable {

    /**
     * Kiểm tra envelope và ném BronzeDataException nếu không hợp lệ.
     *
     * @param event RawNetworkEvent đã được parse từ JSON
     * @param expectedSchemaVersion schema version Bronze đang hỗ trợ
     * @throws BronzeDataException nếu dữ liệu envelope không hợp lệ
     */
    public void validateOrThrow(
            RawNetworkEvent event,
            String expectedSchemaVersion
    ) throws BronzeDataException {

        /*
         * expectedSchemaVersion đến từ application.yaml.
         *
         * Nếu cấu hình này null hoặc rỗng thì đó là lỗi cấu hình hệ thống,
         * không phải lỗi của một Kafka record. Vì vậy ta ném
         * IllegalArgumentException để job fail sớm.
         */
        if (expectedSchemaVersion == null
                || expectedSchemaVersion.isBlank()) {

            throw new IllegalArgumentException(
                    "expectedSchemaVersion must not be blank"
            );
        }

        /*
         * JSON literal "null" là JSON đúng cú pháp.
         *
         * Jackson có thể parse nó thành event == null. Tuy nhiên nó
         * không phải một envelope hợp lệ nên phải đi DLQ.
         */
        if (event == null) {
            throw new BronzeDataException(
                    BronzeErrorCode.INVALID_ENVELOPE_SCHEMA,
                    "Raw envelope must be a JSON object"
            );
        }

        /*
         * schema_version là field bắt buộc.
         *
         * Nếu field bị thiếu hoặc chỉ chứa khoảng trắng, envelope không
         * đủ cấu trúc để xác định contract.
         */
        if (isBlank(event.schemaVersion())) {
            throw new BronzeDataException(
                    BronzeErrorCode.INVALID_ENVELOPE_SCHEMA,
                    "Raw envelope is missing schema_version"
            );
        }

        /*
         * Field tồn tại nhưng version không bằng version Bronze hỗ trợ.
         *
         * Đây khác với INVALID_ENVELOPE_SCHEMA:
         *
         * - INVALID_ENVELOPE_SCHEMA: thiếu hoặc sai cấu trúc.
         * - UNSUPPORTED_ENVELOPE_SCHEMA_VERSION: có version nhưng
         *   application hiện tại không hỗ trợ version đó.
         */
        if (!expectedSchemaVersion.equals(event.schemaVersion())) {
            throw new BronzeDataException(
                    BronzeErrorCode
                            .UNSUPPORTED_ENVELOPE_SCHEMA_VERSION,
                    "Raw envelope schema version is not supported"
            );
        }

        /*
         * raw_record_id dùng để truy vết và làm Kafka output key.
         * Vì vậy field này bắt buộc phải có giá trị.
         */
        if (isBlank(event.rawRecordId())) {
            throw new BronzeDataException(
                    BronzeErrorCode.INVALID_ENVELOPE_SCHEMA,
                    "Raw envelope is missing raw_record_id"
            );
        }

        /*
         * source_file cho biết record được đọc từ file log nào.
         * Không đưa giá trị source_file thật vào error message để tránh
         * sao chép dữ liệu nguồn không cần thiết.
         */
        if (isBlank(event.sourceFile())) {
            throw new BronzeDataException(
                    BronzeErrorCode.INVALID_ENVELOPE_SCHEMA,
                    "Raw envelope is missing source_file"
            );
        }

        /*
         * source_line dùng Long nên có thể nhận null nếu JSON thiếu field.
         */
        if (event.sourceLine() == null) {
            throw new BronzeDataException(
                    BronzeErrorCode.INVALID_ENVELOPE_SCHEMA,
                    "Raw envelope is missing source_line"
            );
        }

        /*
         * Số dòng trong file bắt đầu từ 1.
         *
         * Giá trị 0 hoặc âm không thể đại diện cho một dòng hợp lệ.
         */
        if (event.sourceLine() < 1) {
            throw new BronzeDataException(
                    BronzeErrorCode.INVALID_ENVELOPE_SCHEMA,
                    "Raw envelope source_line must be positive"
            );
        }

        /*
         * Không kiểm tra rawPayload rỗng tại đây.
         *
         * RawLogLineParser chịu trách nhiệm đó và sẽ trả về error code
         * EMPTY_RAW_PAYLOAD. Nhờ vậy ta phân biệt được:
         *
         * - Envelope sai cấu trúc.
         * - Envelope đúng cấu trúc nhưng payload rỗng.
         */
    }

    /**
     * Kiểm tra String null, rỗng hoặc chỉ chứa khoảng trắng.
     */
    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}