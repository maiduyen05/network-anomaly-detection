package com.network.preprocess.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Event đã vượt qua bước kiểm tra envelope và cấu trúc raw payload.
 * <p>Đối tượng này bảo đảm:</p>
 *
 * <ul>
 *     <li>rawRecordId là SHA-256 lowercase hợp lệ.</li>
 *     <li>schemaVersion đúng phiên bản được hỗ trợ.</li>
 *     <li>sourceFile không rỗng.</li>
 *     <li>sourceLine lớn hơn hoặc bằng 1.</li>
 *     <li>raw_payload có đúng 52 trường.</li>
 * </ul>
 *
 * <p>Các giá trị trong fieldValues vẫn là String.</p>
 */
public class ValidatedEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * ID truy vết về record nguồn.
     */
    private String rawRecordId;

    /**
     * Phiên bản JSON envelope.
     */
    private String schemaVersion;

    /**
     * Tên file nguồn.
     */
    private String sourceFile;

    /**
     * Số dòng trong file nguồn.
     */
    private Long sourceLine;

    /**
     * 52 giá trị được tách từ raw_payload.
     *
     * <p>Thứ tự của mảng được định nghĩa bởi RawLogField.</p>
     */
    private String[] fieldValues;

    /**
     * Constructor không tham số để Flink nhận diện Java POJO.
     */
    public ValidatedEvent() {
        this.fieldValues = new String[RawLogField.count()];
    }

    public ValidatedEvent(
            String rawRecordId,
            String schemaVersion,
            String sourceFile,
            Long sourceLine,
            String[] fieldValues
    ) {
        this.rawRecordId = rawRecordId;
        this.schemaVersion = schemaVersion;
        this.sourceFile = sourceFile;
        this.sourceLine = sourceLine;
        setFieldValues(fieldValues);
    }

    /**
     * Lấy một raw field theo tên thay vì dùng magic index.
     *
     * @param field field cần lấy
     * @return giá trị String nguyên bản; có thể là chuỗi rỗng
     */
    public String getField(RawLogField field) {
        Objects.requireNonNull(
                field,
                "field must not be null"
        );

        ensureFieldArrayIsValid();

        return fieldValues[field.getIndex()];
    }

    /**
     * Kiểm tra trạng thái nội bộ trước khi truy cập.
     */
    private void ensureFieldArrayIsValid() {
        if (
                fieldValues == null
                        || fieldValues.length != RawLogField.count()
        ) {
            throw new IllegalStateException(
                    "ValidatedEvent must contain exactly "
                            + RawLogField.count()
                            + " fields"
            );
        }
    }

    public String getRawRecordId() {
        return rawRecordId;
    }

    public void setRawRecordId(String rawRecordId) {
        this.rawRecordId = rawRecordId;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(String sourceFile) {
        this.sourceFile = sourceFile;
    }

    public Long getSourceLine() {
        return sourceLine;
    }

    public void setSourceLine(Long sourceLine) {
        this.sourceLine = sourceLine;
    }

    /**
     * Trả về bản sao để code bên ngoài không sửa trực tiếp mảng nội bộ.
     */
    public String[] getFieldValues() {
        return fieldValues == null
                ? null
                : fieldValues.clone();
    }

    /**
     * Lưu bản sao của mảng đầu vào.
     *
     * <p>ValidatedEvent chỉ chấp nhận đúng số field được định nghĩa
     * trong RawLogField.</p>
     */
    public void setFieldValues(String[] fieldValues) {
        Objects.requireNonNull(
                fieldValues,
                "fieldValues must not be null"
        );

        if (fieldValues.length != RawLogField.count()) {
            throw new IllegalArgumentException(
                    "fieldValues must contain exactly "
                            + RawLogField.count()
                            + " fields, actual="
                            + fieldValues.length
            );
        }

        this.fieldValues = fieldValues.clone();
    }

    /**
     * Không đưa 52 field vào toString().
     *
     * <p>Các field có thể chứa IMSI, MSISDN và thông tin thuê bao.
     * Chỉ ghi metadata an toàn phục vụ debug.</p>
     */
    @Override
    public String toString() {
        return "ValidatedEvent{" +
                "rawRecordId='" + rawRecordId + '\'' +
                ", schemaVersion='" + schemaVersion + '\'' +
                ", sourceFile='" + sourceFile + '\'' +
                ", sourceLine=" + sourceLine +
                ", fieldCount=" +
                (fieldValues == null ? 0 : fieldValues.length) +
                ", fieldValues='<redacted>'" +
                '}';
    }
}