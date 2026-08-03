package com.network.preprocess.model;

/**
 * Danh mục lỗi dữ liệu mà Bronze được phép route sang DLQ.
 *
 * <p>Không thêm MISSING_IMSI hoặc UNSUPPORTED_EVENT_ID ở đây,
 * vì hai lỗi đó thuộc tầng Silver.</p>
 */
public enum BronzeErrorCode {
    NULL_KAFKA_VALUE,
    INVALID_ENVELOPE_JSON,
    INVALID_ENVELOPE_SCHEMA,
    UNSUPPORTED_ENVELOPE_SCHEMA_VERSION,
    EMPTY_RAW_PAYLOAD,
    WRONG_RAW_FIELD_COUNT,
    INVALID_EVENT_TIME,
    INVALID_DURATION,
    INVALID_REQUEST_RETRIES,
    INVALID_PAGING_ATTEMPTS
}