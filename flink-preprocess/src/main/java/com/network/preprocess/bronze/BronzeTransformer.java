package com.network.preprocess.bronze;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.network.preprocess.model.*;
import com.network.preprocess.operator.TimestampNormalizer;
import com.network.preprocess.operator.TypeCastOperator;
import com.network.preprocess.parser.*;
import com.network.preprocess.util.DeterministicId;
import com.network.preprocess.validation.SchemaValidator;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

/**
 * Logic biến Kafka raw record thành Bronze event.
 * 1. KafkaRawRecord
 * 2. Kiểm tra value null/rỗng
 * 3. Parse JSON thành RawNetworkEvent
 * 4. Kiểm tra schema version, raw_record_id, source_file, source_line
 * 5. Parse raw_payload thành 52 field
 * 6. Chuẩn hóa EVENT_TIME thành UTC ISO-8601
 * 7. Ép kiểu numeric, giữ null nếu rỗng, đi DLQ
 * 8. Tạo BronzeEvent với Kafka metadata để Silver deduplicate
 * <p>Class này không phụ thuộc Flink API nên có thể unit test trực tiếp.</p>
 */
public final class BronzeTransformer implements Serializable {

    private static final String DLQ_SCHEMA_VERSION =
            "bronze-dlq-v1";

    private final String envelopeSchemaVersion;
    private final String outputSchemaVersion;

    private final JsonEventParser jsonEventParser;
    private final SchemaValidator schemaValidator;
    private final RawLogLineParser rawLogLineParser;
    private final TimestampNormalizer timestampNormalizer;
    private final TypeCastOperator typeCastOperator;

    public BronzeTransformer(
            String envelopeSchemaVersion,
            String outputSchemaVersion,
            JsonEventParser jsonEventParser,
            SchemaValidator schemaValidator,
            RawLogLineParser rawLogLineParser,
            TimestampNormalizer timestampNormalizer,
            TypeCastOperator typeCastOperator
    ) {
        this.envelopeSchemaVersion = envelopeSchemaVersion;
        this.outputSchemaVersion = outputSchemaVersion;
        this.jsonEventParser = jsonEventParser;
        this.schemaValidator = schemaValidator;
        this.rawLogLineParser = rawLogLineParser;
        this.timestampNormalizer = timestampNormalizer;
        this.typeCastOperator = typeCastOperator;
    }

    /**
     * Transform một Kafka record.
     *
     * @param kafkaRecord record cùng Kafka metadata
     * @param ingestTimeMillis processing time tại Bronze
     * @return valid result hoặc invalid result
     */
    public BronzeTransformResult transform(
            KafkaRawRecord kafkaRecord,
            long ingestTimeMillis
    ) {
        String ingestTime =
                Instant.ofEpochMilli(ingestTimeMillis).toString();

        BronzeSourceMetadata kafkaOnlySource =
                createSourceMetadata(
                        kafkaRecord,
                        null,
                        null,
                        ingestTime
                );

        /*
         * Kafka value null là dữ liệu không hợp lệ nhưng không được
         * làm Kafka source throw exception.
         */
        if (kafkaRecord.value() == null) {
            return invalid(
                    kafkaRecord,
                    null,
                    BronzeErrorCode.NULL_KAFKA_VALUE,
                    "Kafka record value is null",
                    kafkaOnlySource,
                    ingestTime
            );
        }

        RawNetworkEvent envelope;

        try {
            /*
             * Parse envelope JSON:
             * raw_record_id, schema_version, source_file,
             * source_line và raw_payload.
             */
            envelope = jsonEventParser.parse(
                    kafkaRecord.value()
            );

        } catch (JsonProcessingException exception) {
            return invalid(
                    kafkaRecord,
                    null,
                    BronzeErrorCode.INVALID_ENVELOPE_JSON,
                    "Kafka value is not a valid raw envelope JSON",
                    kafkaOnlySource,
                    ingestTime
            );
        }

        BronzeSourceMetadata fullSource =
                createSourceMetadata(
                        kafkaRecord,
                        envelope.sourceFile(),
                        envelope.sourceLine(),
                        ingestTime
                );

        try {
            /*
             * Validate envelope trước khi đọc raw payload.
             */
            schemaValidator.validateOrThrow(
                    envelope,
                    envelopeSchemaVersion
            );

            /*
             * Parse chính xác 52 trường.
             */
            Map<String, String> fields =
                    rawLogLineParser.parse(
                            envelope.rawPayload()
                    );

            /*
             * EVENT_TIME bắt buộc hợp lệ theo chính sách
             * đã chốt ở Checkpoint 4.
             */
            String eventTime =
                    timestampNormalizer
                            .normalizeRequiredToUtc(
                                    fields.get("EVENT_TIME")
                            );

            /*
             * Field numeric rỗng được giữ null.
             * Field sai format hoặc âm đi DLQ.
             */
            Long durationMs =
                    typeCastOperator
                            .parseNullableNonNegativeLong(
                                    fields.get("DURATION"),
                                    BronzeErrorCode.INVALID_DURATION
                            );

            Integer requestRetries =
                    typeCastOperator
                            .parseNullableNonNegativeInteger(
                                    fields.get("REQUEST_RETRIES"),
                                    BronzeErrorCode
                                            .INVALID_REQUEST_RETRIES
                            );

            Integer pagingAttempts =
                    typeCastOperator
                            .parseNullableNonNegativeInteger(
                                    fields.get("PAGING_ATTEMPTS"),
                                    BronzeErrorCode
                                            .INVALID_PAGING_ATTEMPTS
                            );

            BronzeEvent bronzeEvent = new BronzeEvent(
                    outputSchemaVersion,
                    envelope.rawRecordId(),

                    trimToNull(fields.get("EVENT_ID")),
                    trimPreserveEmpty(fields.get("EVENT_RESULT")),
                    durationMs,
                    requestRetries,
                    pagingAttempts,

                    eventTime,
                    "source",

                    trimToNull(fields.get("MSISDN")),
                    trimToNull(fields.get("IMSI")),
                    trimToNull(fields.get("MTMSI")),
                    trimToNull(fields.get("IMEISV")),
                    trimToNull(fields.get("MMEGI")),
                    trimToNull(fields.get("MMEC")),
                    trimToNull(fields.get("SUB_CAUSE_CODE")),
                    trimToNull(fields.get("MSC")),
                    trimToNull(fields.get("TAC")),
                    trimToNull(fields.get("ECI")),
                    trimToNull(fields.get("SGW")),
                    trimToNull(fields.get("PDN_PGW")),

                    fields,
                    fullSource
            );

            return BronzeTransformResult.valid(
                    bronzeEvent
            );

        } catch (BronzeDataException exception) {
            /*
             * Chỉ lỗi dữ liệu dự kiến mới được chuyển sang DLQ.
             *
             * Không catch RuntimeException ở đây. NullPointerException,
             * lỗi code hoặc lỗi môi trường phải làm job fail để được
             * phát hiện và phục hồi đúng cách.
             */
            return invalid(
                    kafkaRecord,
                    envelope.rawRecordId(),
                    exception.getErrorCode(),
                    exception.getMessage(),
                    fullSource,
                    ingestTime
            );
        }
    }

    private BronzeTransformResult invalid(
            KafkaRawRecord kafkaRecord,
            String rawRecordId,
            BronzeErrorCode errorCode,
            String safeMessage,
            BronzeSourceMetadata source,
            String failedAt
    ) {
        /*
         * Nếu chưa parse được raw_record_id thì tạo fallback từ
         * Kafka coordinates. Cùng một Kafka record luôn có cùng ID.
         */
        String effectiveRawRecordId =
                rawRecordId != null
                        ? rawRecordId
                        : DeterministicId.sha256(
                                kafkaRecord.topic()
                                        + ":"
                                        + kafkaRecord.partition()
                                        + ":"
                                        + kafkaRecord.offset()
                        );

        /*
         * Thêm errorCode để một record có các loại lỗi khác nhau
         * không vô tình dùng cùng dlqId.
         */
        String dlqId = DeterministicId.sha256(
                "bronze-dlq:"
                        + kafkaRecord.topic()
                        + ":"
                        + kafkaRecord.partition()
                        + ":"
                        + kafkaRecord.offset()
                        + ":"
                        + errorCode.name()
        );

        BronzeDlqRecord dlqRecord =
                new BronzeDlqRecord(
                        DLQ_SCHEMA_VERSION,
                        dlqId,
                        effectiveRawRecordId,
                        errorCode,
                        safeMessage,
                        failedAt,
                        kafkaRecord.value(),
                        source
                );

        return BronzeTransformResult.invalid(
                dlqRecord
        );
    }

    private BronzeSourceMetadata createSourceMetadata(
            KafkaRawRecord record,
            String sourceFile,
            Long sourceLine,
            String ingestTime
    ) {
        /*
         * Kafka timestamp -1 nghĩa là Kafka không cung cấp timestamp.
         */
        String kafkaTimestamp =
                record.kafkaTimestamp() < 0
                        ? null
                        : Instant.ofEpochMilli(
                                record.kafkaTimestamp()
                        ).toString();

        return new BronzeSourceMetadata(
                sourceFile,
                sourceLine,
                record.topic(),
                record.partition(),
                record.offset(),
                kafkaTimestamp,
                ingestTime
        );
    }

    private String trimToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        return value.trim();
    }

    private String trimPreserveEmpty(String value) {
        if (value == null) {
                return null;
        }

        return value.trim();
        }
}