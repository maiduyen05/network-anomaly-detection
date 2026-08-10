package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.SilverEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Chuyển SilverEvent thành event trung gian của tầng Gold.
 *
 * <p>
 * Mapper chỉ lấy và project dữ liệu nguồn.
 * Mapper không encode category thành vocabulary ID.
 * </p>
 *
 * <p>
 * Các field phục vụ model và các field phục vụ evidence
 * được tách biệt:
 * </p>
 *
 * <ul>
 *     <li>
 *         Model feature source:
 *         luôn giữ những field bắt buộc cho GoldFeatureEncoder.
 *     </li>
 *     <li>
 *         Evidence/display:
 *         được lựa chọn thông qua gold.evidence.fields.
 *     </li>
 * </ul>
 */
public final class GoldSequenceEventMapper
        implements MapFunction<
                SilverEvent,
                GoldSequenceEvent> {

    /**
     * Projector chịu trách nhiệm chọn các evidence field
     * theo cấu hình:
     *
     * <pre>
     * gold:
     *   evidence:
     *     fields:
     * </pre>
     */
    private final GoldEvidenceFieldProjector
            evidenceFieldProjector;

    /**
     * Mapper nhận danh sách evidence từ GoldJobConfig.
     *
     * <p>
     * Mapper không tự đọc application.yaml.
     * Configuration phải được inject từ GoldJob.
     * </p>
     *
     * @param evidenceFields danh sách evidence field được phép output
     */
    public GoldSequenceEventMapper(
            List<String> evidenceFields
    ) {

        this.evidenceFieldProjector =
                new GoldEvidenceFieldProjector(
                        Objects.requireNonNull(
                                evidenceFields,
                                "evidenceFields must not be null"
                        )
                );
    }

    /**
     * Chuyển một SilverEvent thành GoldSequenceEvent.
     */
    @Override
    public GoldSequenceEvent map(
            SilverEvent silverEvent
    ) {

        Objects.requireNonNull(
                silverEvent,
                "silverEvent must not be null"
        );

        /*
         * =========================================================
         * BƯỚC 1: LẤY RAW FIELDS
         * =========================================================
         */

        Map<String, String> rawFields =
                Objects.requireNonNull(
                        silverEvent.rawFields(),
                        "silverEvent.rawFields must not be null"
                );

        /*
         * CAUSE_CODE và SUB_CAUSE_CODE là feature source
         * bắt buộc của model hiện tại.
         *
         * Empty string "" vẫn là category hợp lệ.
         */
        String normalizedCauseCode =
                requireRawCategory(
                        rawFields,
                        "CAUSE_CODE"
                );

        String subCauseCode =
                requireRawCategory(
                        rawFields,
                        "SUB_CAUSE_CODE"
                );


        /*
         * =========================================================
         * BƯỚC 2: TẠO GOLD EVENT
         * =========================================================
         */

        GoldSequenceEvent goldEvent =
                new GoldSequenceEvent();


        /*
         * =========================================================
         * BƯỚC 3: IDENTITY
         * =========================================================
         *
         * Identity đã được Silver resolve.
         */

        goldEvent.setUeKey(
                silverEvent.ueKey()
        );

        goldEvent.setImsi(
                silverEvent.imsi()
        );


        /*
         * =========================================================
         * BƯỚC 4: CATEGORICAL MODEL SOURCES
         * =========================================================
         *
         * Giữ nguyên dạng String.
         *
         * Không encode vocabulary tại Mapper.
         * GoldFeatureEncoder sẽ thực hiện bước encode.
         */

        goldEvent.setEventId(
                silverEvent.eventId()
        );

        goldEvent.setEventResult(
                silverEvent
                        .eventResult()
                        .wireValue()
        );

        goldEvent.setNormalizedCauseCode(
                normalizedCauseCode
        );

        goldEvent.setSubCauseCode(
                subCauseCode
        );


        /*
         * =========================================================
         * BƯỚC 5: NUMERIC MODEL SOURCES
         * =========================================================
         *
         * Chưa normalize ở đây.
         *
         * GoldFeatureEncoder + NumericFeatureEncoder
         * sẽ clip và normalize theo feature-contract.
         */

        goldEvent.setDurationMs(
                silverEvent.durationMs()
        );

        goldEvent.setRequestRetries(
                silverEvent.requestRetries()
        );


        /*
         * =========================================================
         * BƯỚC 6: EVENT TIME
         * =========================================================
         *
         * GoldSequenceEvent lưu event time dưới dạng epoch millis
         * để thân thiện với Flink POJO serialization.
         */

        goldEvent.setEventTimeEpochMs(
                Instant.parse(
                        silverEvent.eventTime()
                ).toEpochMilli()
        );


        /*
         * =========================================================
         * BƯỚC 7: FEATURE SOURCE FIELDS
         * =========================================================
         *
         * Chỉ giữ những raw field mà feature contract
         * hiện tại thực sự cần.
         *
         * Không copy toàn bộ 52 raw fields vì mỗi Gold sample
         * chứa 32 event. Nếu giữ toàn bộ rawFields ở đây thì
         * lượng dữ liệu dư sẽ bị nhân lên rất nhiều.
         */

        Map<String, String> featureSourceFields =
                new LinkedHashMap<>();

        featureSourceFields.put(
                "CAUSE_CODE",
                normalizedCauseCode
        );

        featureSourceFields.put(
                "SUB_CAUSE_CODE",
                subCauseCode
        );

        goldEvent.setFeatureSourceFields(
                featureSourceFields
        );


        /*
         * =========================================================
         * BƯỚC 8: CONFIGURABLE EVIDENCE
         * =========================================================
         *
         * Những field này phục vụ:
         *
         * - UI;
         * - audit;
         * - debug;
         * - điều tra anomaly;
         * - giải thích kết quả model.
         *
         * Danh sách field được quyết định bởi:
         *
         * gold.evidence.fields
         *
         * Thêm/bớt field evidence KHÔNG làm thay đổi:
         *
         * - sequence;
         * - x_cat;
         * - x_num;
         * - vocabulary;
         * - numeric normalization.
         */

        Map<String, String> displayFields =
                evidenceFieldProjector.project(
                        silverEvent
                );

        goldEvent.setDisplayFields(
                displayFields
        );


        /*
         * =========================================================
         * BƯỚC 9: QUALITY EVIDENCE
         * =========================================================
         *
         * Quality metadata mô tả Silver đã xử lý event như thế nào.
         *
         * Phần này tách riêng với configurable display evidence
         * vì nó phục vụ audit/chẩn đoán pipeline.
         */

        Map<String, String> qualityFields =
                new LinkedHashMap<>();

        if (silverEvent.quality() != null) {

            if (silverEvent
                    .quality()
                    .identityResolutionSource() != null) {

                qualityFields.put(
                        "identity_resolution_source",
                        silverEvent
                                .quality()
                                .identityResolutionSource()
                                .name()
                                .toLowerCase(
                                        Locale.ROOT
                                )
                );
            }

            qualityFields.put(
                    "event_id_changed",
                    Boolean.toString(
                            silverEvent
                                    .quality()
                                    .eventIdChanged()
                    )
            );

            qualityFields.put(
                    "event_result_changed",
                    Boolean.toString(
                            silverEvent
                                    .quality()
                                    .eventResultChanged()
                    )
            );

            qualityFields.put(
                    "event_result_recognized",
                    Boolean.toString(
                            silverEvent
                                    .quality()
                                    .eventResultRecognized()
                    )
            );

            /*
             * Map chỉ chứa String nên warning list
             * được nối thành một String.
             *
             * Ví dụ:
             *
             * warning1|warning2
             */
            if (silverEvent
                    .quality()
                    .warnings() != null) {

                qualityFields.put(
                        "warnings",
                        String.join(
                                "|",
                                silverEvent
                                        .quality()
                                        .warnings()
                        )
                );
            }
        }

        goldEvent.setQualityFields(
                qualityFields
        );


        /*
         * =========================================================
         * BƯỚC 10: DETERMINISTIC SOURCE ORDER
         * =========================================================
         *
         * Khi hai event có cùng event_time,
         * rawRecordId được dùng làm tie-breaker
         * để sequence luôn có thứ tự xác định.
         */

        goldEvent.setSourceOrderKey(
                silverEvent.rawRecordId()
        );

        return goldEvent;
    }


    /**
     * Lấy một raw categorical field bắt buộc.
     *
     * <p>
     * Phân biệt hai trường hợp:
     * </p>
     *
     * <ul>
     *     <li>
     *         Không có field hoặc value = null:
     *         vi phạm contract.
     *     </li>
     *     <li>
     *         Value = "":
     *         vẫn có thể là category hợp lệ.
     *     </li>
     * </ul>
     */
    private static String requireRawCategory(
            Map<String, String> rawFields,
            String fieldName
    ) {

        Objects.requireNonNull(
                rawFields,
                "rawFields must not be null"
        );

        if (!rawFields.containsKey(fieldName)
                || rawFields.get(fieldName) == null) {

            throw new IllegalStateException(
                    "Silver rawFields must contain "
                            + fieldName
            );
        }

        return rawFields.get(
                fieldName
        );
    }
}