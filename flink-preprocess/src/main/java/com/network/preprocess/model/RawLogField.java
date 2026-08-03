package com.network.preprocess.model;

/**
 * Danh sách 52 trường trong raw_payload (schemas/source/raw-log-line-v1.json)
 *
 * <p>Mỗi enum giữ index thực tế của trường trong mảng sau khi
 * raw_payload được tách bằng dấu chấm phẩy.</p>
 *
 * <p>Không dùng trực tiếp ordinal() vì index là một phần của
 * data contract. Nếu sau này enum bị sắp xếp lại, index vẫn phải
 * được xem xét và cập nhật có chủ đích.</p>
 */
public enum RawLogField {

    EVENT_ID(0),
    EVENT_RESULT(1),
    DURATION(2),
    REQUEST_RETRIES(3),
    SUB_TYPE(4),
    MSISDN(5),
    IMSI(6),
    MTMSI(7),
    IMEISV(8),
    MMEGI(9),
    MMEC(10),
    TAC(11),
    ECI(12),
    SGW(13),
    SGSN(14),
    L_CAUSE_PROT_TYPE(15),
    CAUSE_CODE(16),
    SUB_CAUSE_CODE(17),
    APN(18),
    PDN_DEFAULT_BEARER_ID(19),
    PDN_PAA(20),
    PDN_PGW(21),
    ORIGINATING_CAUSE_PROT_TYPE(22),
    ORIGINATING_CAUSE_CODE(23),
    CSG_ID(24),
    OLD_MTMSI(25),
    OLD_TAC(26),
    OLD_MMEGI(27),
    OLD_MMEC(28),
    OLD_ECI(29),
    OLD_SGW(30),
    OLD_SGSN(31),
    MSC(32),
    TARGET_LAC(33),
    LAC(34),
    RAC(35),
    CI(36),
    HANDOVER_NODE_ROLE(37),
    HANDOVER_RAT_CHANGE_TYPE(38),
    HANDOVER_SGW_CHANGE_TYPE(39),
    TARGET_RNC_ID(40),
    TARGET_MACRO_ENODEB_ID(41),
    SRVCC_TYPE(42),
    CS_FALLBACK_SERVICE_TYPE(43),
    CSFB_TRIGGERED(44),
    L_SERVICE_REQ_TRIGGER(45),
    COMBINED_TAU_TYPE(46),
    DETACH_TRIGGER(47),
    EVENT_TIME(48),
    PAGING_ATTEMPTS(49),
    UE_REQUESTED_APN(50),
    DATE_HOUR(51);

    /**
     * Vị trí của field trong raw_payload sau khi tách.
     */
    private final int index;

    RawLogField(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    /**
     * Tổng số trường được định nghĩa bởi schema hiện tại.
     */
    public static int count() {
        return values().length;
    }
}