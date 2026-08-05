package com.network.preprocess.model;

import java.io.Serializable;

/**
 * Nguyên nhân IdentityResolvedEvent không thể trở thành SilverEvent.
 */
public enum UnsupportedEventReason implements Serializable {

    /**
     * EVENT_ID null, rỗng hoặc chỉ chứa ký tự phân cách.
     */
    MISSING_EVENT_ID,

    /**
     * EVENT_ID có giá trị nhưng không nằm trong model catalog.
     */
    UNSUPPORTED_EVENT_ID,

    /**
     * EVENT_RESULT null, rỗng hoặc chỉ chứa khoảng trắng.
     */
    MISSING_EVENT_RESULT,

    /**
     * EVENT_RESULT có giá trị nhưng không nằm trong
     * vocabulary reject/success của feature contract.
     */
    UNSUPPORTED_EVENT_RESULT
}