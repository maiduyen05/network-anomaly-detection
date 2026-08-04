package com.network.preprocess.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Mô tả nguồn gốc và chất lượng chuẩn hóa của SilverEvent.
 *
 * <p>Không dùng các flag này làm thay đổi danh tính của event.
 * Chúng phục vụ audit, monitoring và phân tích chất lượng dữ liệu.</p>
 */
public record SilverQuality(
        IdentityResolutionSource identityResolutionSource,
        boolean eventIdChanged,
        boolean eventResultChanged,
        boolean eventResultRecognized,
        List<String> warnings
) implements Serializable {

    public SilverQuality {
        Objects.requireNonNull(
                identityResolutionSource,
                "identityResolutionSource must not be null"
        );

        /*
         * Không giữ List mutable do caller truyền vào.
         */
        warnings = warnings == null
            ? new ArrayList<>()
            : new ArrayList<>(warnings);
    }
    
    @Override
    public List<String> warnings() {
        return Collections.unmodifiableList(warnings);
        }
}