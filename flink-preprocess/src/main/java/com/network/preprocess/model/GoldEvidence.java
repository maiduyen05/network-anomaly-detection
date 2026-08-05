package com.network.preprocess.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Evidence đi kèm một Gold sample.
 *
 * <p>Model chỉ sử dụng:</p>
 *
 * <ul>
 *     <li>x_cat[32][4]</li>
 *     <li>x_num[32][2]</li>
 * </ul>
 *
 * <p>Tuy nhiên hệ thống vẫn phải giữ các event nguồn để:</p>
 *
 * <ul>
 *     <li>Hiển thị chuỗi event trên UI.</li>
 *     <li>Điều tra nguyên nhân model dự đoán anomaly.</li>
 *     <li>Đối soát tensor với dữ liệu nguồn.</li>
 *     <li>Audit và tái tạo sample khi cần.</li>
 * </ul>
 *
 * <p>Class được viết theo chuẩn Flink POJO:</p>
 *
 * <ul>
 *     <li>Có constructor không tham số.</li>
 *     <li>Có getter và setter.</li>
 *     <li>Không giữ unmodifiable list.</li>
 * </ul>
 */
public class GoldEvidence implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Đúng 32 event đã tạo nên sample.
     */
    private List<GoldSequenceEvent> events =
            new ArrayList<>();

    /**
     * Constructor rỗng cho Jackson và Flink.
     */
    public GoldEvidence() {
    }

    public GoldEvidence(
            List<GoldSequenceEvent> events
    ) {
        setEvents(
                Objects.requireNonNull(
                        events,
                        "events must not be null"
                )
        );
    }

    public List<GoldSequenceEvent> getEvents() {
        return events;
    }

    /**
     * Luôn tạo ArrayList mới.
     *
     * <p>Không giữ trực tiếp List.of() hoặc danh sách không thể sửa,
     * vì Flink có thể cần copy object khi shuffle hoặc checkpoint.</p>
     */
    public void setEvents(
            List<GoldSequenceEvent> events
    ) {
        if (events == null) {
            this.events = new ArrayList<>();
        } else {
            this.events = new ArrayList<>(events);
        }
    }

    /**
     * Accessor dạng record để code nghiệp vụ dễ đọc.
     */
    public List<GoldSequenceEvent> events() {
        return events;
    }
}