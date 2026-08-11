package com.network.preprocess.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Hai tensor đầu vào trực tiếp của model.
 *
 * <pre>
 * x_cat: INT64[32][4]
 * x_num: FLOAT32[32][2]
 * </pre>
 *
 * <p>Dùng primitive array để giữ đúng datatype của model:</p>
 *
 * <ul>
 *     <li>Java long tương ứng INT64.</li>
 *     <li>Java float tương ứng FLOAT32.</li>
 * </ul>
 *
 * <p>
 * Tên JSON được khai báo rõ bằng @JsonProperty thay vì để Jackson
 * tự suy luận từ getter getXCat()/getXNum().
 * </p>
 *
 * <p>
 * Điều này đặc biệt quan trọng vì chữ "X" đứng trước "Cat"/"Num"
 * có thể khiến naming strategy tạo ra "xcat"/"xnum", trong khi
 * model contract yêu cầu chính xác:
 * </p>
 *
 * <pre>
 * {
 *   "x_cat": [...],
 *   "x_num": [...]
 * }
 * </pre>
 */
public class GoldModelInput implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Categorical tensor.
     *
     * Shape bắt buộc theo feature contract:
     *
     * INT64[32][4]
     */
    private long[][] xCat;

    /**
     * Numeric tensor.
     *
     * Shape bắt buộc theo feature contract:
     *
     * FLOAT32[32][2]
     */
    private float[][] xNum;

    /**
     * Constructor rỗng bắt buộc để Jackson và Flink
     * có thể khởi tạo object khi deserialize.
     */
    public GoldModelInput() {
    }

    /**
     * Constructor được GoldFeatureEncoder sử dụng
     * sau khi encode sequence thành tensor.
     */
    public GoldModelInput(
            long[][] xCat,
            float[][] xNum
    ) {
        this.xCat = xCat;
        this.xNum = xNum;
    }

    /**
     * Ép tên JSON thành "x_cat".
     *
     * Không để Jackson tự chuyển getXCat()
     * vì kết quả có thể trở thành "xcat".
     */
    @JsonProperty("x_cat")
    public long[][] getXCat() {
        return xCat;
    }

    /**
     * Dùng cùng @JsonProperty với getter để
     * deserialize JSON "x_cat" trở lại Java object.
     */
    @JsonProperty("x_cat")
    public void setXCat(long[][] xCat) {
        this.xCat = xCat;
    }

    /**
     * Ép tên JSON thành "x_num".
     *
     * Không để Jackson tự chuyển getXNum()
     * vì kết quả có thể trở thành "xnum".
     */
    @JsonProperty("x_num")
    public float[][] getXNum() {
        return xNum;
    }

    /**
     * Dùng cùng @JsonProperty với getter để
     * deserialize JSON "x_num" trở lại Java object.
     */
    @JsonProperty("x_num")
    public void setXNum(float[][] xNum) {
        this.xNum = xNum;
    }
}