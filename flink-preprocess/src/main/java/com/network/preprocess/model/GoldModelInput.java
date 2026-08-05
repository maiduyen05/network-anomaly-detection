package com.network.preprocess.model;

import java.io.Serializable;

/**
 * Hai tensor đầu vào trực tiếp của model.
 *
 * <pre>
 * xCat: INT64[32][4]
 * xNum: FLOAT32[32][2]
 * </pre>
 *
 * <p>Dùng primitive array để datatype không bị nhầm:</p>
 *
 * <ul>
 *     <li>Java long tương ứng INT64.</li>
 *     <li>Java float tương ứng FLOAT32.</li>
 * </ul>
 */
public class GoldModelInput implements Serializable {

    private static final long serialVersionUID = 1L;

    private long[][] xCat;
    private float[][] xNum;

    /**
     * Constructor rỗng để Jackson và Flink POJO serializer sử dụng.
     */
    public GoldModelInput() {
    }

    public GoldModelInput(
            long[][] xCat,
            float[][] xNum
    ) {
        this.xCat = xCat;
        this.xNum = xNum;
    }

    public long[][] getXCat() {
        return xCat;
    }

    public void setXCat(long[][] xCat) {
        this.xCat = xCat;
    }

    public float[][] getXNum() {
        return xNum;
    }

    public void setXNum(float[][] xNum) {
        this.xNum = xNum;
    }
}