package com.network.preprocess.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract test cho JSON representation của GoldModelInput.
 *
 * <p>
 * Test này bảo vệ tên field mà downstream model sử dụng:
 * </p>
 *
 * <pre>
 * model_input:
 *   x_cat: INT64[32][4]
 *   x_num: FLOAT32[32][2]
 * </pre>
 *
 * <p>
 * Trước đây Jackson serialize getter:
 * </p>
 *
 * <pre>
 * getXCat() -> xcat
 * getXNum() -> xnum
 * </pre>
 *
 * <p>
 * trong khi contract yêu cầu:
 * </p>
 *
 * <pre>
 * x_cat
 * x_num
 * </pre>
 */
class GoldModelInputJsonTest {

    /**
     * Xác nhận JSON luôn dùng đúng tên field của model contract.
     */
    @Test
    void shouldSerializeUsingModelContractFieldNames()
            throws Exception {

        /*
         * Tạo tensor có đúng shape của model v1.
         */
        long[][] xCat = new long[32][4];
        float[][] xNum = new float[32][2];

        /*
         * Gán vài giá trị khác 0 để chắc chắn
         * dữ liệu thật sự được serialize.
         */
        xCat[0][0] = 8L;
        xCat[0][1] = 1L;

        xNum[0][0] = 0.5F;
        xNum[0][1] = 0.2F;

        GoldModelInput modelInput =
                new GoldModelInput(
                        xCat,
                        xNum
                );

        /*
         * Dùng cùng naming strategy với
         * JsonKafkaRecordSerializationSchema.
         */
        ObjectMapper objectMapper =
                new ObjectMapper();

        objectMapper.setPropertyNamingStrategy(
                PropertyNamingStrategies.SNAKE_CASE
        );

        JsonNode json =
                objectMapper.readTree(
                        objectMapper.writeValueAsBytes(
                                modelInput
                        )
                );

        /*
         * Contract bắt buộc phải có:
         *
         * x_cat
         * x_num
         */
        assertTrue(
                json.has("x_cat"),
                "JSON must contain x_cat"
        );

        assertTrue(
                json.has("x_num"),
                "JSON must contain x_num"
        );

        /*
         * Hai tên sai trước đây không được xuất hiện.
         */
        assertFalse(
                json.has("xcat"),
                "JSON must not contain xcat"
        );

        assertFalse(
                json.has("xnum"),
                "JSON must not contain xnum"
        );

        /*
         * Kiểm tra shape categorical.
         */
        JsonNode serializedXCat =
                json.get("x_cat");

        assertNotNull(serializedXCat);

        assertEquals(
                32,
                serializedXCat.size()
        );

        assertEquals(
                4,
                serializedXCat.get(0).size()
        );

        /*
         * Kiểm tra shape numeric.
         */
        JsonNode serializedXNum =
                json.get("x_num");

        assertNotNull(serializedXNum);

        assertEquals(
                32,
                serializedXNum.size()
        );

        assertEquals(
                2,
                serializedXNum.get(0).size()
        );

        /*
         * Kiểm tra một vài giá trị thật.
         */
        assertEquals(
                8L,
                serializedXCat
                        .get(0)
                        .get(0)
                        .longValue()
        );

        assertEquals(
                1L,
                serializedXCat
                        .get(0)
                        .get(1)
                        .longValue()
        );

        assertEquals(
                0.5F,
                serializedXNum
                        .get(0)
                        .get(0)
                        .floatValue()
        );

        assertEquals(
                0.2F,
                serializedXNum
                        .get(0)
                        .get(1)
                        .floatValue()
        );
    }

    /**
     * Xác nhận JSON theo contract cũng deserialize
     * ngược trở lại GoldModelInput được.
     */
    @Test
    void shouldDeserializeModelContractFieldNames()
            throws Exception {

        String json = """
                {
                  "x_cat": [
                    [8, 1, 0, 0]
                  ],
                  "x_num": [
                    [0.5, 0.2]
                  ]
                }
                """;

        ObjectMapper objectMapper =
                new ObjectMapper();

        objectMapper.setPropertyNamingStrategy(
                PropertyNamingStrategies.SNAKE_CASE
        );

        GoldModelInput result =
                objectMapper.readValue(
                        json,
                        GoldModelInput.class
                );

        assertNotNull(result);

        assertNotNull(
                result.getXCat()
        );

        assertNotNull(
                result.getXNum()
        );

        assertEquals(
                8L,
                result.getXCat()[0][0]
        );

        assertEquals(
                1L,
                result.getXCat()[0][1]
        );

        assertEquals(
                0.5F,
                result.getXNum()[0][0]
        );

        assertEquals(
                0.2F,
                result.getXNum()[0][1]
        );
    }
}