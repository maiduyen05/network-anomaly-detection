package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceWindow;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bảo vệ model Gold khỏi vô tình bị đổi lại thành kiểu
 * khiến Flink phải dùng Kryo.
 */
class GoldPojoTypeTest {

    @Test
    void shouldRecognizeGoldModelsAsFlinkPojos() {
        TypeInformation<GoldSequenceEvent> eventType =
                TypeInformation.of(
                        GoldSequenceEvent.class
                );

        TypeInformation<GoldSequenceWindow> windowType =
                TypeInformation.of(
                        GoldSequenceWindow.class
                );

        assertTrue(
                eventType instanceof PojoTypeInfo<?>,
                () -> "GoldSequenceEvent phải là Flink POJO, "
                        + "nhưng Flink nhận diện thành: "
                        + eventType
        );

        assertTrue(
                windowType instanceof PojoTypeInfo<?>,
                () -> "GoldSequenceWindow phải là Flink POJO, "
                        + "nhưng Flink nhận diện thành: "
                        + windowType
        );
    }
}