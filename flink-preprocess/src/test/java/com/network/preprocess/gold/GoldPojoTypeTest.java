package com.network.preprocess.gold;

import com.network.preprocess.model.GoldSequenceEvent;
import com.network.preprocess.model.GoldSequenceWindow;
import com.network.preprocess.model.GoldEvidence;
import com.network.preprocess.model.GoldSequenceSample;
import com.network.preprocess.model.InvalidGoldFeatureRecord;
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

        TypeInformation<GoldEvidence> evidenceType =
                TypeInformation.of(
                        GoldEvidence.class
                );

        TypeInformation<GoldSequenceSample> sampleType =
                TypeInformation.of(
                        GoldSequenceSample.class
                );

        TypeInformation<InvalidGoldFeatureRecord>
                invalidFeatureType =
                TypeInformation.of(
                        InvalidGoldFeatureRecord.class
                );

        assertTrue(
                evidenceType instanceof PojoTypeInfo<?>,
                () -> "GoldEvidence phải là Flink POJO, "
                        + "nhưng Flink nhận diện thành: "
                        + evidenceType
        );

        assertTrue(
                sampleType instanceof PojoTypeInfo<?>,
                () -> "GoldSequenceSample phải là Flink POJO, "
                        + "nhưng Flink nhận diện thành: "
                        + sampleType
        );

        assertTrue(
                invalidFeatureType instanceof PojoTypeInfo<?>,
                () -> "InvalidGoldFeatureRecord phải là Flink POJO, "
                        + "nhưng Flink nhận diện thành: "
                        + invalidFeatureType
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