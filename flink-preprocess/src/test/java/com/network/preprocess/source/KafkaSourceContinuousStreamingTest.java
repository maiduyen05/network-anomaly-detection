package com.network.preprocess.source;

import com.network.preprocess.config.BronzeJobConfig;
import com.network.preprocess.config.GoldJobConfig;
import com.network.preprocess.config.SilverJobConfig;
import com.network.preprocess.model.BronzeEvent;
import com.network.preprocess.model.KafkaRawRecord;
import com.network.preprocess.model.SilverEvent;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Contract test bảo vệ continuous streaming behavior
 * của toàn bộ Bronze -> Silver -> Gold pipeline.
 *
 * <p>
 * Mục tiêu:
 * </p>
 *
 * <ul>
 *     <li>Bronze Kafka source phải là continuous unbounded.</li>
 *     <li>Silver Kafka source phải là continuous unbounded.</li>
 *     <li>Gold Kafka source phải là continuous unbounded.</li>
 * </ul>
 *
 * <p>
 * Điều này rất quan trọng vì preprocessing pipeline
 * không phải batch job.
 * </p>
 *
 * <p>
 * Khi Kafka tạm thời hết message:
 * </p>
 *
 * <pre>
 * Job KHÔNG được FINISHED.
 *
 * Job phải:
 *
 * RUNNING
 *    ↓
 * chờ Kafka
 *    ↓
 * Kafka có record mới
 *    ↓
 * tiếp tục xử lý
 * </pre>
 */
class KafkaSourceContinuousStreamingTest {

    /**
     * Bronze source phải chạy liên tục.
     *
     * <p>
     * Bronze đọc:
     * </p>
     *
     * <pre>
     * raw.ue.log.line
     * </pre>
     */
    @Test
    void bronzeKafkaSourceShouldBeContinuousUnbounded() {

        BronzeJobConfig config =
                BronzeJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        KafkaSource<KafkaRawRecord> source =
                RawEventKafkaSource.create(
                        config
                );

        assertNotNull(
                source
        );

        /*
         * CONTINUOUS_UNBOUNDED nghĩa là:
         *
         * source không biết trước điểm kết thúc.
         *
         * Khi Kafka tạm hết dữ liệu,
         * source tiếp tục chờ message mới.
         */
        assertEquals(
                Boundedness.CONTINUOUS_UNBOUNDED,
                source.getBoundedness()
        );
    }


    /**
     * Silver source phải chạy liên tục.
     *
     * <p>
     * Silver đọc:
     * </p>
     *
     * <pre>
     * bronze.ue.event
     * </pre>
     */
    @Test
    void silverKafkaSourceShouldBeContinuousUnbounded() {

        SilverJobConfig config =
                SilverJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        KafkaSource<BronzeEvent> source =
                BronzeEventKafkaSource.create(
                        config
                );

        assertNotNull(
                source
        );

        assertEquals(
                Boundedness.CONTINUOUS_UNBOUNDED,
                source.getBoundedness()
        );
    }


    /**
     * Gold source phải chạy liên tục.
     *
     * <p>
     * Gold đọc:
     * </p>
     *
     * <pre>
     * silver.ue.event
     * </pre>
     */
    @Test
    void goldKafkaSourceShouldBeContinuousUnbounded() {

        GoldJobConfig config =
                GoldJobConfig.loadFromClasspath(
                        "application.yaml"
                );

        KafkaSource<SilverEvent> source =
                SilverEventKafkaSource.create(
                        config
                );

        assertNotNull(
                source
        );

        assertEquals(
                Boundedness.CONTINUOUS_UNBOUNDED,
                source.getBoundedness()
        );
    }
}