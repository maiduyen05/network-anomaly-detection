package com.network.producer.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.network.producer.model.RawNetworkEvent;
import com.network.producer.serialization.RawNetworkEventJsonSerializer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit test publisher bằng Kafka MockProducer (class có sẵn trong thư viện kafka để test).
 *
 * <p>Không cần khởi động broker Kafka.</p>
 */
class KafkaRawEventPublisherTest {

    /**
     * Kiểm tra publisher tạo đúng ProducerRecord.
     */
    @Test
    void shouldPublishRawEventToConfiguredTopic()
            throws Exception {

        /*
         * autoComplete=true:
         * MockProducer tự đánh dấu send thành công.
         */
        MockProducer<String, String> mockProducer =
                new MockProducer<>(
                        true,    // tự động coi như send() thành công
                        new StringSerializer(),
                        new StringSerializer()
                );

        RawNetworkEventJsonSerializer serializer =
                new RawNetworkEventJsonSerializer();

        KafkaMessageKeyResolver keyResolver =
                new KafkaMessageKeyResolver();

        RawNetworkEvent event =
                new RawNetworkEvent(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "raw-envelope-v1",
                        "sample.log",
                        1L,
                        "event;success;10;0;normal;"
                                + "84900000001;"
                                + "452040000000001"
                );

        try (
                KafkaRawEventPublisher publisher =
                        new KafkaRawEventPublisher(
                                mockProducer,
                                "raw.ue.log.line",
                                serializer,
                                keyResolver
                        )
        ) {
            publisher.publish(
                    event,
                    null
            );

            publisher.flush();
        }

        List<ProducerRecord<String, String>> history =
                mockProducer.history();

        // Một event phải tạo đúng một Kafka record.
        assertEquals(
                1,
                history.size()
        );

        ProducerRecord<String, String> record =
                history.get(0);

        // Kiểm tra topic.
        assertEquals(
                "raw.ue.log.line",
                record.topic()
        );

        // Key phải tồn tại.
        assertNotNull(
                record.key()
        );

        // Parse Kafka value thành JSON.
        JsonNode root =
                new ObjectMapper().readTree(
                        record.value()
                );

        assertEquals(
                "raw-envelope-v1",
                root.get("schema_version").asText()
        );

        assertEquals(
                "sample.log",
                root.get("source_file").asText()
        );

        assertEquals(
                1L,
                root.get("source_line").asLong()
        );
    }
}