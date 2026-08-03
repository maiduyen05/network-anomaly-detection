package com.network.preprocess.source;

import com.network.preprocess.model.KafkaRawRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer
        .KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * Đọc nội dung từng message và chuyển thành object Java KafkaRawRecord.
 *
 * <p>Class này chỉ chuyển bytes sang UTF-8 và giữ metadata Kafka.
 * Nó không parse JSON và không validate dữ liệu.</p>
 */
public final class KafkaRawRecordDeserializationSchema
        implements KafkaRecordDeserializationSchema<KafkaRawRecord> {

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<KafkaRawRecord> out
    ) {
        /*
         * Kafka cho phép record value bằng null.
         * BronzeTransformer sẽ route trường hợp này sang DLQ.
         */
        String value = record.value() == null
                ? null
                : new String(
                        record.value(),
                        StandardCharsets.UTF_8
                );

        out.collect(
                new KafkaRawRecord(
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.timestamp(),
                        value
                )
        );
    }

    @Override
    public TypeInformation<KafkaRawRecord> getProducedType() {
        /*
         * Cung cấp type information để Flink tạo serializer nội bộ.
         */
        return TypeInformation.of(KafkaRawRecord.class);
    }
}