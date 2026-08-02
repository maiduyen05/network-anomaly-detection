# Log Producer

Module này mô phỏng vai trò ingest của NiFi trong giai đoạn phát triển:

```text
data/raw/incoming
  -> FileLogReader
  -> RawNetworkEventFactory
  -> RawNetworkEventJsonSerializer
  -> KafkaRawEventPublisher
  -> Kafka topic raw.ue.log.line
```

Producer chỉ đọc từng dòng bằng `BufferedReader`, đóng gói raw envelope và gửi
Kafka. Producer không parse, validate hoặc transform 52 field nghiệp vụ; các
bước đó thuộc Flink Bronze.

## Yêu cầu

- Java 17.
- Maven 3.9 trở lên.
- Kafka trong `docker-compose.yml` đang chạy và healthy.

Khi chạy Java trên host/WSL, producer kết nối `localhost:9092`. Listener
`kafka:29092` chỉ dùng cho container trong cùng Docker Compose network.

## Build và test

Chạy từ thư mục gốc repository:

```bash
mvn -f log-producer/pom.xml clean test
```

## Chuẩn bị Kafka

Kafka đang tắt auto-create topic, vì vậy cần tạo topic trước khi chạy producer:

```bash
docker compose up -d kafka
./scripts/create-topics.sh
```

Tên topic được lấy từ `config/kafka/topics.yaml`

## Chạy producer

Đặt các file log vào `data/raw/incoming/`, sau đó chạy:

```bash
mvn -f log-producer/pom.xml \
  -Dexec.mainClass=com.network.producer.LogProducerApplication \
  -Dexec.args="data/raw/incoming" \
  exec:java
```

Mặc định ứng dụng đọc `application.properties` từ classpath. Có thể truyền file
cấu hình ngoài làm tham số thứ hai:

```bash
mvn -f log-producer/pom.xml \
  -Dexec.mainClass=com.network.producer.LogProducerApplication \
  -Dexec.args="data/raw/incoming /path/to/application.properties" \
  exec:java
```

## Kiểm tra message

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic raw.ue.log.line \
  --from-beginning \
  --max-messages 5 \
  --property print.key=true
```

Kafka key phải là `raw_record_id`. Kafka value phải là JSON envelope gồm
`raw_record_id`, `schema_version`, `source_file`, `source_line` và
`raw_payload`; `raw_payload` phải giữ nguyên dòng log phân cách bằng `;`.
