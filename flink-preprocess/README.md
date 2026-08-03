# flink-preprocess

sửa sau :< 
 
Flink job đọc log 5G thô từ Kafka topic `raw-network-events`, xử lý theo
thời gian thực, ghi kết quả ra topic `processed-network-events`.
 
## Vai trò trong pipeline
 
```
Kafka: raw.ue.log.line
        │
        ▼
Flink Bronze
  parse envelope
  validate raw schema
  parse 52 fields
  normalize timestamp/type
        │
        ├── lỗi ──► dlq.ue.log.line
        ▼
Kafka: bronze.ue.event
        │
        ▼
Flink Silver
  resolve IMSI
  normalize event/result
  deduplicate
  watermark + late event
        │
        ├── thiếu identity ──► invalid-identity
        ├── unsupported ─────► unsupported-event
        ├── quá muộn ────────► late-ue-event
        ▼
Kafka: silver.ue.event
        │
        ▼
Flink Gold
  keyBy IMSI
  gom đúng 32 event
  stride configurable
  tạo x_cat[32,4]
  tạo x_num[32,2]
  giữ evidence.events
        ▼
Kafka: gold.ue.sequence
        │
        ▼
AnLF
```

 
## Package
 
Toàn bộ code Java nằm trong package `com.network.preprocess`, tương ứng
đường dẫn `src/main/java/com/network/preprocess/`.
 
## Cấu hình
 
Tên topic, địa chỉ Kafka, group id... khai báo trong
`src/main/resources/application.yaml`, KHÔNG hardcode trong code Java —
để dùng chung code cho dev/staging/prod chỉ bằng cách đổi file config.
 
## Build & chạy
 
```bash
# Build file .jar
mvn clean package
# hoặc dùng script chung của project:
../scripts/build-flink-job.sh
 
# Nộp job lên Flink cluster đang chạy (qua docker-compose)
../scripts/submit-flink-job.sh
```
 
## Test
 
```bash
mvn test
```
 
Unit test nằm trong `src/test/java`, mỗi operator có 1 file test riêng
(vd: `TimestampNormalizerTest.java`) — chạy độc lập, không cần Kafka/Flink
thật.
 
## Message lỗi đi đâu?
 
Message không qua được bước "kiểm tra schema" sẽ được tách qua
`sideoutput/InvalidEventSink.java` thay vì bị drop âm thầm, để có thể xem
lại và điều tra nguyên nhân (thường ghi ra topic riêng, ví dụ
`invalid-network-events`, hoặc log file).