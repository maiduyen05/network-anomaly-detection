# flink-preprocess

sửa sau :< 
 
Flink job đọc log 5G thô từ Kafka topic `raw-network-events`, xử lý theo
thời gian thực, ghi kết quả ra topic `processed-network-events`.
 
## Vai trò trong pipeline
 
```
Kafka: raw-network-events → [flink-preprocess] → Kafka: processed-network-events
```
 
## Các bước xử lý (theo thứ tự chạy trong `PreprocessJob.java`)
 
| # | Bước | Class | Mục đích |
|---|---|---|---|
| 1 | Parse JSON | `parser/JsonEventParser.java` | Chuyển bytes từ Kafka thành object Java |
| 2 | Kiểm tra schema | `validation/SchemaValidator.java` | Đối chiếu `schemas/raw-network-events.schema.json` |
| 3 | Loại message lỗi | `validation/InvalidEventFilter.java` | Tách message không hợp lệ sang side output |
| 4 | Chuẩn hóa timestamp | `operator/TimestampNormalizer.java` | Đưa về cùng định dạng/timezone |
| 5 | Chuyển kiểu dữ liệu | `operator/TypeCastOperator.java` | Ép kiểu đúng (chuỗi số → số...) |
| 6 | keyBy subscriber/device | `operator/SubscriberKeySelector.java` | Gom event theo thuê bao/thiết bị |
| 7 | Window theo thời gian | `operator/EventWindowAssigner.java` | Gom event trong 1 khoảng thời gian |
| 8 | Tạo đặc trưng | `operator/FeatureExtractor.java` | Tính chỉ số đầu vào cho model |
 
> Muốn thêm/bớt bước: thêm/xóa 1 class trong `operator/` rồi
> thêm/xóa 1 dòng gọi tương ứng trong `PreprocessJob.java`. Không cần sửa
> các bước khác.
 
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