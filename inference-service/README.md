# inference-service
 
Đọc dữ liệu đã tiền xử lý từ Kafka topic `processed-network-events`, chạy
model phát hiện bất thường (anomaly detection), ghi kết quả ra topic
`anomaly-predictions`.
 
## Vai trò trong pipeline
 
```
Kafka: processed-network-events → [inference-service] → Kafka: anomaly-predictions
```
 
## Cấu trúc
 
```
inference-service/
├── src/
│   ├── consumer/    # Đọc message từ processed-network-events
│   ├── model/        # Load model đã huấn luyện, chạy dự đoán
│   └── producer/      # Ghi kết quả dự đoán vào anomaly-predictions
├── models/             # Model đã huấn luyện (xem lưu ý bên dưới)
└── tests/
```
 
## Đầu vào / Đầu ra
 
- **Đầu vào**: message theo `schemas/processed-network-events.schema.json`
  (đã có timestamp chuẩn hoá, kiểu dữ liệu đúng, và các đặc trưng đã tính
  sẵn từ Flink).
- **Đầu ra**: message theo `schemas/anomaly-predictions.schema.json`, gồm
  tối thiểu: subscriber/device id, thời điểm, có bất thường hay không, độ
  tin cậy (confidence score).
## Model
 
Model đã huấn luyện đặt ở `models/`. **Không commit file model nặng
(>50–100MB) trực tiếp vào Git** — dùng Git LFS, hoặc tốt hơn là lưu ở model
registry/S3/MLflow và chỉ lưu đường dẫn/phiên bản model trong repo (ví dụ
biến môi trường `MODEL_VERSION` trong `.env`).
 
## Chạy
 
```bash
# Build image riêng cho service này
docker build -t inference-service .
 
# Hoặc chạy cùng toàn bộ hệ thống qua docker-compose ở gốc project
../scripts/start.sh
```
 
## Test
 
```bash
# Ví dụ nếu dùng Python
pytest tests/
```
 
## Ghi chú vận hành
 
- Nếu model lỗi khi load hoặc dự đoán, service nên có cơ chế retry/log rõ
  ràng thay vì crash toàn bộ consumer — tránh làm nghẽn topic
  `processed-network-events`.
- Nên theo dõi độ trễ (latency) giữa lúc nhận message và lúc ghi kết quả,
  vì đây là service quyết định tốc độ cảnh báo tới NWDAF.