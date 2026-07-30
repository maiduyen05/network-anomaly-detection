# nwdaf-adapter
 
Đọc kết quả dự đoán bất thường từ Kafka topic `anomaly-predictions`, chuyển
đổi sang định dạng chuẩn NWDAF (3GPP) hoặc định dạng hệ thống giám sát nội
bộ, rồi gửi đi.
 
## Vai trò trong pipeline
 
```
Kafka: anomaly-predictions → [nwdaf-adapter] → NWDAF / hệ thống giám sát
```
 
## Vì sao tách riêng khỏi inference-service?
 
Việc gửi dữ liệu tới NWDAF liên quan tới một chuẩn giao tiếp riêng (định
dạng request/response theo 3GPP, cơ chế xác thực, retry, rate limit) — khác
hẳn với việc chạy model. Tách riêng giúp:
 
- NWDAF đổi API → chỉ sửa `nwdaf-adapter/`, không đụng tới model.
- Model đổi → không ảnh hưởng phần gửi NWDAF.
- Có thể scale/riêng biệt khi NWDAF phản hồi chậm mà không ảnh hưởng
  inference.
## Cấu trúc
 
```
nwdaf-adapter/
├── src/
│   ├── consumer/   # Đọc message từ anomaly-predictions
│   ├── mapper/      # Chuyển định dạng nội bộ -> định dạng chuẩn NWDAF
│   └── client/       # Gọi API NWDAF / hệ thống giám sát
└── tests/
```
 
## Cấu hình
 
Endpoint NWDAF, thông tin xác thực, timeout, số lần retry... khai báo ở
`config/nwdaf/adapter-config.yaml` và `.env` (không commit thông tin xác
thực thật lên Git).
 
## Chạy
 
```bash
docker build -t nwdaf-adapter .
# hoặc chạy cùng hệ thống:
../scripts/start.sh
```
 
## Xử lý lỗi khi gửi NWDAF thất bại
 
Khi gọi API NWDAF thất bại (mạng lỗi, NWDAF quá tải...), adapter nên:
1. Retry theo cơ chế backoff (thử lại sau khoảng thời gian tăng dần).
2. Nếu vẫn thất bại sau số lần retry cho phép, ghi lại vào log/dead-letter
   để không mất dữ liệu cảnh báo, đồng thời không chặn các message tiếp
   theo trong Kafka.
## Test
 
```bash
pytest tests/     # hoặc mvn test / npm test tuỳ ngôn ngữ dùng
```
 
Nên có test giả lập (mock) endpoint NWDAF để không phụ thuộc hệ thống
NWDAF thật khi chạy CI.