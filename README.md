

Readme · MD
# Pipeline giám sát bất thường mạng 5G
 
---
 
## 1. Luồng xử lý dữ liệu (Data Flow)
 
```
Nguồn log 5G
    ↓
NiFi                              (thu thập, định tuyến log thô)
    ↓
Kafka: raw-network-events         (hàng đợi chứa log thô)
    ↓
Flink Job (flink-preprocess)      (xử lý streaming theo thời gian thực)
    ├── parse JSON
    ├── kiểm tra schema
    ├── loại message lỗi
    ├── chuẩn hóa timestamp
    ├── chuyển kiểu dữ liệu
    ├── keyBy theo subscriber/device
    ├── window theo thời gian
    └── tạo đặc trưng (feature)
    ↓
Kafka: processed-network-events   (dữ liệu đã sạch, đã có đặc trưng)
    ↓
Inference Service                 (chạy model AI/ML để phát hiện bất thường)
    ↓
Kafka: anomaly-predictions        (kết quả dự đoán)
    ↓
NWDAF Adapter                     (chuyển đổi & gửi theo chuẩn 3GPP)
    ↓
NWDAF hoặc hệ thống giám sát
```
 
## 2. Cấu trúc thư mục
 
```
~/projects/A/
├── README.md                  # File bạn đang đọc
├── .gitignore
├── .env.example                # Mẫu biến môi trường — copy thành .env rồi điền giá trị thật
├── docker-compose.yml          # "Công thức" bật toàn bộ hệ thống
│
├── config/                     # Cấu hình cho hạ tầng (không phải code)
│   ├── kafka/
│   │   └── topics.yaml         # Danh sách topic, số partition, retention
│   ├── flink/
│   │   └── config.yaml         # Cấu hình chung cho Flink cluster
│   ├── nifi/
│   └── nwdaf/
│       └── adapter-config.yaml # Endpoint NWDAF, thông tin xác thực
├── docs/
│   ├── architecture.md            # Kiến trúc tổng thể hệ thống
│   ├── data-flow.md                # Sơ đồ luồng dữ liệu (giống mục 1 ở trên)
│   ├── setup.md                    # Hướng dẫn cài đặt chi tiết
│   └── operations.md               # Hướng dẫn vận hành, xử lý sự cố
│
├── schemas/                    # Định dạng dữ liệu
│   ├── raw-network-events.schema.json
│   ├── processed-network-events.schema.json
│   ├── anomaly-predictions.schema.json
│   └── examples/                # Vài bản ghi mẫu minh hoạ đúng schema
│
├── nifi/                       # Cấu hình luồng thu thập log (NiFi flow)
│   ├── flows/                   # File export flow NiFi (để backup, version control)
│   ├── parameter-contexts/      # Bộ tham số theo môi trường (dev/staging/prod)
│   └── README.md
│
├── flink-preprocess/            # Job Flink: đọc raw-network-events, xử lý, ghi processed-network-events
│   ├── pom.xml                  # Khai báo thư viện Java (Maven)
│   ├── Dockerfile
│   ├── README.md
│   └── src/
│       ├── main/java/com/network/preprocess/
│       │   ├── PreprocessJob.java     # Tổng howpj các bước xử lý theo thứ tự
│       │   ├── source/                # Đọc dữ liệu từ Kafka
│       │   ├── parser/                # Bước: parse JSON
│       │   ├── validation/            # Bước: kiểm tra schema, loại message lỗi
│       │   ├── operator/              # Các bước biến đổi: map, chuẩn hoá timestamp,
│       │   │                          #   chuyển kiểu, keyBy, window, tạo feature
│       │   ├── sideoutput/            # Tách riêng message lỗi để điều tra sau
│       │   ├── sink/                  # Ghi kết quả ra Kafka
│       │   └── model/                 # Định nghĩa cấu trúc dữ liệu (Java class)
│       └── test/                # Unit test cho từng bước xử lý
│
├── inference-service/           # Đọc processed-network-events, chạy model, ghi anomaly-predictions
│   ├── Dockerfile
│   ├── src/
│   │   ├── consumer/              # Đọc từ Kafka
│   │   ├── model/                 # Load & chạy model AI/ML
│   │   └── producer/              # Ghi kết quả ra Kafka
│   ├── models/                    # Model đã huấn luyện (xem lưu ý ở mục 5)
│   └── tests/
│
├── nwdaf-adapter/                # Đọc anomaly-predictions, gửi tới NWDAF
│   ├── Dockerfile
│   ├── src/
│   │   ├── consumer/               # Đọc từ Kafka
│   │   ├── mapper/                 # Chuyển định dạng nội bộ -> chuẩn 3GPP
│   │   └── client/                 # Gọi API NWDAF / hệ thống giám sát
│   └── tests/
│
├── scripts/                     # Các lệnh tiện ích, chạy bằng ./scripts/ten-file.sh
│   ├── start.sh                  # Bật toàn bộ hệ thống
│   ├── stop.sh                   # Tắt toàn bộ hệ thống
│   ├── create-topics.sh          # Tạo các topic Kafka theo config/kafka/topics.yaml
│   ├── build-flink-job.sh        # Build file .jar cho Flink job
│   ├── submit-flink-job.sh       # Nộp job đã build lên Flink cluster
│   └── test-pipeline.sh          # Bơm dữ liệu mẫu, kiểm tra chạy tới cuối
│
├── tests/
│   ├── integration/                # Test tích hợp giữa nhiều service
│   ├── test-data/                  # Dữ liệu đầu vào dùng để test
│   └── expected-output/            # Kết quả mong đợi, dùng để so sánh
│
│
└── runtime/                      # Dữ liệu sinh ra khi hệ thống chạy (không commit Git)
    ├── checkpoints/                # Flink lưu trạng thái để phục hồi khi crash
    ├── savepoints/                 # Bản lưu trạng thái thủ công (trước khi nâng cấp job)
    └── logs/
```
 
---
 
## 4. Giải thích chi tiết từng thành phần trong pipeline
 
### 4.1. NiFi
Công cụ thu thập log từ nhiều nguồn 5G, xử lý sơ bộ (routing, lọc cơ bản),
rồi đẩy vào Kafka topic `raw-network-events`. Cấu hình được lưu ở `nifi/`
dưới dạng file export, giúp backup và version control (thay vì chỉ tồn tại
trên giao diện web, dễ mất khi restart).
 
### 4.2. Kafka — 3 topic chính
| Topic | Vai trò | Ai ghi vào | Ai đọc ra |
|---|---|---|---|
| `raw-network-events` | Log thô chưa xử lý | NiFi | Flink |
| `processed-network-events` | Log đã làm sạch, có đặc trưng | Flink | Inference Service |
| `anomaly-predictions` | Kết quả dự đoán bất thường | Inference Service | NWDAF Adapter |
 
Cấu hình chi tiết (số partition, thời gian giữ dữ liệu) nằm ở
`config/kafka/topics.yaml`.
 
### 4.3. Flink Job (`flink-preprocess/`)
Đọc `raw-network-events`, thực hiện tuần tự các bước sau, rồi ghi ra
`processed-network-events`:
 
1. **Parse JSON** (`parser/`) — chuyển chuỗi bytes từ Kafka thành object Java.
2. **Kiểm tra schema** (`validation/`) — đối chiếu với
   `schemas/raw-network-events.schema.json`, đảm bảo đủ field cần thiết.
3. **Loại message lỗi** (`validation/`) — message không hợp lệ bị tách ra
   (side output) thay vì drop âm thầm, để có thể điều tra sau.
4. **Chuẩn hóa timestamp** (`operator/`) — đưa về cùng 1 định dạng/timezone.
5. **Chuyển kiểu dữ liệu** (`operator/`) — ví dụ chuỗi số → kiểu số thật.
6. **keyBy theo subscriber/device** (`operator/`) — gom các event cùng 1
   thuê bao/thiết bị lại với nhau, chuẩn bị cho bước window.
7. **Window theo thời gian** (`operator/`) — gom event trong 1 khoảng thời
   gian (ví dụ mỗi 1 phút) để tính toán tổng hợp.
8. **Tạo đặc trưng (feature)** (`operator/`) — tính các chỉ số dùng làm đầu
   vào cho model, ví dụ: tần suất kết nối, tốc độ truyền dữ liệu trung bình.
> **Vì các bước này "có thể thay đổi/thêm bớt sau"**, mỗi bước được viết
> thành 1 class riêng trong `operator/`. Muốn thêm bước mới: tạo 1 file mới,
> rồi thêm 1 dòng gọi trong `PreprocessJob.java`. Muốn bỏ bước: xóa dòng gọi
> tương ứng, không ảnh hưởng các bước khác.
 
### 4.4. Inference Service
Đọc dữ liệu đã có đặc trưng từ `processed-network-events`, đưa vào model đã
huấn luyện để phát hiện bất thường, ghi kết quả (có bất thường hay không,
mức độ tin cậy...) vào `anomaly-predictions`.
 
### 4.5. NWDAF Adapter
Đọc `anomaly-predictions`, chuyển đổi sang định dạng chuẩn NWDAF (theo
3GPP), rồi gọi API gửi tới NWDAF hoặc hệ thống giám sát nội bộ. Tách riêng
service này khỏi Inference Service để khi chuẩn API NWDAF thay đổi, chỉ cần
sửa ở đây mà không ảnh hưởng tới model.
 
---
 
<!-- ## 5. Lưu ý 
 
- **Không commit model AI vào Git** nếu model nặng (vài trăm MB trở lên).
  Dùng Git LFS hoặc lưu ở model registry/S3, chỉ lưu đường dẫn/phiên bản
  trong repo.
- **File `.env` (không phải `.env.example`) không được commit** — chứa
  thông tin nhạy cảm như mật khẩu, endpoint NWDAF thật.
- **Thư mục `runtime/` và `data/`** chứa dữ liệu sinh ra khi chạy, không nên
  đưa vào Git (đã khai báo trong `.gitignore`).
---
  -->
## 6. Chạy project 
 
```bash
# 1. Copy file cấu hình mẫu và điền giá trị thật
cp .env.example .env
 
# 2. Bật toàn bộ hệ thống (Kafka, NiFi, Flink, các service...)
./scripts/start.sh
 
# 3. Tạo các topic Kafka cần thiết
./scripts/create-topics.sh
 
# 4. Build và nộp Flink job
./scripts/build-flink-job.sh
./scripts/submit-flink-job.sh
 
# 5. Kiểm tra pipeline chạy đúng bằng dữ liệu mẫu
./scripts/test-pipeline.sh
 
# Tắt hệ thống khi xong
./scripts/stop.sh
```
 
Xem hướng dẫn chi tiết hơn tại [`docs/setup.md`](docs/setup.md) và
[`docs/operations.md`](docs/operations.md).
 
---
 
## 7. Tài liệu liên quan
 
- [`docs/architecture.md`](docs/architecture.md) — Kiến trúc tổng thể.
- [`docs/data-flow.md`](docs/data-flow.md) — Sơ đồ & giải thích luồng dữ liệu.
- [`docs/setup.md`](docs/setup.md) — Hướng dẫn cài đặt từ đầu.
- [`docs/operations.md`](docs/operations.md) — Vận hành, xử lý sự cố thường gặp.
