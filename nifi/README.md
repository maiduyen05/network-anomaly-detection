# nifi
 
Cấu hình luồng thu thập log 5G từ nhiều nguồn, đẩy vào Kafka topic
`raw-network-events`.
 
## Vai trò trong pipeline
 
```
Nguồn log 5G → [NiFi] → Kafka: raw-network-events
```
 
## Cấu trúc
 
```
nifi/
├── flows/                 # File export flow NiFi (JSON), dùng để backup & version control
└── parameter-contexts/    # Bộ tham số theo môi trường (dev/staging/prod)
```
 
## Vì sao cần export flow ra file thay vì chỉ để trên giao diện web?
 
NiFi cho phép kéo-thả xây dựng flow trực tiếp trên web UI, nhưng nếu chỉ
lưu trong NiFi mà không export ra file:
- Mất flow khi container bị xoá/restart mà không có volume đúng.
- Không thể xem lịch sử thay đổi (ai sửa gì, khi nào) qua Git.
- Không dễ triển khai đúng flow đó sang môi trường khác (staging → prod).
Vì vậy sau khi chỉnh sửa flow trên UI, cần export (Download Flow Definition)
và lưu file JSON vào `flows/`, commit lên Git.
 
## Parameter Contexts
 
`parameter-contexts/` chứa các giá trị có thể thay đổi theo môi trường (địa
chỉ Kafka, đường dẫn nguồn log, thông tin xác thực...) mà **không cần sửa
lại logic flow**. Ví dụ dev dùng `kafka:9092`, prod dùng địa chỉ Kafka thật
khác — chỉ cần đổi parameter context tương ứng.
 
## Luồng xử lý chính trong NiFi
 
1. Nhận log từ nguồn (file, syslog, API...) tuỳ theo nguồn 5G thực tế.
2. Lọc/định tuyến sơ bộ (loại log rõ ràng không đúng định dạng, tách theo
   loại nguồn nếu cần).
3. Đẩy vào Kafka topic `raw-network-events` theo đúng
   `schemas/raw-network-events.schema.json`.
## Ghi chú
 
- Không nên đặt logic xử lý phức tạp (parse sâu, tính toán) trong NiFi —
  các bước đó nên để Flink (`flink-preprocess/`) đảm nhiệm, vì Flink xử lý
  streaming có kiểm soát trạng thái/checkpoint tốt hơn cho khối lượng lớn.
- Khi thêm nguồn log mới, chỉ cần thêm processor mới trong flow và export
  lại, không ảnh hưởng các bước phía sau (Kafka trở đi).