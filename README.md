# 5G Network Anomaly Detection Pipeline

Pipeline streaming dùng Apache Kafka và Apache Flink để tiền xử lý log mạng 5G/LTE và tạo dữ liệu đầu vào cho mô hình phát hiện bất thường.

Phần preprocessing hiện tại được chia thành ba Flink job độc lập:

```text
Raw
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Model-ready sequence
```

Mỗi layer giao tiếp với layer tiếp theo thông qua Kafka, vì vậy ba job có thể chạy liên tục, restart độc lập và phục hồi state bằng checkpoint/savepoint.

---

## 1. Kiến trúc hiện tại

```text
5G / LTE raw log
        │
        │
        ▼
Log Producer / NiFi
        │
        ▼
Kafka
raw.ue.log.line
        │
        ▼
┌─────────────────────┐
│    Flink Bronze     │
│   flink-bronze-v1   │
│                     │
│ Parse raw envelope  │
│ Validate 52 fields  │
│ Normalize timestamp │
│ Type conversion     │
└──────────┬──────────┘
           │
           ▼
Kafka
bronze.ue.event
           │
           ▼
┌─────────────────────────┐
│      Flink Silver       │
│     flink-silver-v1     │
│                         │
│ Resolve UE identity     │
│ Normalize event         │
│ Deduplicate             │
│ Event-time watermark    │
│ Late-event routing      │
└────────────┬────────────┘
             │
             ▼
Kafka
silver.ue.event
             │
             ▼
┌──────────────────────────┐
│        Flink Gold        │
│      flink-gold-v1       │
│                          │
│ Event-time ordering      │
│ keyBy UE                 │
│ sequence length = 32     │
│ stride = 8               │
│ feature encoding         │
└─────────────┬────────────┘
              │
              ▼
Kafka
gold.ue.sequence
              │
              ▼
Model input

x_cat[32][4]
x_num[32][2]
```

Kafka đóng vai trò boundary giữa các layer. Bronze, Silver và Gold là ba streaming job độc lập chứ không phải ba bước nằm trong một Flink job duy nhất.

---

## 2. Runtime stack

Local development hiện sử dụng:

- Java 17
- Apache Flink 1.20.1
- Apache Kafka 3.8.1
- Maven
- Docker Compose
- Kafka single-node KRaft
- 1 Flink JobManager
- 1 Flink TaskManager
- 9 Task Slots cho ba job parallelism 3

Flink Web UI:

```text
http://localhost:8081
```

Kafka listener:

```text
Host:
localhost:9092

Docker network:
kafka:29092
```

---

## 3. Kafka topics

### Main pipeline

| Layer | Input | Output | Consumer Group |
|---|---|---|---|
| Bronze | `raw.ue.log.line` | `bronze.ue.event` | `flink-bronze-v1` |
| Silver | `bronze.ue.event` | `silver.ue.event` | `flink-silver-v1` |
| Gold | `silver.ue.event` | `gold.ue.sequence` | `flink-gold-v1` |

### Side-output topics

| Topic | Layer | Mục đích |
|---|---|---|
| `dlq.ue.log.line` | Bronze | Raw record không parse/validate được |
| `invalid-identity` | Silver | Không resolve được UE identity |
| `unsupported-event` | Silver | Event không thuộc supported event catalog |
| `late-ue-event` | Silver | Event đến sau Silver watermark |
| `gold-too-late-event` | Gold | Event đến quá trễ đối với Gold state |
| `invalid-gold-feature` | Gold | Sequence không encode được theo feature contract |

Danh sách topic chính thức nằm tại:

```text
config/kafka/topics.yaml
```

---

## 4. Bronze layer

Entry point:

```text
com.network.preprocess.bronze.BronzeJob
```

Pipeline:

```text
raw.ue.log.line
        ↓
KafkaRawRecord
        ↓
BronzeTransformer
        ↓
BronzeEvent
        ↓
bronze.ue.event
```

Bronze thực hiện:

1. Đọc raw envelope từ Kafka.
2. Parse raw payload.
3. Kiểm tra raw log có đúng 52 field.
4. Chuẩn hóa timestamp.
5. Chuyển dữ liệu sang kiểu phù hợp.
6. Ghi record hợp lệ vào `bronze.ue.event`.
7. Route record lỗi sang `dlq.ue.log.line`.

Raw log hiện sử dụng:

```text
delimiter   = ;
field-count = 52
timezone    = Asia/Ho_Chi_Minh
```

---

## 5. Silver layer

Entry point:

```text
com.network.preprocess.silver.SilverJob
```

Pipeline:

```text
bronze.ue.event
        ↓
UE identity resolution
        ↓
Event normalization
        ↓
Deduplication
        ↓
Event-time watermark
        ↓
Late-event routing
        ↓
silver.ue.event
```

Silver chịu trách nhiệm chuẩn hóa dữ liệu nghiệp vụ trước khi Gold tạo sequence.

### Deduplication

Deduplication sử dụng Kafka source metadata để xác định lại cùng một Kafka record.

State được giữ với TTL:

```text
24 hours
```

### Event time

Silver sử dụng:

```text
max out-of-orderness = 30 seconds
idleness timeout     = 60 seconds
```

Partition không có dữ liệu trong thời gian idleness sẽ không giữ watermark của toàn pipeline.

### Supported event catalog

Model hiện hỗ trợ 9 event:

```text
l_attach
l_bearer_modify
l_dedicated_bearer_activate
l_dedicated_bearer_deactivate
l_detach
l_handover
l_pdn_connect
l_service_request
l_tau
```

---

## 6. Gold layer

Entry point:

```text
com.network.preprocess.gold.GoldJob
```

Gold đọc:

```text
silver.ue.event
```

và ghi model-ready sample vào:

```text
gold.ue.sequence
```

Gold thực hiện:

```text
SilverEvent
    ↓
GoldSequenceEvent
    ↓
keyBy ueKey
    ↓
event-time ordering
    ↓
sliding sequence
    ↓
feature encoder
    ↓
GoldSequenceSample
```

### Sequence contract

```text
sequence length = 32
stride          = 8
partial window  = false
```

Ví dụ:

```text
Event  1 .. 32 → sample 1
Event  9 .. 40 → sample 2
Event 17 .. 48 → sample 3
```

Đây là count-based sliding sequence, không phải time-window 1 phút hoặc 5 phút.

---

## 7. Model feature contract

Source of truth nằm trong:

```text
flink-preprocess/src/main/resources/application.yaml
```

Feature version hiện tại:

```text
gold-ue-sequence-feature-v2
```

Model input:

```text
x_cat[32][4]
x_num[32][2]
```

### Categorical tensor

```text
x_cat[:, 0] → event_code
x_cat[:, 1] → event_result_code
x_cat[:, 2] → normalized_cause_code
x_cat[:, 3] → sub_cause_code
```

Data type:

```text
INT64
```

Unknown hoặc missing categorical value không được tự động thêm vocabulary:

```text
unknown-policy = REJECT
missing-policy = REJECT
```

### Numeric tensor

```text
x_num[:, 0] → duration_ms
x_num[:, 1] → request_retries
```

Data type:

```text
FLOAT32
```

Normalized valid range:

```text
[0.0, 1.0]
```

### Quan trọng

Không được thay đổi:

```text
sequence length
stride
feature count
feature order
vocabulary mapping
normalization rule
```

mà vẫn giữ:

```text
gold-ue-sequence-feature-v2
```

Nếu feature contract thay đổi thì cần:

```text
1. tạo feature version mới
2. train lại model tương ứng
3. deploy model mới tương ứng
```

---

## 8. Gold output

Một Kafka message trong:

```text
gold.ue.sequence
```

tương ứng một model-ready sample.

Cấu trúc chính:

```json
{
  "schema_version": "gold-sequence-v1",
  "feature_version": "gold-ue-sequence-feature-v2",
  "sample_id": "...",
  "ue_key": "...",
  "imsi": "...",
  "sequence_length": 32,
  "stride": 8,
  "model_input": {
    "x_cat": [],
    "x_num": []
  },
  "evidence": {
    "events": []
  }
}
```

Model chỉ sử dụng:

```text
model_input.x_cat
model_input.x_num
```

`evidence` phục vụ:

- debug
- audit
- UI
- giải thích anomaly
- điều tra dữ liệu

Các evidence field có thể cấu hình trong `application.yaml` mà không thay đổi model tensor contract.

---

## 9. Checkpoint và Exactly-Once

Bronze, Silver và Gold dùng chung runtime configuration:

```text
parallelism                  = 3
checkpoint interval          = 60 seconds
checkpoint timeout           = 5 minutes
max concurrent checkpoints   = 1
minimum pause                = 30 seconds
```

Kafka sinks sử dụng:

```text
EXACTLY_ONCE
```

Kafka consumers downstream sử dụng:

```text
read_committed
```

Do đó output transactional chỉ được downstream nhìn thấy sau khi transaction tương ứng được commit.

---

## 10. Runtime state

Flink state local được lưu tại:

```text
runtime/flink/
├── checkpoints/
├── savepoints/
├── logs/
└── usrlib/
```

Docker mount:

```text
Host:
runtime/flink/checkpoints

Container:
/opt/flink/runtime/checkpoints
```

và:

```text
Host:
runtime/flink/savepoints

Container:
/opt/flink/runtime/savepoints
```

Các thư mục này là runtime artifact và không được commit Git.

Không xóa thủ công:

```text
runtime/flink/checkpoints/*
runtime/flink/savepoints/*
```

khi chưa xác định rõ state nào còn cần dùng.

---

## 11. Stable operator UID

Các operator quan trọng trong Bronze, Silver và Gold sử dụng `.uid(...)` cố định.

Ví dụ:

```text
bronze-kafka-source-v1
bronze-transform-v1

silver-bronze-event-source-v1
silver-deduplicate-source-offset-v1
silver-late-event-router-v1

gold-silver-event-source-v1
gold-build-sequence-window-v1
gold-encode-model-feature-v1
```

UID ổn định giúp Flink map operator state khi restore từ checkpoint/savepoint.

Không tùy tiện đổi UID của stateful operator trong một deployment đang cần restore state cũ.

---

## 12. Build Flink job

Từ root repository:

```bash
./scripts/build-flink-job.sh
```

Script build Maven module:

```text
flink-preprocess
```

và deploy JAR tới:

```text
runtime/flink/usrlib/flink-preprocess-1.0.0-SNAPSHOT.jar
```

JAR trong `runtime/flink/usrlib` độc lập với Maven `target`, nên `mvn clean` không xóa JAR đang được Flink container sử dụng.

---

## 13. Start pipeline

Lần đầu:

```bash
cp .env.example .env
```

Sau đó:

```bash
./scripts/start.sh
```

`start.sh` chịu trách nhiệm:

```text
Docker services
      ↓
Kafka ready
      ↓
Kafka topics
      ↓
Flink runtime
      ↓
JAR availability
      ↓
submit / restore jobs
```

Submit order:

```text
Gold
  ↓
Silver
  ↓
Bronze
```

Downstream được khởi động trước để consumer sẵn sàng trước khi upstream bắt đầu phát sinh dữ liệu.

Kiểm tra:

```bash
docker exec a-flink-jobmanager \
  flink list -r
```

Kết quả bình thường phải có đúng:

```text
flink-gold-v1   (RUNNING)
flink-silver-v1 (RUNNING)
flink-bronze-v1 (RUNNING)
```

Không được có duplicate instance của cùng một job.

---

## 14. Submit job thủ công qua script

Có thể chạy:

```bash
./scripts/submit-flink-job.sh
```

Script có duplicate protection.

Nếu cả ba job đã RUNNING:

```text
Gold   = 1
Silver = 1
Bronze = 1
```

script không submit thêm.

Nếu topology bị partial, ví dụ:

```text
Gold   RUNNING
Silver RUNNING
Bronze missing
```

script sẽ fail thay vì tự động restore hoặc submit một job không rõ state.

Điều này tránh rollback hoặc mất state ngoài ý muốn.

---

## 15. State-safe stop

Không dùng `docker compose down` trực tiếp để dừng một pipeline stateful đang chạy.

Dùng:

```bash
./scripts/stop.sh
```

Script thực hiện:

```text
Bronze
   ↓
savepoint
   ↓
stop

Silver
   ↓
savepoint
   ↓
stop

Gold
   ↓
savepoint
   ↓
stop
```

Sau khi cả ba savepoint thành công, script tạo:

```text
runtime/flink/restore-manifest.env
```

Manifest lưu đường dẫn:

```text
BRONZE_SAVEPOINT
SILVER_SAVEPOINT
GOLD_SAVEPOINT
```

Sau đó Docker services mới được stop.

### Stop order

```text
Bronze → Silver → Gold
```

Bronze được dừng trước để ngăn raw data mới tiếp tục chảy vào pipeline.

---

## 16. Restore từ savepoint

Lần `start.sh` tiếp theo:

```bash
./scripts/start.sh
```

nếu tìm thấy:

```text
runtime/flink/restore-manifest.env
```

pipeline sẽ chạy restore mode.

Restore order:

```text
Gold
  ↓
Silver
  ↓
Bronze
```

Mỗi job được submit với:

```text
flink run -s <savepoint>
```

Không sử dụng:

```text
--allowNonRestoredState
```

trong normal lifecycle.

Nếu state không map được, deployment phải fail để điều tra thay vì âm thầm bỏ state.

Sau khi cả ba job restore thành công, manifest được archive:

```text
restore-manifest.env.used.<timestamp>
```

nhằm tránh lần start tiếp theo vô tình restore lại cùng savepoint.

---

## 17. Kiểm tra Kafka consumer lag

Bronze:

```bash
docker exec a-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --group flink-bronze-v1
```

Silver:

```bash
docker exec a-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --group flink-silver-v1
```

Gold:

```bash
docker exec a-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --group flink-gold-v1
```

Sau restart có thể tạm thời xuất hiện lag. Khi pipeline bắt kịp Kafka, lag sẽ giảm về 0.

---

## 18. End-to-end smoke test

Chạy:

```bash
./scripts/test-pipeline.sh
```

Smoke test tự động kiểm tra:

```text
Raw
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

Smoke data sử dụng một IMSI mới cho mỗi lần chạy để không bị trộn với Gold state cũ.

Test gửi:

```text
40 target events
+
1 watermark flush event
=
41 raw events
```

Expected:

```text
Raw     41
Bronze  41
Silver  41
Gold     2
```

Gold windows:

```text
events 1..32
events 9..40
```

Flush event ở event-time phía sau dùng để đẩy watermark đủ xa cho Gold event-time timer fire.

Smoke test còn kiểm tra:

```text
x_cat[32][4]
x_num[32][2]
feature_version
sequence_length
stride
sample_id
x_cat/x_num JSON field names
side-output topics
```

Expected final result:

```text
================================================
 END-TO-END SMOKE TEST PASSED
================================================

Pipeline:

  Raw     41/41 PASS
  Bronze  41/41 PASS
  Silver  41/41 PASS
  Gold     2/2 PASS

Model contract:

  x_cat[32][4] PASS
  x_num[32][2] PASS

Side outputs:

  PASS
```

---

## 19. Generate smoke data riêng

Có thể chỉ tạo dữ liệu test mà chưa gửi Kafka:

```bash
./scripts/create-gold-smoke-data.sh
```

File được tạo tại:

```text
data/smoke-gold/gold-smoke.log
```

Kiểm tra:

```bash
wc -l data/smoke-gold/gold-smoke.log
```

Expected:

```text
41
```

---

## 20. Log Producer

Trong local development, `log-producer` mô phỏng phần ingest trước Kafka.

Pipeline:

```text
input directory
      ↓
FileLogReader
      ↓
RawNetworkEventFactory
      ↓
JSON envelope
      ↓
Kafka producer
      ↓
raw.ue.log.line
```

Producer chỉ ingest raw line.

Producer không:

```text
parse 52 business fields
normalize event
resolve identity
generate features
```

Các trách nhiệm đó thuộc Flink.

Chạy producer thủ công:

```bash
mvn \
  -f log-producer/pom.xml \
  -Dexec.mainClass=com.network.producer.LogProducerApplication \
  -Dexec.args="data/raw/incoming" \
  exec:java
```

---

## 21. Useful commands

### List Kafka topics

```bash
docker exec a-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --list
```

### List Flink jobs

```bash
docker exec a-flink-jobmanager \
  flink list -r
```

### Check Gold topic end offsets

```bash
docker exec a-kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 \
  --topic gold.ue.sequence \
  --time -1
```

### Read committed Gold records

```bash
docker exec a-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic gold.ue.sequence \
  --from-beginning \
  --consumer-property isolation.level=read_committed
```

### Docker status

```bash
docker compose ps
```

### JobManager logs

```bash
docker logs a-flink-jobmanager
```

### TaskManager logs

```bash
docker logs a-flink-taskmanager
```

---

## 22. Repository structure

```text
network-anomaly-detection/
│
├── README.md
├── .env.example
├── .gitignore
├── docker-compose.yml
│
├── config/
│   ├── kafka/
│   │   └── topics.yaml
│   ├── flink/
│   ├── nifi/
│   └── nwdaf/
│
├── flink-preprocess/
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/com/network/preprocess/
│       │   │   ├── bronze/
│       │   │   ├── silver/
│       │   │   ├── gold/
│       │   │   ├── config/
│       │   │   ├── runtime/
│       │   │   ├── source/
│       │   │   ├── sink/
│       │   │   ├── parser/
│       │   │   ├── operator/
│       │   │   └── model/
│       │   │
│       │   └── resources/
│       │       └── application.yaml
│       │
│       └── test/
│
├── log-producer/
│   ├── pom.xml
│   └── src/
│
├── scripts/
│   ├── start.sh
│   ├── stop.sh
│   ├── create-topics.sh
│   ├── build-flink-job.sh
│   ├── submit-flink-job.sh
│   ├── create-gold-smoke-data.sh
│   └── test-pipeline.sh
│
├── data/
│   └── smoke-gold/
│
└── runtime/
    └── flink/
        ├── checkpoints/
        ├── savepoints/
        ├── logs/
        └── usrlib/
```

---

## 23. Development rules

### Rule 1 — Kafka boundaries are contracts

Không thay đổi schema giữa:

```text
Raw → Bronze
Bronze → Silver
Silver → Gold
```

mà không xem xét backward compatibility.

### Rule 2 — Stable UID

Không thay `.uid(...)` của stateful operator nếu vẫn cần restore state cũ.

### Rule 3 — Feature contract versioning

Nếu tensor contract thay đổi, phải tăng `feature-version` và retrain model.

### Rule 4 — Không commit runtime state

Không commit:

```text
checkpoint
savepoint
Flink logs
deployed JAR
restore manifest
generated smoke data
```

### Rule 5 — Không xóa state tùy tiện

Checkpoint và savepoint local có thể thuộc một Flink Job ID cũ nhưng vẫn hữu ích cho recovery/debug.

Chỉ cleanup khi xác định rõ state đó không còn cần thiết.

### Rule 6 — State-safe shutdown

Đối với pipeline đang RUNNING:

```text
./scripts/stop.sh
```

thay vì stop container trực tiếp.

---

## 24. Validated pipeline status

Pipeline preprocessing hiện đã được kiểm tra với:

```text
Bronze runtime                PASS
Silver runtime                PASS
Gold runtime                  PASS

Checkpoint                    PASS
Exactly-once Kafka sinks      PASS

Bronze savepoint restore      PASS
Silver savepoint restore      PASS
Gold savepoint restore        PASS

Automated stop                PASS
Automated restore             PASS

End-to-end smoke test         PASS

Raw → Bronze → Silver → Gold  PASS

Gold model contract:
x_cat[32][4]                  PASS
x_num[32][2]                  PASS
```

Phần được xác nhận ở đây là preprocessing pipeline đến `gold.ue.sequence`.

Inference Service và NWDAF Adapter là downstream components riêng và không nằm trong end-to-end preprocessing smoke test hiện tại.