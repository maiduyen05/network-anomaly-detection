# Phát hiện Bất thường Mạng 5G/LTE

Hệ thống streaming dùng Apache Kafka và Apache Flink để chuyển đổi log mạng 5G/LTE thành các chuỗi dữ liệu model-ready phục vụ:

- huấn luyện mô hình phát hiện bất thường;
- suy luận online trên dữ liệu thực tế;
- tích hợp kết quả dự đoán với các hệ thống downstream như NWDAF.

Pipeline preprocessing được tổ chức theo kiến trúc nhiều tầng:

```text
Raw
 │
 ▼
Bronze
 │
 ▼
Silver
 │
 ▼
Gold
 │
 ▼
Model-ready Sequence
 │
 ▼
Inference Service
 │
 ▼
Anomaly Prediction
 │
 ▼
NWDAF Adapter
```

Các mục tiêu thiết kế chính:

- xử lý streaming liên tục;
- dữ liệu có lineage rõ ràng;
- hỗ trợ replay;
- xử lý event-time đúng ngữ nghĩa;
- hạn chế training-serving skew;
- xử lý state an toàn;
- Kafka output theo exactly-once;
- không silently drop dữ liệu lỗi;
- feature contract ổn định và có version;
- có khả năng phục hồi từ checkpoint/savepoint.

---

# Mục lục

1. [Bối cảnh nghiệp vụ](#1-bối-cảnh-nghiệp-vụ)
2. [Phạm vi hệ thống](#2-phạm-vi-hệ-thống)
3. [Kiến trúc tổng thể](#3-kiến-trúc-tổng-thể)
4. [Trách nhiệm của từng tầng dữ liệu](#4-trách-nhiệm-của-từng-tầng-dữ-liệu)
5. [Kafka Topics và luồng dữ liệu](#5-kafka-topics-và-luồng-dữ-liệu)
6. [Tầng Bronze](#6-tầng-bronze)
7. [Tầng Silver](#7-tầng-silver)
8. [Event-time và Watermark](#8-event-time-và-watermark)
9. [Tầng Gold](#9-tầng-gold)
10. [Sinh chuỗi sự kiện](#10-sinh-chuỗi-sự-kiện)
11. [Model Feature Contract](#11-model-feature-contract)
12. [Training và Online Serving](#12-training-và-online-serving)
13. [Data Quality và Side Outputs](#13-data-quality-và-side-outputs)
14. [Processing Guarantees](#14-processing-guarantees)
15. [Quản lý State](#15-quản-lý-state)
16. [Phục hồi khi lỗi](#16-phục-hồi-khi-lỗi)
17. [Replay dữ liệu](#17-replay-dữ-liệu)
18. [Baseline đã kiểm chứng](#18-baseline-đã-kiểm-chứng)
19. [Runtime Configuration](#19-runtime-configuration)
20. [Build và Deployment](#20-build-và-deployment)
21. [Monitoring và vận hành](#21-monitoring-và-vận-hành)
22. [Mức độ sẵn sàng Production](#22-mức-độ-sẵn-sàng-production)
23. [Bảo mật và Data Governance](#23-bảo-mật-và-data-governance)
24. [Quản lý thay đổi](#24-quản-lý-thay-đổi)
25. [Cấu trúc Repository](#25-cấu-trúc-repository)

---

# 1. Bối cảnh nghiệp vụ

Mạng viễn thông liên tục phát sinh các sự kiện signaling, mobility, session và bearer liên quan đến hoạt động của UE.

Ví dụ:

```text
Attach
Detach
Handover
Service Request
Tracking Area Update
PDN Connect
PDN Disconnect
Bearer Modify
Dedicated Bearer Activate
Dedicated Bearer Deactivate
```

Một sự kiện đơn lẻ thường không đủ để xác định UE có hành vi bất thường hay không.

Mô hình anomaly detection vì vậy sử dụng **chuỗi nhiều sự kiện liên tiếp của cùng một UE**.

Pipeline preprocessing chịu trách nhiệm chuyển đổi:

```text
Raw network logs
        ↓
Validated events
        ↓
Normalized business events
        ↓
UE identity
        ↓
Ordered UE timeline
        ↓
Fixed-length sequence
        ↓
Model tensors
        ↓
Anomaly detection model
```

Gold output vì vậy không chỉ là dữ liệu đã làm sạch.

Nó là **contract trực tiếp giữa nền tảng streaming và mô hình machine learning**.

---

# 2. Phạm vi hệ thống

Repository bao gồm các thành phần chính:

```text
Data ingestion
    ↓
Kafka transport
    ↓
Flink preprocessing
    ↓
Model-ready sequence
    ↓
Inference service
    ↓
Anomaly prediction
    ↓
NWDAF integration
```

Các component:

| Thành phần | Trách nhiệm |
|---|---|
| Log Producer / NiFi | Thu thập và đưa raw network logs vào Kafka |
| Kafka | Streaming transport và durable layer boundary |
| Flink Bronze | Parse và validate dữ liệu raw |
| Flink Silver | Chuẩn hóa dữ liệu nghiệp vụ và UE identity |
| Flink Gold | Tạo sequence và model features |
| Inference Service | Chạy mô hình anomaly detection |
| NWDAF Adapter | Chuyển prediction sang format phục vụ downstream NWDAF |

README này tập trung chủ yếu vào:

```text
Bronze
Silver
Gold
```

vì đây là phần định nghĩa preprocessing contract và model-input contract.

---

# 3. Kiến trúc tổng thể

```text
                       ┌──────────────────┐
                       │  Network Logs    │
                       └────────┬─────────┘
                                │
                                ▼
                    ┌──────────────────────┐
                    │ Log Producer / NiFi  │
                    └──────────┬───────────┘
                               │
                               ▼
                       raw.ue.log.line
                               │
                               ▼
               ┌──────────────────────────────┐
               │        Flink Bronze          │
               │       flink-bronze-v1        │
               │                              │
               │ Parse envelope               │
               │ Validate 52 fields           │
               │ Normalize EVENT_TIME         │
               │ Type conversion              │
               │ Preserve Kafka metadata      │
               └──────────────┬───────────────┘
                              │
                              ▼
                      bronze.ue.event
                              │
                              ▼
             ┌─────────────────────────────────┐
             │          Flink Silver           │
             │         flink-silver-v1         │
             │                                 │
             │ Source timestamp                │
             │ Watermark                       │
             │ Watermark alignment             │
             │ UE identity resolution          │
             │ Event normalization             │
             │ Deduplication                   │
             │ keyBy ueKey                     │
             │ Late routing                    │
             └───────────────┬─────────────────┘
                             │
                             ▼
                     silver.ue.event
                             │
                             ▼
              ┌──────────────────────────────┐
              │         Flink Gold           │
              │        flink-gold-v1         │
              │                              │
              │ keyBy ueKey                  │
              │ Per-UE reorder               │
              │ Sliding sequence             │
              │ Feature encoding             │
              └──────────────┬───────────────┘
                             │
                             ▼
                     gold.ue.sequence
                             │
                             ▼
                  Inference Service
                             │
                             ▼
                  anomaly-predictions
                             │
                             ▼
                     NWDAF Adapter
```

Bronze, Silver và Gold là **ba Flink job độc lập**.

Kafka là durable boundary giữa các tầng.

Ưu điểm:

- restart từng job độc lập;
- replay từng layer độc lập;
- quản lý consumer offset riêng;
- quản lý state riêng;
- scale độc lập;
- giảm coupling giữa các tầng.

---

# 4. Trách nhiệm của từng tầng dữ liệu

## Raw

Raw là dữ liệu gần nhất với source ban đầu.

Topic:

```text
raw.ue.log.line
```

Raw nên được xem là immutable source để phục vụ full replay.

---

## Bronze

Bronze trả lời câu hỏi:

> Record raw này có hợp lệ về mặt cấu trúc hay không?

Bronze thực hiện:

```text
Parse
Schema validation
Timestamp normalization
Type conversion
Source metadata preservation
```

Bronze không thực hiện model feature engineering.

---

## Silver

Silver trả lời:

> Đây là sự kiện nghiệp vụ nào và thuộc về UE nào?

Silver thực hiện:

```text
UE identity resolution
Business event normalization
Deduplication
Event-time control
Late routing
```

Silver là canonical normalized event layer.

---

## Gold

Gold trả lời:

> Các event của cùng UE được biến thành model input như thế nào?

Gold thực hiện:

```text
Per-UE ordering
Sliding sequence
Categorical encoding
Numeric normalization
Model tensor generation
Evidence generation
```

---

# 5. Kafka Topics và luồng dữ liệu

Source of truth:

```text
config/kafka/topics.yaml
```

Cấu hình local hiện tại:

```text
partitions         = 3
replication factor = 1
```

## Main topics

| Topic | Producer | Consumer |
|---|---|---|
| `raw.ue.log.line` | Log Producer | Bronze |
| `bronze.ue.event` | Bronze | Silver |
| `silver.ue.event` | Silver | Gold |
| `gold.ue.sequence` | Gold | Inference Service |
| `anomaly-predictions` | Inference Service | NWDAF Adapter |

## Side-output topics

| Topic | Tầng | Ý nghĩa |
|---|---|---|
| `dlq.ue.log.line` | Bronze | Raw record parse/validate thất bại |
| `invalid-identity` | Silver | Không resolve được UE identity |
| `unsupported-event` | Silver | Event không nằm trong supported catalog |
| `late-ue-event` | Silver | Event vượt event-time boundary của Silver |
| `gold-too-late-event` | Gold | Event đến sau khi timeline của cùng UE đã finalized |
| `invalid-gold-feature` | Gold | Sequence không encode được theo feature contract |

Nguyên tắc:

> Không silently drop dữ liệu lỗi.

Mỗi loại lỗi cần được route sang side output riêng để monitoring và investigation.

---

# 6. Tầng Bronze

Entry point:

```text
com.network.preprocess.bronze.BronzeJob
```

Flow:

```text
raw.ue.log.line
        ↓
KafkaRawRecord
        ↓
Envelope parser
        ↓
Raw log parser
        ↓
Schema validation
        ↓
Timestamp normalization
        ↓
Type conversion
        ↓
BronzeEvent
        ↓
bronze.ue.event
```

## Raw log format

```text
delimiter   = ;
field-count = 52
timezone    = Asia/Ho_Chi_Minh
```

Parser phải giữ trailing empty fields.

Ví dụ:

```text
a;b;c;;
```

vẫn chứa đầy đủ các cột rỗng cuối record.

---

## Timestamp

Timestamp raw được hiểu theo:

```text
Asia/Ho_Chi_Minh
```

và chuẩn hóa về UTC:

```text
ISO-8601 UTC
```

Ví dụ:

```text
2024-06-26T07:05:23.330Z
```

Downstream sử dụng timestamp đã normalize này làm event time.

---

## Kafka source lineage

Bronze giữ:

```text
source.topic
source.partition
source.offset
```

Metadata này dùng cho:

- traceability;
- audit;
- deduplication;
- replay investigation.

---

# 7. Tầng Silver

Entry point:

```text
com.network.preprocess.silver.SilverJob
```

Topology hiện tại:

```text
bronze.ue.event
        ↓
KafkaSource
        ↓
Timestamp + Watermark
        ↓
Watermark Alignment
        ↓
Resolve UE Identity
        ↓
Normalize Event
        ↓
keyBy(source coordinates)
        ↓
Deduplicate
        ↓
keyBy(ueKey)
        ↓
Late routing
        ↓
silver.ue.event
```

---

## UE Identity Resolution

Canonical identity:

```text
ueKey
```

Hiện tại ưu tiên IMSI.

Logic tổng quát:

```text
Nếu có IMSI
    ↓
dùng IMSI

Nếu không
    ↓
MSISDN / MTMSI mapping
    ↓
IMSI
```

Nếu không resolve được:

```text
invalid-identity
```

---

## Deduplication

Silver deduplicate theo:

```text
source.topic
+
source.partition
+
source.offset
```

Ví dụ:

```text
raw.ue.log.line
partition = 1
offset    = 200
```

sẽ luôn tạo cùng dedup key.

Điều này giúp record bị Kafka/Flink đọc lại sau restart không bị xử lý như event mới.

Không dùng:

```text
ueKey
```

làm dedup key vì một UE hợp lệ có nhiều event khác nhau.

---

# 8. Event-time và Watermark

Processing-time không đại diện chính xác cho thời gian nghiệp vụ của network event.

Pipeline vì vậy dùng:

```text
event-time
```

---

## Silver Source Watermark

Watermark được tạo **ngay tại Kafka source của Silver**.

Không assign lại watermark sau `keyBy`.

Cấu hình:

```text
bounded out-of-orderness = 30 seconds
idleness timeout         = 60 seconds
```

Timestamp:

```text
BronzeEvent.eventTime
```

Không dùng:

```text
Kafka CreateTime
```

---

## Watermark Alignment

Silver bật watermark alignment cho Kafka source partitions.

Cấu hình:

```text
alignment group:
silver-bronze-kafka-source

max drift:
5 seconds

update interval:
1 second
```

Mục tiêu:

> Không cho một Kafka source partition chạy nhanh hơn các partition còn lại hàng phút.

Điều này đặc biệt quan trọng khi:

```text
historical replay
backfill
consumer throughput không đều
partition workload không cân bằng
```

---

# 9. Tầng Gold

Entry point:

```text
com.network.preprocess.gold.GoldJob
```

Input:

```text
silver.ue.event
```

Output:

```text
gold.ue.sequence
```

Flow:

```text
SilverEvent
    ↓
GoldSequenceEvent
    ↓
keyBy(ueKey)
    ↓
Per-UE reorder state
    ↓
Ordered UE timeline
    ↓
Sliding sequence
    ↓
Feature encoding
    ↓
GoldSequenceSample
```

---

## Nguyên tắc ordering

Kafka partition là transport concept.

UE timeline là business/model concept.

Không được đánh đồng hai khái niệm này.

Một Kafka partition có thể chứa:

```text
UE-A
UE-B
UE-C
...
```

Do đó:

> Timestamp tiến xa của UE-A không được làm event UE-B trở thành late.

Gold thực hiện:

```text
keyBy(ueKey)
```

và giữ timeline state riêng cho từng UE.

---

# 10. Sinh chuỗi sự kiện

Sequence contract:

```text
length          = 32
stride          = 8
partial windows = false
```

Ví dụ:

```text
Event 1  → 32
        ↓
Window 1

Event 9  → 40
        ↓
Window 2

Event 17 → 48
        ↓
Window 3
```

Đây là:

```text
count-based sliding sequence
```

không phải time window.

---

## Per-UE reorder tolerance

Gold dùng:

```text
30 seconds
```

Với mỗi UE:

```text
safeThrough
=
maxSeenEventTime
-
30 seconds
```

Event đủ cũ so với `maxSeenEventTime` sẽ được:

```text
sort
 ↓
finalize
 ↓
append vào sequence
```

---

## Too-late semantics

Gold không dùng Kafka partition watermark làm business late rule.

Event bị coi là too-late khi:

```text
event.eventTime
<=
finalizedThrough
```

của **chính cùng `ueKey`**.

Khi đó sequence cũ đã emit và không còn an toàn để chèn event ngược vào.

Side output:

```text
gold-too-late-event
```

---

## Idle flush

Gold hiện dùng:

```text
per-UE idle flush = 60 seconds
```

Nếu UE không phát sinh event mới đủ lâu:

```text
pending buffer
    ↓
sort
    ↓
flush
    ↓
finalize
```

Mục tiêu:

- tránh giữ tail event vô thời hạn;
- hoàn tất tail trong replay;
- hỗ trợ live serving khi UE tạm ngừng phát sinh event.

---

# 11. Model Feature Contract

Source of truth:

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

---

## Categorical tensor

```text
x_cat
```

Shape:

```text
[32][4]
```

Type:

```text
INT64
```

Order:

```text
x_cat[:, 0] = event_code

x_cat[:, 1] = event_result_code

x_cat[:, 2] = normalized_cause_code

x_cat[:, 3] = sub_cause_code
```

Vocabulary là fixed vocabulary.

Runtime không tự tạo category ID mới.

---

## Numeric tensor

```text
x_num
```

Shape:

```text
[32][2]
```

Type:

```text
FLOAT32
```

Order:

```text
x_num[:, 0] = duration_ms

x_num[:, 1] = request_retries
```

Clip range và normalization rule được định nghĩa trong feature contract.

---

## Quy tắc versioning

Không được thay đổi các thành phần sau mà vẫn giữ nguyên feature version:

```text
sequence length
stride
feature count
feature order
vocabulary
category ID
numeric clipping
normalization
missing-value semantics
```

Nếu thay đổi:

```text
1. tạo feature-version mới
2. regenerate training data
3. train lại model
4. version model
5. deploy model và preprocessing tương thích
```

---

# 12. Training và Online Serving

Một mục tiêu quan trọng là giảm:

```text
training-serving skew
```

Training và inference phải dùng cùng:

```text
ueKey semantics
event ordering
sequence length
stride
feature order
categorical vocabulary
numeric normalization
feature version
```

Luồng logic:

```text
                 SilverEvent
                      │
                      ▼
             Shared Gold Contract
                      │
            ┌─────────┴─────────┐
            │                   │
            ▼                   ▼
     Historical Replay       Live Stream
            │                   │
            ▼                   ▼
      Training Dataset        Inference
```

Gold output cần mang:

```text
schema_version
feature_version
```

để downstream xác định compatibility.

---

# 13. Data Quality và Side Outputs

Mỗi lớp lỗi cần có ý nghĩa rõ ràng.

## Bronze

```text
dlq.ue.log.line
```

Các lỗi ví dụ:

```text
Malformed JSON
Invalid envelope
Wrong field count
Invalid timestamp
Type conversion error
```

---

## Silver

```text
invalid-identity
unsupported-event
late-ue-event
```

Không nên gộp các lỗi này vào cùng một DLQ vì nguyên nhân và cách xử lý khác nhau.

---

## Gold

```text
gold-too-late-event
invalid-gold-feature
```

Phân biệt:

```text
too-late
=
ordering/timeline issue
```

và:

```text
invalid-feature
=
model contract issue
```

---

# 14. Processing Guarantees

Kafka sinks sử dụng:

```text
EXACTLY_ONCE
```

Consumer downstream nên sử dụng:

```text
isolation.level=read_committed
```

để chỉ nhìn thấy Kafka transaction đã commit.

---

## Không dùng offset làm business record count

Kafka log-end offset có thể bao gồm transaction/control records.

Do đó:

```text
kafka-get-offsets.sh
```

không phải nguồn chính xác để đếm business records.

Cách đếm committed record:

```bash
docker compose exec -T kafka bash -c "
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 \
    --topic silver.ue.event \
    --from-beginning \
    --consumer-property isolation.level=read_committed \
    --timeout-ms 10000 \
    2>/dev/null | wc -l
"
```

---

# 15. Quản lý State

Silver và Gold sử dụng:

```text
EmbeddedRocksDBStateBackend
```

Stateful use cases:

```text
Silver:
deduplication

Gold:
per-UE reorder
finalized timeline
sliding sequence
idle timer
```

State TTL:

```text
24 hours
```

RocksDB được sử dụng để tránh giữ toàn bộ keyed state trong JVM heap.

---

# 16. Phục hồi khi lỗi

Checkpoint configuration:

```text
checkpoint interval          = 60 seconds
checkpoint timeout           = 300 seconds
max concurrent checkpoints   = 1
minimum pause                = 30 seconds
```

Runtime directories:

```text
runtime/flink/checkpoints
runtime/flink/savepoints
```

---

## Stable Operator UID

Stateful operator sử dụng UID cố định.

Ví dụ:

```text
silver-bronze-event-source-v1
silver-deduplicate-source-offset-v1
silver-late-event-router-v1

gold-silver-event-source-v1
gold-build-sequence-window-v1
gold-encode-model-feature-v1
```

Không tùy tiện đổi UID nếu còn cần restore state cũ.

Đổi UID phải được xem là state migration.

---

# 17. Replay dữ liệu

Kafka boundary cho phép replay theo từng layer.

Không cần full replay nếu upstream layer đã được xác nhận đúng.

---

## Gold-only replay

Dùng khi:

```text
Silver đúng
Gold logic thay đổi
sequence logic thay đổi
feature encoder thay đổi
```

Giữ:

```text
silver.ue.event
```

Reset:

```text
flink-gold-v1

gold.ue.sequence
gold-too-late-event
invalid-gold-feature
```

---

## Silver + Gold replay

Dùng khi:

```text
Bronze đúng
Silver logic thay đổi
watermark thay đổi
identity logic thay đổi
```

Giữ:

```text
bronze.ue.event
```

Reset:

```text
flink-silver-v1
flink-gold-v1

silver.ue.event
invalid-identity
unsupported-event
late-ue-event

gold.ue.sequence
gold-too-late-event
invalid-gold-feature
```

---

## Full replay

Dùng khi:

```text
raw parser thay đổi
Bronze schema thay đổi
timestamp normalization thay đổi
```

Replay:

```text
Raw
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

---

## Quy trình replay khuyến nghị

```text
1. Stop các job bị ảnh hưởng
2. Xác nhận upstream Kafka data hoàn chỉnh
3. Reset consumer group bị ảnh hưởng
4. Xóa đúng downstream output topics
5. Recreate topics
6. Không restore incompatible savepoint
7. Deploy đúng JAR
8. Submit downstream trước upstream
9. Chờ consumer lag = 0
10. Đếm committed records
11. So sánh với regression baseline
```

---


# 19. Runtime Configuration

Local environment:

```text
Java          17
Flink         1.20.1
Kafka         3.8.1
NiFi          1.28.1
```

Job parallelism:

```text
Bronze = 2
Silver = 2
Gold   = 2
```

TaskManager topology:

```text
2 TaskManagers
×
3 slots
=
6 task slots
```

---

# 20. Build và Deployment

Build:

```bash
./scripts/build-flink-job.sh
```

Flow:

```text
mvn clean package
        ↓
verify JAR
        ↓
copy deploy artifact
        ↓
runtime/flink/usrlib
```

JAR:

```text
runtime/flink/usrlib/
└── flink-preprocess-1.0.0-SNAPSHOT.jar
```

Container path:

```text
/opt/flink/usrlib/flink-preprocess-1.0.0-SNAPSHOT.jar
```

Verify:

```bash
docker exec a-flink-jobmanager \
  ls -lh /opt/flink/usrlib/
```

---

## Start local runtime

Kafka + JobManager:

```bash
docker compose up -d \
  kafka \
  flink-jobmanager
```

TaskManagers:

```bash
docker compose up -d \
  --scale flink-taskmanager=2 \
  flink-taskmanager
```

Verify:

```bash
docker compose ps
```

---

## Create topics

```bash
./scripts/create-topics.sh
```

---

## Submit jobs

```bash
./scripts/submit-flink-job.sh
```

Order:

```text
Gold
 ↓
Silver
 ↓
Bronze
```

Downstream-first giúp consumer sẵn sàng trước upstream.

---

## Verify Flink jobs

```bash
docker exec a-flink-jobmanager \
  flink list -r
```

Expected:

```text
flink-gold-v1
flink-silver-v1
flink-bronze-v1
```

Không được tồn tại duplicate RUNNING instance của cùng một job.

---

# 21. Monitoring và vận hành

Production monitoring không chỉ kiểm tra:

```text
job RUNNING
```

mà phải giám sát cả technical metrics và business/data-quality metrics.

---

## Flink metrics

Nên theo dõi:

```text
Job state
Restart count
Checkpoint success
Checkpoint failure
Checkpoint duration
Checkpoint size
Backpressure
Busy time
Idle time
TaskManager memory
Managed memory
RocksDB state size
GC
```

---

## Kafka metrics

```text
Consumer lag
Partition lag imbalance
Producer error
Consumer error
Transaction error
Broker disk usage
Throughput
Under-replicated partition
```

---

## Business/Data metrics

```text
Raw received count

Bronze accepted
Bronze DLQ

Silver accepted
Invalid identity
Unsupported event
Silver late

Gold sequence
Gold too-late
Invalid Gold feature
```

Recommended ratios:

```text
bronze_dlq_rate

invalid_identity_rate

unsupported_event_rate

silver_late_rate

gold_too_late_rate

invalid_gold_feature_rate
```

Các tỷ lệ này có thể báo hiệu data issue ngay cả khi Flink vẫn RUNNING.

---

## Consumer lag commands

Silver:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 \
  --describe \
  --group flink-silver-v1
```

Gold:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:29092 \
  --describe \
  --group flink-gold-v1
```

---

# 22. Mức độ sẵn sàng Production

Docker Compose hiện tại phục vụ:

```text
local development
integration testing
controlled replay
pipeline validation
```

Không nên coi topology local hiện tại là production HA.

---

## Local topology hiện tại

```text
Kafka brokers             = 1
Kafka replication factor  = 1

Flink JobManager          = 1
TaskManagers              = 2

Checkpoint storage        = local filesystem
Savepoint storage         = local filesystem
```

---

## Production deployment thường cần

### Kafka

```text
multiple brokers
replication factor > 1
durable storage
broker monitoring
capacity planning
failure tolerance
```

---

### Flink

```text
High Availability JobManager
multiple TaskManagers
durable checkpoint storage
durable savepoint storage
resource isolation
automatic restart
production-grade observability
```

---

### Durable State

Production không nên phụ thuộc vào state nằm hoàn toàn trên filesystem của một máy local.

Checkpoint/savepoint nên đặt trên durable/shared storage phù hợp với môi trường triển khai.

---

### Observability

Production cần tích hợp với hệ thống:

```text
Centralized logging
Metrics
Dashboard
Alert
Incident management
Consumer lag monitoring
Checkpoint alerting
Data-quality alerting
```

---

# 23. Bảo mật và Data Governance

Pipeline xử lý các identifier nhạy cảm như:

```text
IMSI
MSISDN
MTMSI
IMEISV
```

Trong production nên áp dụng:

```text
least privilege
secret management
network access control
encryption in transit
audit logging
data retention policy
access logging
identifier masking
controlled production-data access
```

Không nên log đầy đủ subscriber identifier ở mức INFO nếu không thật sự cần thiết.

Training data cũng phải tuân thủ các rule quản trị dữ liệu tương tự dữ liệu online.

---

# 24. Quản lý thay đổi

Một số thay đổi phải được xem là architecture/model contract change, không phải chỉnh config thông thường.

---

## Stateful Flink changes

High-risk:

```text
đổi operator UID
đổi keyBy
đổi state type
đổi serializer
đổi dedup key
đổi TTL semantics
```

Các thay đổi này cần kế hoạch:

```text
migration
savepoint compatibility
hoặc clean replay
```

---

## Event-time changes

High-risk:

```text
watermark tolerance
watermark alignment max drift
idleness
Gold reorder tolerance
Gold idle flush
```

Các thay đổi này có thể làm thay đổi:

```text
late rate
event ordering
sequence count
model input
```

Phải regression test bằng baseline.

---

## Model feature changes

High-risk:

```text
feature order
vocabulary
category ID
normalization
sequence length
stride
```

Bắt buộc tạo:

```text
feature-version mới
```

và train lại model.

---

## Kafka changes

High-risk:

```text
topic name
partition count
message key
consumer group
transactional prefix
```

Vì có thể ảnh hưởng:

```text
ordering
state
replay
exactly-once
```

---

# 25. Cấu trúc Repository

```text
network-anomaly-detection/
│
├── config/
│   └── kafka/
│       └── topics.yaml
│
├── data/
│
├── docs/
│
├── flink-preprocess/
│   ├── pom.xml
│   └── src/
│       ├── main/
│       │   ├── java/
│       │   └── resources/
│       │       └── application.yaml
│       └── test/
│
├── inference-service/
│
├── log-producer/
│
├── nifi/
│
├── nwdaf-adapter/
│
├── runtime/
│   └── flink/
│       ├── checkpoints/
│       ├── savepoints/
│       ├── logs/
│       └── usrlib/
│
├── schemas/
│
├── scripts/
│
├── tests/
│
├── docker-compose.yml
└── README.md
```

---

# Các bất biến vận hành quan trọng

## Accounting

Silver phải đảm bảo về mặt logic:

```text
Bronze input
≈
Silver main
+
invalid identity
+
unsupported event
+
Silver late
```

Không được có record biến mất mà không có side output.

---

## UE isolation

Một Gold sequence chỉ được chứa event của:

```text
một ueKey duy nhất
```

Không được trộn nhiều UE trong cùng sequence.

---

## Model shape

Với:

```text
gold-ue-sequence-feature-v2
```

shape phải luôn là:

```text
x_cat[32][4]
x_num[32][2]
```

---

## Event ordering

Kafka partition progress không phải UE timeline.

Gold ordering phải luôn theo:

```text
ueKey
```


