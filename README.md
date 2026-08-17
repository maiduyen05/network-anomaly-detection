# Phát hiện Bất thường Mạng 5G/LTE

Hệ thống streaming phát hiện bất thường trên chuỗi sự kiện thuê bao LTE/5G bằng Apache Kafka, Apache Flink, Python inference worker và Streamlit.

Luồng đã triển khai:

```text
Raw log files
      │
      ▼
Log Producer
      │
      ▼
raw.ue.log.line
      │
      ▼
Flink Bronze
      │
      ▼
bronze.ue.event
      │
      ▼
Flink Silver
      │
      ▼
silver.ue.event
      │
      ▼
Flink Gold
      │
      ▼
gold.ue.sequence
      │
      ▼
Multi-model Inference Worker
      │
      ▼
anomaly-predictions
      │
      ▼
Streamlit Dashboard
```

Các mục tiêu chính:

- xử lý streaming theo nhiều tầng;
- tách Raw → Bronze → Silver → Gold;
- xử lý event-time đúng ngữ nghĩa;
- quản lý state theo UE;
- giảm training-serving skew bằng feature contract có version;
- hỗ trợ replay;
- không silently drop dữ liệu lỗi;
- giữ Kafka lineage;
- chạy nhiều model trên cùng một Gold window;
- hiển thị kết quả runtime bằng Streamlit.

---

# Mục lục

1. [Phạm vi đã triển khai](#1-phạm-vi-đã-triển-khai)
2. [Kiến trúc tổng thể](#2-kiến-trúc-tổng-thể)
3. [Kafka Topics](#3-kafka-topics)
4. [Log Producer](#4-log-producer)
5. [Tầng Bronze](#5-tầng-bronze)
6. [Tầng Silver](#6-tầng-silver)
7. [Event-time và Watermark](#7-event-time-và-watermark)
8. [Tầng Gold](#8-tầng-gold)
9. [Sequence và Feature Contract](#9-sequence-và-feature-contract)
10. [Inference Service](#10-inference-service)
11. [Streamlit Dashboard](#11-streamlit-dashboard)
12. [Processing Guarantees](#12-processing-guarantees)
13. [State và Fault Recovery](#13-state-và-fault-recovery)
14. [Replay dữ liệu](#14-replay-dữ-liệu)
15. [Runtime Configuration](#15-runtime-configuration)
16. [Build và khởi động](#16-build-và-khởi-động)
17. [Chạy Inference và Dashboard](#17-chạy-inference-và-dashboard)
18. [Monitoring](#18-monitoring)
19. [Cấu trúc Repository đã triển khai](#19-cấu-trúc-repository-đã-triển-khai)
20. [Các bất biến quan trọng](#20-các-bất-biến-quan-trọng)

---

# 1. Phạm vi đã triển khai

Repository hiện có code thực tế cho các thành phần sau:

| Thành phần | Trạng thái | Vai trò |
|---|---|---|
| Log Producer | Đã triển khai | Đọc file log và publish raw event vào Kafka |
| Kafka config | Đã triển khai | Khai báo topic và bootstrap server |
| Flink Bronze | Đã triển khai | Parse, validate, normalize timestamp, type conversion |
| Flink Silver | Đã triển khai | Resolve identity, normalize event, deduplicate, late routing |
| Flink Gold | Đã triển khai | Reorder theo UE, tạo sequence, encode model input |
| Inference Worker | Đã triển khai | Chạy multi-model inference từ `gold.ue.sequence` |
| Prediction Buffer | Đã triển khai | Đọc `anomaly-predictions` cho dashboard |
| Streamlit Dashboard | Đã triển khai | Hiển thị KPI, chart, chọn model, threshold và UE |
| Flink runtime scripts | Đã triển khai | Build, start, stop, submit, create topics, smoke test |

README này **không mô tả các placeholder chưa có implementation code** như một thành phần đang chạy.

---

# 2. Kiến trúc tổng thể

```text
                         Raw log files
                              │
                              ▼
                    ┌───────────────────┐
                    │   Log Producer    │
                    └─────────┬─────────┘
                              │
                              ▼
                      raw.ue.log.line
                              │
                              ▼
              ┌────────────────────────────┐
              │        Flink Bronze        │
              │       flink-bronze-v1      │
              │                            │
              │ parse envelope             │
              │ validate 52 fields         │
              │ normalize EVENT_TIME       │
              │ type conversion            │
              │ preserve Kafka metadata    │
              └─────────────┬──────────────┘
                            │
                            ▼
                    bronze.ue.event
                            │
                            ▼
              ┌────────────────────────────┐
              │        Flink Silver        │
              │       flink-silver-v1      │
              │                            │
              │ source watermark           │
              │ UE identity resolution     │
              │ event normalization        │
              │ deduplication              │
              │ keyBy ueKey                │
              │ late routing               │
              └─────────────┬──────────────┘
                            │
                            ▼
                    silver.ue.event
                            │
                            ▼
              ┌────────────────────────────┐
              │         Flink Gold         │
              │        flink-gold-v1       │
              │                            │
              │ keyBy ueKey                │
              │ per-UE reorder             │
              │ sliding sequence           │
              │ feature encoding           │
              │ evidence projection        │
              └─────────────┬──────────────┘
                            │
                            ▼
                    gold.ue.sequence
                            │
                            ▼
              ┌────────────────────────────┐
              │     Inference Worker       │
              │                            │
              │ IsolationForest            │
              │ MixedTransformer           │
              │ MFMT                       │
              └─────────────┬──────────────┘
                            │
                            ▼
                  anomaly-predictions
                            │
                            ▼
              ┌────────────────────────────┐
              │    Streamlit Dashboard     │
              │                            │
              │ model selector             │
              │ threshold selector         │
              │ KPI                        │
              │ timeline                   │
              │ UE investigation           │
              └────────────────────────────┘
```

Bronze, Silver và Gold là ba Flink job độc lập.

Kafka đóng vai trò durable boundary giữa các tầng.

---

# 3. Kafka Topics

Source of truth:

```text
config/kafka/topics.yaml
```

Bootstrap server:

```text
Docker internal : kafka:29092
Host / WSL      : localhost:9092
```

Cấu hình local:

```text
partitions         = 3
replication factor = 1
```

## Main topics

| Topic | Producer | Consumer |
|---|---|---|
| `raw.ue.log.line` | Log Producer | Flink Bronze |
| `bronze.ue.event` | Flink Bronze | Flink Silver |
| `silver.ue.event` | Flink Silver | Flink Gold |
| `gold.ue.sequence` | Flink Gold | Inference Worker |
| `anomaly-predictions` | Inference Worker | Streamlit Prediction Buffer |

## Side-output topics

| Topic | Layer | Ý nghĩa |
|---|---|---|
| `dlq.ue.log.line` | Bronze | Raw record parse/validate thất bại |
| `invalid-identity` | Silver | Không resolve được UE identity |
| `unsupported-event` | Silver | Event không nằm trong supported catalog |
| `late-ue-event` | Silver | Event bị route late tại Silver |
| `gold-too-late-event` | Gold | Event đến sau timeline đã finalized |
| `invalid-gold-feature` | Gold | Window không encode được theo feature contract |

Nguyên tắc:

> Dữ liệu lỗi phải có đường đi rõ ràng và không bị silently drop.

---

# 4. Log Producer

Module:

```text
log-producer/
```

Entry point:

```text
com.network.producer.LogProducerApplication
```

Code hiện có các nhóm chính:

```text
config/
factory/
kafka/
model/
reader/
serialization/
util/
```

Flow:

```text
input directory
      ↓
FileLogReader
      ↓
RawNetworkEventFactory
      ↓
RawNetworkEvent
      ↓
JSON serializer
      ↓
Kafka publisher
      ↓
raw.ue.log.line
```

Producer được chạy local bằng Maven:

```bash
mvn \
  -f log-producer/pom.xml \
  -Dexec.mainClass=com.network.producer.LogProducerApplication \
  -Dexec.args="/absolute/path/to/input-directory" \
  exec:java
```

Nên dùng absolute path cho input directory để tránh phụ thuộc Maven working directory.

---

# 5. Tầng Bronze

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
Envelope / JSON parse
        ↓
RawLogLineParser
        ↓
SchemaValidator
        ↓
TimestampNormalizer
        ↓
TypeCastOperator
        ↓
BronzeEvent
        ↓
bronze.ue.event
```

## Raw contract

```text
delimiter   = ;
field-count = 52
timezone    = Asia/Ho_Chi_Minh
```

Raw parser giữ trailing empty fields.

Timestamp raw được hiểu theo timezone Việt Nam và normalize về ISO-8601 UTC.

## Kafka lineage

Bronze giữ metadata nguồn để downstream dùng cho traceability và dedup:

```text
topic
partition
offset
```

## Bronze side output

```text
dlq.ue.log.line
```

Các lỗi parse/validation/type/timestamp được route sang DLQ thay vì bỏ im lặng.

---

# 6. Tầng Silver

Entry point:

```text
com.network.preprocess.silver.SilverJob
```

Flow:

```text
bronze.ue.event
        ↓
KafkaSource + timestamp/watermark
        ↓
UE identity resolution
        ↓
Event normalization
        ↓
keyBy source coordinates
        ↓
Deduplicate
        ↓
keyBy ueKey
        ↓
Late routing
        ↓
silver.ue.event
```

## UE identity

Canonical identity:

```text
ueKey
```

Silver có code cho:

```text
IdentityNormalizer
UeIdentityResolver
UeIdentityMappingLookup
MapBackedUeIdentityMappingLookup
```

Nếu không resolve được identity:

```text
invalid-identity
```

## Event normalization

Silver có catalog và normalizer cho:

```text
event ID
event result
supported event
```

Unsupported event được route sang:

```text
unsupported-event
```

## Deduplication

Dedup key dựa trên Kafka source coordinates:

```text
source topic
+
source partition
+
source offset
```

Không dùng `ueKey` làm dedup key.

---

# 7. Event-time và Watermark

Pipeline dùng event-time cho business ordering.

## Silver

Watermark được gắn tại Kafka source.

Cấu hình trong `application.yaml`:

```text
watermark max out-of-orderness = 30 seconds
watermark idleness             = 60 seconds
```

Source timestamp được lấy từ event time đã normalize ở Bronze.

Repository có các implementation:

```text
BronzeWatermarkStrategyFactory
SilverWatermarkStrategyFactory
SilverEventTimestampExtractor
SilverLateEventProcessFunction
```

## Gold

Gold đọc lại dữ liệu từ Kafka nên cần event-time handling riêng.

Business ordering của Gold dựa trên:

```text
ueKey
```

chứ không coi Kafka partition progress là UE timeline.

---

# 8. Tầng Gold

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

Các class chính đã triển khai:

```text
GoldSequenceProcessFunction
GoldSequenceEventMapper
GoldSequenceWindowFactory
GoldSequenceSampleFactory
GoldFeatureProcessFunction
GoldEvidenceFieldProjector
GoldSampleIdGenerator
```

Flow:

```text
SilverEvent
    ↓
keyBy ueKey
    ↓
per-UE reorder
    ↓
ordered UE timeline
    ↓
sliding sequence
    ↓
feature encoding
    ↓
evidence projection
    ↓
GoldSequenceSample
```

## Too-late event

Gold có side output:

```text
gold-too-late-event
```

Event được đánh giá late theo timeline đã finalized của cùng UE.

## Invalid feature

Nếu sequence không encode được theo contract:

```text
invalid-gold-feature
```

---

# 9. Sequence và Feature Contract

Source of truth:

```text
flink-preprocess/src/main/resources/application.yaml
```

Feature version hiện tại:

```text
gold-ue-sequence-feature-v2
```

Sequence:

```text
length          = 32
stride          = 8
partial windows = false
padding side    = LEFT
```

Đây là count-based sliding sequence.

Ví dụ:

```text
events 1..32  → window 1
events 9..40  → window 2
events 17..48 → window 3
```

## Model input

```text
x_cat[32][4]
x_num[32][2]
```

### Categorical

```text
dtype = INT64
```

Order:

```text
x_cat[:, 0] = event_code
x_cat[:, 1] = event_result_code
x_cat[:, 2] = normalized_cause_code
x_cat[:, 3] = sub_cause_code
```

### Numeric

```text
dtype = FLOAT32
```

Order:

```text
x_num[:, 0] = duration_ms
x_num[:, 1] = request_retries
```

Runtime sử dụng fixed vocabulary và normalization rules trong feature contract.

Nếu thay đổi shape, feature order, vocabulary, category ID, clipping, normalization hoặc sequence semantics thì phải tạo feature version mới và dùng model tương thích.

---

# 10. Inference Service

Code đã triển khai:

```text
inference-service/src/
├── gold_probe.py
├── model_probe.py
└── inference_worker.py
```

## Gold probe

`gold_probe.py` dùng để đọc một Gold record thực từ Kafka và kiểm tra contract runtime.

## Model probe

`model_probe.py` dùng Gold sample thật để kiểm tra production predictor.

## Inference worker

Entry point:

```text
inference-service/src/inference_worker.py
```

Kafka input:

```text
gold.ue.sequence
```

Kafka output:

```text
anomaly-predictions
```

Consumer group mặc định:

```text
inference-runtime-v1
```

Consumer config:

```text
auto.offset.reset = latest
enable.auto.commit = false
isolation.level = read_committed
```

## Multi-model inference

Worker đọc `artifact_metadata.json` từ production bundle local và load toàn bộ model được liệt kê.

Runtime hiện đã kiểm chứng với:

```text
IsolationForest
MixedTransformer
MFMT
```

Mỗi Gold window được chấm bởi tất cả model:

```text
Gold window
    │
    ├── IsolationForest
    ├── MixedTransformer
    └── MFMT
```

Do đó:

```text
1 Gold window → 3 predictions
```

## Prediction ID

Worker tạo deterministic ID:

```text
sample_id::model::selected_seed
```

Ví dụ:

```text
sample-X::IsolationForest::None
sample-X::MixedTransformer::123
sample-X::MFMT::3407
```

## Prediction payload

Output hiện có các nhóm field:

```text
prediction_schema_version
prediction_id

sample_id
ue_key
imsi
feature_version

window_start_event_time
window_end_event_time
sequence_length
stride

model
model_display_name
selected_seed
score_policy
forward_passes_per_window

raw_score
conformal_p_value
anomaly_score
anomaly_score_is_probability
alpha
is_anomaly

top_timestep_indices
top_timestep_raw_contributions
top_evidence_events

inference_time
source_kafka
```

`anomaly_score` không phải probability.

## Commit rule

Worker chỉ commit Gold offset sau khi toàn bộ model đã score và toàn bộ prediction đã được publish thành công.

```text
consume Gold
   ↓
validate contract
   ↓
score all models
   ↓
publish all predictions
   ↓
commit Gold offset
```

---

# 11. Streamlit Dashboard

Code đã triển khai:

```text
inference-service/demo/
├── prediction_buffer.py
└── runtime_app.py
```

Streamlit config:

```text
.streamlit/config.toml
```

Dashboard title:

```text
Hệ thống phát hiện thuê bao bất thường
```

## PredictionBuffer

`prediction_buffer.py` chạy Kafka consumer trong background thread:

```text
anomaly-predictions
        ↓
background thread
        ↓
OrderedDict
        ↓
snapshot()
        ↓
Streamlit
```

Consumer dashboard:

```text
auto.offset.reset = earliest
enable.auto.commit = false
```

Mục đích là cho phép Streamlit đọc lại topic và rebuild buffer sau restart.

Record được deduplicate theo:

```text
prediction_id
```

## Model selector

Dashboard lấy danh sách model từ production metadata local.

Hiện UI hỗ trợ:

```text
Isolation Forest
Mixed Transformer
MFMT
```

Đổi model chỉ filter prediction đã có, không chạy inference lại.

KPI, chart và danh sách UE thay đổi theo model đang chọn.

## Threshold selector

Dashboard cho phép chọn threshold hiển thị:

```text
display_is_anomaly
=
anomaly_score >= selected threshold
```

Threshold UI không ghi đè `is_anomaly` đã được inference worker sinh ra.

## KPI

Dashboard hiện hiển thị ba KPI:

```text
Số UE bất thường
Tỷ lệ request bất thường
Điểm bất thường trung bình
```

Trong implementation, KPI tỷ lệ được tính trên prediction/Gold window, không phải từng raw log line.

## Chart

Mỗi điểm trên chart là một Gold window.

Các điểm được sắp theo:

```text
window_end_event_time
```

và nối thành một đường.

Điểm vượt threshold UI được highlight màu cảnh báo.

Nếu người dùng chọn một UE trong phần cảnh báo, chart được filter chỉ hiển thị UE đó.

## UE investigation

Dashboard nhóm prediction theo `ue_key`.

Các mức hiển thị:

```text
Cần theo dõi
Cảnh báo lặp lại
Cảnh báo cao
```

UI có thể hiển thị:

- số window cảnh báo;
- max anomaly score;
- cảnh báo gần nhất;
- lịch sử cảnh báo;
- evidence event nếu model có timestep-level evidence.

---

# 12. Processing Guarantees

## Flink

Bronze, Silver và Gold sử dụng Kafka sinks theo EXACTLY_ONCE.

Downstream consumer sử dụng:

```text
isolation.level = read_committed
```

## Inference

Inference worker sử dụng:

```text
manual consumer commit
idempotent Kafka producer
```

Tuy nhiên Kafka output và Gold consumer offset không nằm trong cùng một transaction.

Boundary:

```text
gold.ue.sequence
→ inference worker
→ anomaly-predictions
```

vì vậy hiện nên xem là:

```text
at-least-once
```

Nếu worker crash sau produce nhưng trước commit input offset, prediction có thể được publish lại.

Deterministic `prediction_id` giúp downstream deduplicate.

---

# 13. State và Fault Recovery

Flink runtime config sử dụng RocksDB-backed state cho stateful processing.

Stateful logic hiện có:

```text
Silver:
- deduplication

Gold:
- per-UE reorder
- finalized timeline
- sliding sequence
- timer
```

State TTL:

```text
24 hours
```

Checkpoint config:

```text
checkpoint interval        = 60 seconds
checkpoint timeout         = 300 seconds
max concurrent checkpoint  = 1
minimum pause              = 30 seconds
```

Operator UID của stateful operator phải ổn định nếu cần restore state.

Thay đổi `keyBy`, UID, serializer hoặc state type phải được xem là state migration.

---

# 14. Replay dữ liệu

Kafka boundary cho phép replay theo từng layer.

## Gold-only

Dùng khi Silver output đã đúng nhưng Gold logic thay đổi.

## Silver + Gold

Dùng khi Bronze output đã đúng nhưng Silver logic thay đổi.

## Full replay

Dùng khi thay đổi Raw/Bronze contract, parser hoặc timestamp normalization.

Nguyên tắc:

```text
không reset nhiều layer hơn mức cần thiết
```

Khi replay:

```text
1. xác định boundary
2. dừng đúng downstream job
3. kiểm tra upstream Kafka data
4. quyết định consumer group/topic cần reset
5. không restore incompatible state
6. build đúng JAR
7. submit downstream trước upstream
8. chờ lag về 0
9. kiểm tra side output
```

---

# 15. Runtime Configuration

Local stack đã cấu hình:

```text
Java   17
Flink  1.20.1
Kafka  3.8.1
```

Kafka chạy single-node KRaft trong Docker.

Flink:

```text
parallelism mỗi job = 2
```

TaskManager config:

```text
2 TaskManagers
×
3 slots
=
6 task slots
```

Kafka:

```text
partitions         = 3
replication factor = 1
```

Kafka transaction timeout local:

```text
7200000 ms
```

---

# 16. Build và khởi động

## Build Flink JAR

```bash
./scripts/build-flink-job.sh
```

Deploy artifact được copy vào runtime path local:

```text
runtime/flink/usrlib/flink-preprocess-1.0.0-SNAPSHOT.jar
```

## Start Kafka + Flink

```bash
./scripts/start.sh
```

Script hiện thực hiện:

```text
start Kafka + Flink
→ wait JobManager
→ create/verify Kafka topics
→ build JAR nếu deploy JAR chưa tồn tại
→ submit missing Flink jobs
```

## Submit Flink jobs

```bash
./scripts/submit-flink-job.sh
```

Entry classes:

```text
com.network.preprocess.gold.GoldJob
com.network.preprocess.silver.SilverJob
com.network.preprocess.bronze.BronzeJob
```

Submit order:

```text
Gold
 ↓
Silver
 ↓
Bronze
```

## Verify

```bash
docker exec a-flink-jobmanager flink list -r
```

Expected:

```text
flink-gold-v1
flink-silver-v1
flink-bronze-v1
```

Không được có duplicate RUNNING instance của cùng job.

---

# 17. Chạy Inference và Dashboard

Production model artifacts không được commit vào Git.

Local runtime phải cung cấp production bundle tương thích cho predictor.

## Start inference worker

```bash
source .venv-inference/bin/activate

python inference-service/src/inference_worker.py
```

Khi bundle multi-model được load thành công:

```text
ALL MODELS READY | count=3
```

Trong một worker session:

```text
produced predictions
=
processed Gold windows
×
number of loaded models
```

Ví dụ:

```text
20 windows × 3 models = 60 predictions
```

## Start Streamlit

Terminal khác:

```bash
source .venv-inference/bin/activate

streamlit run \
  inference-service/demo/runtime_app.py \
  --server.headless true
```

Default URL:

```text
http://localhost:8501
```

---

# 18. Monitoring

## Flink

Theo dõi:

```text
job state
restart count
checkpoint success/failure
checkpoint duration
backpressure
TaskManager memory
RocksDB state
```

## Kafka

Theo dõi:

```text
consumer lag
partition lag
producer error
consumer error
transaction error
broker disk
throughput
```

## Data quality

Theo dõi:

```text
Bronze accepted
Bronze DLQ

Silver accepted
invalid identity
unsupported event
Silver late

Gold sequence
Gold too-late
invalid Gold feature
```

## Inference

Theo dõi:

```text
Gold consumer lag
processed windows
produced predictions
anomaly count per model
model load failure
prediction failure
```

Consumer group:

```bash
docker exec a-kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --group inference-runtime-v1
```

Khi xử lý hết backlog:

```text
LAG = 0
```

---

# 19. Cấu trúc Repository đã triển khai

```text
network-anomaly-detection/
│
├── .streamlit/
│   └── config.toml
│
├── config/
│   └── kafka/
│       └── topics.yaml
│
├── flink-preprocess/
│   ├── pom.xml
│   ├── README.md
│   │
│   └── src/
│       ├── main/
│       │   ├── java/
│       │   │   └── com/network/preprocess/
│       │   │       ├── bronze/
│       │   │       ├── config/
│       │   │       ├── gold/
│       │   │       ├── model/
│       │   │       ├── operator/
│       │   │       ├── parser/
│       │   │       ├── runtime/
│       │   │       ├── silver/
│       │   │       ├── sink/
│       │   │       ├── source/
│       │   │       ├── util/
│       │   │       └── validation/
│       │   │
│       │   └── resources/
│       │       ├── application.yaml
│       │       └── log4j2.properties
│       │
│       └── test/
│           └── java/
│
├── inference-service/
│   ├── README.md
│   │
│   ├── demo/
│   │   ├── prediction_buffer.py
│   │   └── runtime_app.py
│   │
│   └── src/
│       ├── gold_probe.py
│       ├── inference_worker.py
│       └── model_probe.py
│
├── log-producer/
│   ├── pom.xml
│   ├── README.md
│   │
│   └── src/
│       ├── main/
│       │   ├── java/
│       │   │   └── com/network/producer/
│       │   │       ├── LogProducerApplication.java
│       │   │       ├── config/
│       │   │       ├── factory/
│       │   │       ├── kafka/
│       │   │       ├── model/
│       │   │       ├── reader/
│       │   │       ├── serialization/
│       │   │       └── util/
│       │   │
│       │   └── resources/
│       │       └── application.properties
│       │
│       └── test/
│           └── java/
│
├── runtime/
│   └── inference/
│       ├── gold_contract_probe_sample.json
│       └── model_probe_prediction.json
│
├── schemas/
│   ├── raw-network-events.schema.json
│   ├── source/
│   │   └── raw-log-line-v1.json
│   └── examples/
│       └── raw-network-events.example.json
│
├── scripts/
│   ├── build-flink-job.sh
│   ├── create-gold-smoke-data.sh
│   ├── create-topics.sh
│   ├── start.sh
│   ├── stop.sh
│   ├── submit-flink-job.sh
│   └── test-pipeline.sh
│
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
---

# 20. Các bất biến quan trọng

## UE isolation

Một Gold sequence chỉ chứa event của một `ueKey`.

## Model shape

Với:

```text
gold-ue-sequence-feature-v2
```

shape phải là:

```text
x_cat[32][4]
x_num[32][2]
```

## Event ordering

Kafka partition progress không phải UE timeline.

Gold business ordering phải theo UE.

## Multi-model contract

Mọi model được load trong một production bundle phải nhận cùng Gold feature contract.

## Prediction identity

Prediction phải có deterministic identity:

```text
sample_id + model + selected_seed
```

## Inference accounting

Trong một worker session không lỗi:

```text
produced_predictions
=
processed_windows × loaded_model_count
```

## Dashboard semantics

Threshold trên Streamlit chỉ thay đổi cách UI đánh dấu và lọc cảnh báo.

Nó không sửa prediction gốc trong Kafka.


Multi-model runtime đã kiểm chứng:

```text
1 Gold window
→ IsolationForest prediction
→ MixedTransformer prediction
→ MFMT prediction
```