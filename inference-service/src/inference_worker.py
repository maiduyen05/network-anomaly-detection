from __future__ import annotations

import json
import logging
import os
import signal
import sys

from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer

# File này là service chính 

# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

MODEL_DIR = (
    PROJECT_ROOT
    / "inference-service"
    / "models"
    / "production"
)

METADATA_FILE = (
    MODEL_DIR
    / "artifact_metadata.json"
)


# ============================================================
# IMPORT PRODUCTION PREDICTOR
# ============================================================
#
# predictor.py của bundle import model_def.py cùng folder.
#
# Vì vậy thêm production bundle vào Python path.
#
# Không copy model code sang src/.
# Bundle production là source of truth.
# ============================================================

sys.path.insert(
    0,
    str(MODEL_DIR),
)

from predictor import AnomalyPredictor  # noqa: E402


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "%(levelname)s "
        "%(name)s - "
        "%(message)s"
    ),
)

LOGGER = logging.getLogger(
    "lte-anomaly-inference"
)


# ============================================================
# KAFKA CONFIG
# ============================================================
#
# Worker chạy trực tiếp trong WSL.
#
# Kafka Docker expose ra host:
#
#     localhost:9092
#
# kafka:29092 chỉ dùng bên trong Docker network.
# ============================================================

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

INPUT_TOPIC = os.getenv(
    "KAFKA_INPUT_TOPIC",
    "gold.ue.sequence",
)

OUTPUT_TOPIC = os.getenv(
    "KAFKA_OUTPUT_TOPIC",
    "anomaly-predictions",
)


# ============================================================
# RUNTIME CONSUMER GROUP
# ============================================================
#
# Group này KHÔNG được trùng với:
#
#     flink-gold-v1
#     gold-contract-probe-v1
#
# Đây là consumer group riêng của production inference.
# ============================================================

CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "inference-runtime-v1",
)


EXPECTED_FEATURE_VERSION = (
    "gold-ue-sequence-feature-v2"
)

EXPECTED_SEQUENCE_LENGTH = 32

EXPECTED_CAT_FEATURE_COUNT = 4

EXPECTED_NUM_FEATURE_COUNT = 2


# ============================================================
# GRACEFUL SHUTDOWN
# ============================================================

RUNNING = True


def handle_shutdown(
    signum,
    frame,
):
    """
    Ctrl+C không kill process giữa lúc đang produce.

    Chỉ đánh dấu vòng lặp dừng.
    finally sẽ close Kafka cleanly.
    """

    global RUNNING

    LOGGER.info(
        "Shutdown requested"
    )

    RUNNING = False


signal.signal(
    signal.SIGINT,
    handle_shutdown,
)

signal.signal(
    signal.SIGTERM,
    handle_shutdown,
)


# ============================================================
# UTILITY
# ============================================================

def utc_now() -> str:
    """
    Processing/inference timestamp.

    Đây không phải LTE EVENT_TIME.
    """

    return (
        datetime
        .now(timezone.utc)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def require(
    condition: bool,
    message: str,
) -> None:
    """
    Fail-fast khi contract không đúng.
    """

    if not condition:
        raise ValueError(
            message
        )


# ============================================================
# GOLD VALIDATION
# ============================================================

def validate_gold_record(
    record: dict,
    predictor: AnomalyPredictor,
) -> None:
    """
    Kiểm tra những điều quan trọng nhất trước inference.

    Predictor cũng có validation riêng,
    nhưng worker kiểm tra sớm để lỗi dễ hiểu hơn.
    """

    require(
        isinstance(
            record,
            dict,
        ),
        "Gold value must be a JSON object",
    )


    # --------------------------------------------------------
    # FEATURE VERSION
    # --------------------------------------------------------

    actual_feature_version = (
        record.get(
            "feature_version"
        )
    )

    model_feature_version = (
        predictor
        .meta[
            "feature_contract"
        ][
            "feature_version"
        ]
    )


    require(
        actual_feature_version
        == model_feature_version,
        (
            "Gold/model feature contract mismatch: "
            f"Gold={actual_feature_version!r}, "
            f"Model={model_feature_version!r}"
        ),
    )


    # --------------------------------------------------------
    # WINDOW
    # --------------------------------------------------------

    require(
        record.get(
            "sequence_length"
        )
        == EXPECTED_SEQUENCE_LENGTH,
        (
            "Unexpected sequence_length: "
            f"{record.get('sequence_length')!r}"
        ),
    )


    # --------------------------------------------------------
    # MODEL INPUT
    # --------------------------------------------------------

    model_input = record.get(
        "model_input"
    )


    require(
        isinstance(
            model_input,
            dict,
        ),
        "model_input missing",
    )


    x_cat = model_input.get(
        "x_cat"
    )

    x_num = model_input.get(
        "x_num"
    )


    require(
        isinstance(
            x_cat,
            list,
        )
        and
        len(x_cat)
        == EXPECTED_SEQUENCE_LENGTH,
        (
            "x_cat must have "
            f"{EXPECTED_SEQUENCE_LENGTH} timesteps"
        ),
    )


    require(
        isinstance(
            x_num,
            list,
        )
        and
        len(x_num)
        == EXPECTED_SEQUENCE_LENGTH,
        (
            "x_num must have "
            f"{EXPECTED_SEQUENCE_LENGTH} timesteps"
        ),
    )


    for index, row in enumerate(
        x_cat
    ):

        require(
            isinstance(
                row,
                list,
            )
            and
            len(row)
            == EXPECTED_CAT_FEATURE_COUNT,
            (
                "Invalid x_cat shape at "
                f"timestep={index}"
            ),
        )


    for index, row in enumerate(
        x_num
    ):

        require(
            isinstance(
                row,
                list,
            )
            and
            len(row)
            == EXPECTED_NUM_FEATURE_COUNT,
            (
                "Invalid x_num shape at "
                f"timestep={index}"
            ),
        )


# ============================================================
# TOP EVIDENCE
# ============================================================

def extract_top_evidence(
    gold_record: dict,
    prediction: dict,
) -> list[dict]:
    """
    Predictor trả về các timestep có reconstruction contribution cao.

    Ví dụ:

        top_timestep_indices = [12, 15, 7, ...]

    Gold giữ 32 evidence event.

    Ta nối:

        timestep 12
            ↓
        evidence.events[12]

    để Streamlit sau này có thể hiển thị
    event nào đóng góp mạnh vào anomaly score.
    """

    evidence = (
        gold_record
        .get(
            "evidence",
            {},
        )
    )


    events = (
        evidence.get(
            "events",
            []
        )
        or []
    )


    indices = (
        prediction.get(
            "top_timestep_indices",
            [],
        )
        or []
    )


    contributions = (
        prediction.get(
            "top_timestep_raw_contributions",
            [],
        )
        or []
    )


    output = []


    for rank, timestep in enumerate(
        indices
    ):

        if not isinstance(
            timestep,
            int,
        ):
            continue


        if (
            timestep < 0
            or
            timestep >= len(events)
        ):
            continue


        contribution = None


        if rank < len(
            contributions
        ):

            contribution = float(
                contributions[
                    rank
                ]
            )


        output.append(
            {
                "timestep":
                    timestep,

                "raw_contribution":
                    contribution,

                "event":
                    events[
                        timestep
                    ],
            }
        )


    return output


# ============================================================
# OUTPUT MESSAGE
# ============================================================

def build_output_record(
    gold_record: dict,
    prediction: dict,
    predictor: AnomalyPredictor,
    kafka_message,
) -> dict:
    """
    Tạo contract output cho anomaly-predictions.

    File JSON Schema trong repo hiện chưa được định nghĩa,
    nên worker ghi rõ:

        prediction_schema_version =
            anomaly-prediction-v1

    Sau khi pipeline chạy ổn,
    ta sẽ formalize schema này.
    """

    sample_id = str(
        prediction[
            "sample_id"
        ]
    )

    model_name = str(
        prediction[
            "model"
        ]
    )

    selected_seed = (
        prediction.get(
            "selected_seed"
        )
    )


    # --------------------------------------------------------
    # DETERMINISTIC PREDICTION ID
    # --------------------------------------------------------
    #
    # Worker hiện dùng delivery kiểu at-least-once.
    #
    # Nếu crash đúng lúc:
    #
    # produce prediction thành công
    #     ↓
    # chưa commit Gold offset
    #     ↓
    # restart
    #     ↓
    # Gold message được đọc lại
    #
    # prediction_id deterministic giúp dashboard
    # deduplicate cùng một prediction.
    # --------------------------------------------------------

    prediction_id = (
        f"{sample_id}"
        f"::{model_name}"
        f"::{selected_seed}"
    )


    deployment = (
        predictor
        .meta
        .get(
            "deployment",
            {},
        )
    )


    return {

        # ====================================================
        # OUTPUT CONTRACT
        # ====================================================

        "prediction_schema_version":
            "anomaly-prediction-v1",

        "prediction_id":
            prediction_id,


        # ====================================================
        # GOLD IDENTITY
        # ====================================================

        "sample_id":
            sample_id,

        "ue_key":
            str(
                prediction[
                    "ue_key"
                ]
            ),

        "imsi":
            str(
                gold_record.get(
                    "imsi",
                    "",
                )
            ),

        "feature_version":
            gold_record.get(
                "feature_version"
            ),


        # ====================================================
        # WINDOW
        # ====================================================

        "window_start_event_time":
            gold_record.get(
                "window_start_event_time"
            ),

        "window_end_event_time":
            gold_record.get(
                "window_end_event_time"
            ),

        "sequence_length":
            gold_record.get(
                "sequence_length"
            ),

        "stride":
            gold_record.get(
                "stride"
            ),


        # ====================================================
        # MODEL
        # ====================================================

        "model":
            prediction[
                "model"
            ],

        "model_display_name":
            prediction.get(
                "model_display_name",
                prediction[
                    "model"
                ],
            ),

        "selected_seed":
            prediction.get(
                "selected_seed"
            ),

        "score_policy":
            prediction.get(
                "score_policy"
            ),

        "forward_passes_per_window":
            prediction.get(
                "forward_passes_per_window",
                1,
            ),


        # ====================================================
        # ANOMALY RESULT
        # ====================================================

        "raw_score":
            float(
                prediction[
                    "raw_score"
                ]
            ),

        "conformal_p_value":
            float(
                prediction[
                    "conformal_p_value"
                ]
            ),

        # anomaly_score = 1 - conformal p-value
        #
        # Đây KHÔNG phải probability.
        "anomaly_score":
            float(
                prediction[
                    "anomaly_score"
                ]
            ),

        "anomaly_score_is_probability":
            False,

        "alpha":
            float(
                prediction[
                    "alpha"
                ]
            ),

        "is_anomaly":
            bool(
                prediction[
                    "is_anomaly"
                ]
            ),


        # ====================================================
        # EXPLANATION / EVIDENCE
        # ====================================================

        "top_timestep_indices":
            prediction.get(
                "top_timestep_indices",
                [],
            ),

        "top_timestep_raw_contributions":
            prediction.get(
                "top_timestep_raw_contributions",
                [],
            ),

        "top_evidence_events":
            extract_top_evidence(
                gold_record=
                    gold_record,

                prediction=
                    prediction,
            ),


        # ====================================================
        # RUNTIME
        # ====================================================

        "inference_time":
            utc_now(),


        # ====================================================
        # KAFKA LINEAGE
        # ====================================================

        "source_kafka":
            {
                "topic":
                    kafka_message.topic(),

                "partition":
                    kafka_message.partition(),

                "offset":
                    kafka_message.offset(),
            },
    }


# ============================================================
# KAFKA CONSUMER
# ============================================================

def create_consumer() -> Consumer:
    """
    Runtime consumer Gold.

    auto.offset.reset = latest
    --------------------------

    Đây là CHỦ Ý cho runtime demo.

    inference-runtime-v1 chưa có committed offset.
    Khi worker start trước data/runtime:

        Gold development records
               ↑
             skip

        worker starts here
               ↓

        Gold runtime records
               ↓
             consume


    enable.auto.commit = False
    --------------------------

    Chỉ commit Gold message sau khi:

        inference thành công
            +
        prediction đã gửi Kafka thành công.


    isolation.level = read_committed
    --------------------------------

    Gold Flink sink dùng Kafka transaction,
    nên chỉ đọc transaction đã commit.
    """

    return Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                CONSUMER_GROUP,

            "auto.offset.reset":
                "latest",

            "enable.auto.commit":
                False,

            "isolation.level":
                "read_committed",

            "client.id":
                "lte-anomaly-inference-consumer",
        }
    )


# ============================================================
# KAFKA PRODUCER
# ============================================================

def create_producer() -> Producer:
    """
    Producer gửi prediction sang anomaly-predictions.

    enable.idempotence = True:
        hỗ trợ producer retry an toàn hơn.

    acks = all:
        broker xác nhận sau khi ghi theo durability policy.
    """

    return Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "client.id":
                "lte-anomaly-inference-producer",

            "acks":
                "all",

            "enable.idempotence":
                True,
        }
    )


# ============================================================
# SEND OUTPUT
# ============================================================

def send_prediction(
    producer: Producer,
    record: dict,
) -> None:
    """
    Gửi một anomaly prediction.

    Ở demo này ta flush mỗi prediction.

    Ưu điểm:
        dễ kiểm chứng correctness.

    Nhược:
        throughput thấp hơn batching.

    Sau khi demo chạy đúng,
    mới tối ưu batching.
    """

    payload = json.dumps(
        record,
        ensure_ascii=False,
        separators=(
            ",",
            ":",
        ),
    )


    ue_key = str(
        record[
            "ue_key"
        ]
    )


    producer.produce(
        topic=
            OUTPUT_TOPIC,

        key=
            ue_key.encode(
                "utf-8"
            ),

        value=
            payload.encode(
                "utf-8"
            ),
    )


    # Đợi message thực sự được gửi khỏi local queue.
    remaining = producer.flush(
        timeout=10.0
    )


    if remaining != 0:

        raise RuntimeError(
            (
                "Kafka producer still has "
                f"{remaining} "
                "undelivered record(s)"
            )
        )


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    # --------------------------------------------------------
    # 1. MODEL DIRECTORY
    # --------------------------------------------------------

    if not MODEL_DIR.exists():

        raise FileNotFoundError(
            (
                "Production model directory "
                f"not found: {MODEL_DIR}"
            )
        )


    # --------------------------------------------------------
    # 2. LOAD ALL PRODUCTION MODELS
    # --------------------------------------------------------

    if not METADATA_FILE.exists():

        raise FileNotFoundError(
            f"Metadata not found: {METADATA_FILE}"
        )


    metadata = json.loads(
        METADATA_FILE.read_text(
            encoding="utf-8"
        )
    )


    deployment = metadata.get(
        "deployment",
        {},
    )

    available_models = list(
        deployment.get(
            "available_models",
            metadata.get(
                "models",
                {},
            ).keys(),
        )
    )

    if not available_models:

        raise RuntimeError(
            "Production bundle contains no models"
        )


    LOGGER.info(
        "Available production models: %s",
        available_models,
    )


    predictors: dict[str, AnomalyPredictor] = {}

    for model_name in available_models:

        LOGGER.info(
            "Loading model=%s ...",
            model_name,
        )

        try:
            predictor = AnomalyPredictor(
                MODEL_DIR,
                model_name=model_name,
            )
        except TypeError:
            predictor = AnomalyPredictor(
                MODEL_DIR
            )
            if model_name != predictor.model_name:
                raise

        predictors[model_name] = predictor

        LOGGER.info(
            (
                "MODEL READY | "
                "model=%s | "
                "display=%s | "
                "seed=%s | "
                "alpha=%s | "
                "forward_passes=%s"
            ),
            predictor.model_name,
            getattr(
                predictor,
                "model_display_name",
                predictor.model_name,
            ),
            getattr(
                predictor,
                "selected_seed",
                None,
            ),
            getattr(
                predictor,
                "alpha",
                None,
            ),
            getattr(
                predictor,
                "forward_passes_per_window",
                1,
            ),
        )


    contract_predictor = predictors[
        available_models[0]
    ]


    LOGGER.info(
        "ALL MODELS READY | count=%d",
        len(predictors),
    )


    # --------------------------------------------------------
    # 3. CREATE KAFKA CLIENTS
    # --------------------------------------------------------

    consumer = create_consumer()

    producer = create_producer()


    # --------------------------------------------------------
    # 4. SUBSCRIBE
    # --------------------------------------------------------

    consumer.subscribe(
        [
            INPUT_TOPIC
        ]
    )


    LOGGER.info(
        "Kafka bootstrap=%s",
        BOOTSTRAP_SERVERS,
    )

    LOGGER.info(
        "Consumer group=%s",
        CONSUMER_GROUP,
    )

    LOGGER.info(
        "Subscribed input=%s",
        INPUT_TOPIC,
    )

    LOGGER.info(
        "Prediction output=%s",
        OUTPUT_TOPIC,
    )

    LOGGER.info(
        "Waiting for NEW runtime Gold samples..."
    )


    # --------------------------------------------------------
    # RUNTIME COUNTERS
    # --------------------------------------------------------

    processed_count = 0

    anomaly_count = 0


    try:

        while RUNNING:

            message = consumer.poll(
                timeout=1.0
            )


            if message is None:
                continue


            if message.error():

                raise RuntimeError(
                    (
                        "Kafka consumer error: "
                        f"{message.error()}"
                    )
                )


            # ------------------------------------------------
            # 5. DESERIALIZE GOLD
            # ------------------------------------------------

            raw_value = (
                message
                .value()
                .decode(
                    "utf-8"
                )
            )


            gold_record = json.loads(
                raw_value
            )


            # ------------------------------------------------
            # 6. VALIDATE CONTRACT
            # ------------------------------------------------

            validate_gold_record(
                record=
                    gold_record,

                predictor=
                    predictor,
            )


            # ------------------------------------------------
            # 7. MODEL INFERENCE - ALL MODELS FOR THE SAME GOLD WINDOW
            # ------------------------------------------------

            outputs_for_window = []

            for model_name, predictor in predictors.items():

                prediction = (
                    predictor
                    .score_record(
                        gold_record
                    )
                )

                output_record = (
                    build_output_record(
                        gold_record=
                            gold_record,

                        prediction=
                            prediction,

                        predictor=
                            predictor,

                        kafka_message=
                            message,
                    )
                )

                send_prediction(
                    producer=
                        producer,

                    record=
                        output_record,
                )

                outputs_for_window.append(
                    output_record
                )


            # ------------------------------------------------
            # 8. COMMIT INPUT OFFSET AFTER ALL MODELS SUCCEED
            # ------------------------------------------------

            committed = consumer.commit(
                message=
                    message,

                asynchronous=
                    False,
            )

            if committed:

                for partition in committed:

                    if partition.error:

                        raise RuntimeError(
                            (
                                "Kafka commit failed: "
                                f"{partition}"
                            )
                        )


            # ------------------------------------------------
            # 9. COUNTERS
            # ------------------------------------------------

            processed_count += 1

            for output_record in outputs_for_window:

                if output_record[
                    "is_anomaly"
                ]:

                    anomaly_count += 1


            if processed_count % 20 == 0:

                LOGGER.info(
                    (
                        "WINDOW COMPLETE | "
                        "windows=%d | "
                        "models=%d | "
                        "ue=%s | "
                        "sample=%s"
                    ),
                    processed_count,
                    len(outputs_for_window),
                    gold_record.get(
                        "ue_key"
                    ),
                    gold_record.get(
                        "sample_id"
                    ),
                )


            for output_record in outputs_for_window:

                if output_record[
                    "is_anomaly"
                ]:

                    LOGGER.info(
                        (
                            "ANOMALY | "
                            "model=%s | "
                            "ue=%s | "
                            "sample=%s | "
                            "p=%.6f | "
                            "score=%.6f"
                        ),
                        output_record[
                            "model"
                        ],
                        output_record[
                            "ue_key"
                        ],
                        output_record[
                            "sample_id"
                        ],
                        output_record[
                            "conformal_p_value"
                        ],
                        output_record[
                            "anomaly_score"
                        ],
                    )


    finally:

        LOGGER.info(
            (
                "Stopping worker | "
                "processed=%d | "
                "anomalies=%d"
            ),
            processed_count,
            anomaly_count,
        )


        producer.flush(
            10.0
        )


        consumer.close()


    return 0


if __name__ == "__main__":

    raise SystemExit(
        main()
    )