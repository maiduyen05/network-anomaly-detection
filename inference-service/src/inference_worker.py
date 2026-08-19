from __future__ import annotations

import json
import logging
import os
import signal
import sys

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer


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


# ============================================================
# IMPORT PRODUCTION PREDICTOR
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
# CONSUMER GROUP
# ============================================================
#
# Giữ nguyên group runtime cũ.
#
# Vì group này đã có committed offsets từ worker trước,
# worker mới sẽ tiếp tục từ đúng vị trí đã xử lý.
#
# Không dùng group mới để tránh đọc lại Gold runtime cũ.
# ============================================================

CONSUMER_GROUP = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "inference-runtime-v1",
)


# ============================================================
# GOLD CONTRACT
# ============================================================

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
) -> None:

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

    return (
        datetime
        .now(
            timezone.utc
        )
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

    if not condition:

        raise ValueError(
            message
        )


# ============================================================
# LOAD METADATA
# ============================================================

def load_metadata() -> dict:

    metadata_file = (
        MODEL_DIR
        / "artifact_metadata.json"
    )


    if not metadata_file.exists():

        raise FileNotFoundError(
            (
                "Production metadata not found: "
                f"{metadata_file}"
            )
        )


    return json.loads(
        metadata_file.read_text(
            encoding="utf-8"
        )
    )


# ============================================================
# LOAD ALL MODELS
# ============================================================

def load_predictors(
) -> tuple[
    dict[str, AnomalyPredictor],
    list[str],
]:
    """
    Load tất cả model trong production bundle một lần.

    Bundle hiện tại:

        IsolationForest
        MixedTransformer
        MFMT

    Không load model lại cho từng Gold window.
    """

    metadata = load_metadata()


    deployment = (
        metadata[
            "deployment"
        ]
    )


    models_metadata = (
        metadata[
            "models"
        ]
    )


    available_models = list(
        deployment.get(
            "available_models",
            models_metadata.keys(),
        )
    )


    if not available_models:

        raise RuntimeError(
            (
                "No model found in "
                "production bundle"
            )
        )


    LOGGER.info(
        (
            "Production bundle | "
            "models=%s | "
            "default=%s"
        ),
        available_models,
        deployment.get(
            "default_model"
        ),
    )


    predictors: dict[
        str,
        AnomalyPredictor,
    ] = {}


    for model_name in available_models:

        LOGGER.info(
            "Loading model=%s ...",
            model_name,
        )


        predictor = (
            AnomalyPredictor(
                MODEL_DIR,
                model_name=model_name,
            )
        )


        predictors[
            model_name
        ] = predictor


    LOGGER.info(
        (
            "MODEL READY | "
            "model=%s | "
            "display=%s | "
            "seed=%s | "
            "threshold=%.3f | "
            "score_policy=%s | "
            "forward_passes=%s"
        ),
        predictor.model_name,
        predictor.model_display_name,
        predictor.selected_seed,
        predictor.default_threshold,
        predictor.score_policy,
        predictor.forward_passes_per_window,
    )


    LOGGER.info(
        (
            "ALL MODELS READY | "
            "count=%d"
        ),
        len(
            predictors
        ),
    )


    return (
        predictors,
        available_models,
    )


# ============================================================
# GOLD VALIDATION
# ============================================================

def validate_gold_record(
    record: dict,
    contract_predictor: AnomalyPredictor,
) -> None:
    """
    Validate Gold contract một lần trước khi đưa
    cùng Gold window cho cả ba model.

    Cả ba model dùng cùng:
        gold-ue-sequence-feature-v2
    """

    require(
        isinstance(
            record,
            dict,
        ),
        (
            "Gold value must be "
            "a JSON object"
        ),
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
        contract_predictor
        .meta[
            "feature_contract"
        ][
            "feature_version"
        ]
    )


    require(
        actual_feature_version
        ==
        model_feature_version,
        (
            "Gold/model feature mismatch: "
            f"Gold={actual_feature_version!r}, "
            f"Model={model_feature_version!r}"
        ),
    )


    # --------------------------------------------------------
    # SEQUENCE LENGTH
    # --------------------------------------------------------

    require(
        record.get(
            "sequence_length"
        )
        ==
        EXPECTED_SEQUENCE_LENGTH,
        (
            "Unexpected sequence_length: "
            f"{record.get('sequence_length')!r}"
        ),
    )


    # --------------------------------------------------------
    # MODEL INPUT
    # --------------------------------------------------------

    model_input = (
        record.get(
            "model_input"
        )
    )


    require(
        isinstance(
            model_input,
            dict,
        ),
        "model_input missing",
    )


    x_cat = (
        model_input.get(
            "x_cat"
        )
    )

    x_num = (
        model_input.get(
            "x_num"
        )
    )


    require(
        isinstance(
            x_cat,
            list,
        )
        and
        len(
            x_cat
        )
        ==
        EXPECTED_SEQUENCE_LENGTH,
        (
            "x_cat must contain "
            f"{EXPECTED_SEQUENCE_LENGTH} timesteps"
        ),
    )


    require(
        isinstance(
            x_num,
            list,
        )
        and
        len(
            x_num
        )
        ==
        EXPECTED_SEQUENCE_LENGTH,
        (
            "x_num must contain "
            f"{EXPECTED_SEQUENCE_LENGTH} timesteps"
        ),
    )


    for (
        timestep,
        row,
    ) in enumerate(
        x_cat
    ):

        require(
            isinstance(
                row,
                list,
            )
            and
            len(
                row
            )
            ==
            EXPECTED_CAT_FEATURE_COUNT,
            (
                "Invalid x_cat shape "
                f"at timestep={timestep}"
            ),
        )


    for (
        timestep,
        row,
    ) in enumerate(
        x_num
    ):

        require(
            isinstance(
                row,
                list,
            )
            and
            len(
                row
            )
            ==
            EXPECTED_NUM_FEATURE_COUNT,
            (
                "Invalid x_num shape "
                f"at timestep={timestep}"
            ),
        )


# ============================================================
# EXTRACT TOP EVIDENCE
# ============================================================

def extract_top_evidence(
    gold_record: dict,
    prediction: dict,
) -> list[dict]:
    """
    MixedTransformer và MFMT có:
        top_timestep_indices

    IsolationForest không có timestep-level
    reconstruction contribution nên trả [].
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
            [],
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


    for (
        rank,
        timestep,
    ) in enumerate(
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
            timestep >= len(
                events
            )
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
# BUILD OUTPUT
# ============================================================

def build_output_record(
    gold_record: dict,
    prediction: dict,
    kafka_message,
) -> dict:
    """
    Chuyển model prediction thành Kafka output contract.
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
    # DETERMINISTIC ID
    # --------------------------------------------------------
    #
    # Cùng sample nhưng ba model:
    #
    # sample-X::IsolationForest::None
    # sample-X::MixedTransformer::123
    # sample-X::MFMT::3407
    #
    # nên Streamlit có thể deduplicate độc lập.
    # --------------------------------------------------------

    prediction_id = (
        f"{sample_id}"
        f"::{model_name}"
        f"::{selected_seed}"
    )


    return {

        # ====================================================
        # CONTRACT
        # ====================================================

        "prediction_schema_version":
            "anomaly-prediction-v2",

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
        # GOLD WINDOW
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
        # MODEL IDENTITY
        # ====================================================

        "model":
            model_name,

        "model_display_name":
            prediction.get(
                "model_display_name",
                model_name,
            ),

        "selected_seed":
            selected_seed,

        "score_policy":
            prediction.get(
                "score_policy"
            ),

        "forward_passes_per_window":
            int(
                prediction.get(
                    "forward_passes_per_window",
                    0,
                )
            ),


        # ====================================================
        # SCORE
        # ====================================================

        "raw_score":
            float(
                prediction[
                    "raw_score"
                ]
            ),

        "anomaly_score":
            float(
                prediction[
                    "anomaly_score"
                ]
            ),

        # ECDF score trong [0, 1], không phải probability.
        "anomaly_score_is_probability":
            False,

        "anomaly_threshold":
            float(
                prediction[
                    "anomaly_threshold"
                ]
            ),

        "is_anomaly":
            bool(
                prediction[
                    "is_anomaly"
                ]
            ),


        # ====================================================
        # EXPLANATION
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

    return Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                CONSUMER_GROUP,

            # Chỉ dùng khi group chưa có committed offset.
            #
            # Group inference-runtime-v1 hiện đã có offset,
            # nên worker tiếp tục từ vị trí cũ.
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
# SEND ONE PREDICTION
# ============================================================

def send_prediction(
    producer: Producer,
    record: dict,
) -> None:
    """
    Gửi một model prediction.

    Hiện flush từng prediction để ưu tiên correctness,
    phù hợp demo.

    Sau này có thể batch nếu cần throughput cao hơn.
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

        # Giữ cùng UE vào cùng partition.
        key=
            ue_key.encode(
                "utf-8"
            ),

        value=
            payload.encode(
                "utf-8"
            ),
    )


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
# COMMIT GOLD OFFSET
# ============================================================

def commit_gold_message(
    consumer: Consumer,
    message,
) -> None:
    """
    Chỉ gọi sau khi CẢ BA prediction đã produce thành công.
    """

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
                        "Kafka offset commit failed: "
                        f"{partition}"
                    )
                )


# ============================================================
# MAIN
# ============================================================

def main() -> int:


    # --------------------------------------------------------
    # MODEL DIRECTORY
    # --------------------------------------------------------

    if not MODEL_DIR.exists():

        raise FileNotFoundError(
            (
                "Production model directory "
                f"not found: {MODEL_DIR}"
            )
        )


    # ========================================================
    # LOAD ALL THREE MODELS ONCE
    # ========================================================

    (
        predictors,
        available_models,
    ) = load_predictors()


    # Predictor đầu tiên chỉ dùng để validate contract.
    contract_predictor = (
        predictors[
            available_models[
                0
            ]
        ]
    )


    # ========================================================
    # KAFKA CLIENTS
    # ========================================================

    consumer = create_consumer()

    producer = create_producer()


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
        (
            "Waiting for NEW runtime Gold samples "
            "| scoring each window with %d models..."
        ),
        len(
            available_models
        ),
    )


    # ========================================================
    # COUNTERS
    # ========================================================

    processed_windows = 0

    produced_predictions = 0


    anomalies_by_model = defaultdict(
        int
    )


    try:

        while RUNNING:


            # =================================================
            # POLL GOLD
            # =================================================

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


            # =================================================
            # DESERIALIZE
            # =================================================

            raw_value = (
                message
                .value()
                .decode(
                    "utf-8"
                )
            )


            gold_record = (
                json.loads(
                    raw_value
                )
            )


            # =================================================
            # VALIDATE GOLD ONCE
            # =================================================

            validate_gold_record(
                record=
                    gold_record,

                contract_predictor=
                    contract_predictor,
            )


            # =================================================
            # SCORE SAME WINDOW WITH ALL MODELS
            # =================================================
            #
            # QUAN TRỌNG:
            #
            # Không commit input offset ở giữa vòng lặp.
            #
            # Cả 3 model phải:
            #
            #     score success
            #     +
            #     output Kafka success
            #
            # rồi mới commit Gold.
            # =================================================

            window_outputs = []


            for model_name in available_models:


                predictor = (
                    predictors[
                        model_name
                    ]
                )


                # ---------------------------------------------
                # INFERENCE
                # ---------------------------------------------

                prediction = (
                    predictor
                    .score_record(
                        gold_record
                    )
                )


                # ---------------------------------------------
                # OUTPUT
                # ---------------------------------------------

                output_record = (
                    build_output_record(
                        gold_record=
                            gold_record,

                        prediction=
                            prediction,

                        kafka_message=
                            message,
                    )
                )


                # ---------------------------------------------
                # KAFKA PRODUCE
                # ---------------------------------------------

                send_prediction(
                    producer=
                        producer,

                    record=
                        output_record,
                )


                window_outputs.append(
                    output_record
                )


            # =================================================
            # ALL THREE OUTPUTS SUCCESS
            # =================================================
            #
            # Bây giờ mới được commit Gold input offset.
            # =================================================

            commit_gold_message(
                consumer=
                    consumer,

                message=
                    message,
            )


            # =================================================
            # COUNTERS
            # =================================================

            processed_windows += 1

            produced_predictions += len(
                window_outputs
            )


            for output in window_outputs:

                if output[
                    "is_anomaly"
                ]:

                    anomalies_by_model[
                        output[
                            "model"
                        ]
                    ] += 1


            # =================================================
            # LOG ANOMALIES
            # =================================================

            for output in window_outputs:

                if not output[
                    "is_anomaly"
                ]:

                    continue


                LOGGER.info(
                    (
                        "ANOMALY | "
                        "model=%s | "
                        "ue=%s | "
                        "sample=%s | "
                        "raw=%.6f | "
                        "score=%.6f | "
                        "threshold=%.3f"
                    ),
                    output["model"],
                    output["ue_key"],
                    output["sample_id"],
                    output["raw_score"],
                    output["anomaly_score"],
                    output["anomaly_threshold"],
                )


            # =================================================
            # PERIODIC LOG
            # =================================================

            if (
                processed_windows <= 3
                or
                processed_windows % 20 == 0
            ):

                LOGGER.info(
                    (
                        "WINDOW COMPLETE | "
                        "windows=%d | "
                        "predictions=%d | "
                        "models=%d | "
                        "ue=%s | "
                        "sample=%s | "
                        "anomalies=%s"
                    ),
                    processed_windows,
                    produced_predictions,
                    len(
                        available_models
                    ),
                    gold_record.get(
                        "ue_key"
                    ),
                    gold_record.get(
                        "sample_id"
                    ),
                    dict(
                        anomalies_by_model
                    ),
                )


    finally:


        LOGGER.info(
            (
                "Stopping worker | "
                "windows=%d | "
                "predictions=%d | "
                "anomalies=%s"
            ),
            processed_windows,
            produced_predictions,
            dict(
                anomalies_by_model
            ),
        )


        producer.flush(
            10.0
        )


        consumer.close()


    return 0


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":

    raise SystemExit(
        main()
    )