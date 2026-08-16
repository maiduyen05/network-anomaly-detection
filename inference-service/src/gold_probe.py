from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka import Consumer


# ============================================================
# CONFIG
# ============================================================
#
# Python chạy trực tiếp trong WSL.
#
# Kafka Docker expose broker ra host tại:
#
#     localhost:9092
#
# Không dùng:
#
#     kafka:29092
#
# vì hostname "kafka" chỉ dùng được giữa các container Docker.
# ============================================================

BOOTSTRAP_SERVERS = "localhost:9092"

GOLD_TOPIC = "gold.ue.sequence"


# Đây là consumer group CHỈ DÙNG CHO CONTRACT PROBE.
#
# Tuyệt đối không dùng:
#
#     flink-gold-v1
#
# vì đó là group của Flink Gold Job.
#
# Cũng chưa dùng:
#
#     inference-runtime-v1
#
# vì group đó sau này dành cho inference thật.
PROBE_GROUP = "gold-contract-probe-v1"


EXPECTED_SCHEMA_VERSION = (
    "gold-sequence-v1"
)

EXPECTED_FEATURE_VERSION = (
    "gold-ue-sequence-feature-v2"
)

EXPECTED_SEQUENCE_LENGTH = 32

EXPECTED_STRIDE = 8

EXPECTED_CAT_FEATURES = 4

EXPECTED_NUM_FEATURES = 2


# ============================================================
# OUTPUT
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "runtime"
    / "inference"
    / "gold_contract_probe_sample.json"
)


# ============================================================
# VALIDATION HELPERS
# ============================================================

def require(
    condition: bool,
    message: str,
) -> None:
    """
    Fail-fast khi Gold contract không đúng.

    Không cố sửa hoặc tự reshape dữ liệu ở inference layer.
    """

    if not condition:
        raise ValueError(message)


def validate_x_cat(
    x_cat,
) -> None:
    """
    Contract:

        x_cat = INT64[32][4]

    Categorical ranges:

        column 0 event_code:
            1..10

        column 1 event_result_code:
            0..3

        column 2 cause_code:
            0..26

        column 3 sub_cause_code:
            0..62
    """

    require(
        isinstance(x_cat, list),
        "model_input.x_cat must be a list",
    )

    require(
        len(x_cat)
        == EXPECTED_SEQUENCE_LENGTH,
        (
            "x_cat sequence length mismatch: "
            f"expected={EXPECTED_SEQUENCE_LENGTH}, "
            f"actual={len(x_cat)}"
        ),
    )


    allowed_ranges = [
        (1, 10),
        (0, 3),
        (0, 26),
        (0, 62),
    ]


    for timestep, row in enumerate(x_cat):

        require(
            isinstance(row, list),
            (
                "x_cat row must be list: "
                f"timestep={timestep}"
            ),
        )

        require(
            len(row)
            == EXPECTED_CAT_FEATURES,
            (
                "x_cat feature count mismatch: "
                f"timestep={timestep}, "
                f"expected={EXPECTED_CAT_FEATURES}, "
                f"actual={len(row)}"
            ),
        )


        for feature_index, value in enumerate(row):

            # bool là subclass của int trong Python,
            # nên phải reject bool riêng.
            require(
                isinstance(value, int)
                and not isinstance(value, bool),
                (
                    "x_cat value must be integer: "
                    f"timestep={timestep}, "
                    f"feature={feature_index}, "
                    f"value={value!r}"
                ),
            )


            lower, upper = (
                allowed_ranges[
                    feature_index
                ]
            )


            require(
                lower <= value <= upper,
                (
                    "x_cat value outside "
                    "feature contract: "
                    f"timestep={timestep}, "
                    f"feature={feature_index}, "
                    f"value={value}, "
                    f"expected=[{lower},{upper}]"
                ),
            )


def validate_x_num(
    x_num,
) -> None:
    """
    Contract:

        x_num = FLOAT32[32][2]

    Giá trị hợp lệ:

        -1.0
            missing

    hoặc:

        0.0 .. 1.0
            normalized value
    """

    require(
        isinstance(x_num, list),
        "model_input.x_num must be a list",
    )

    require(
        len(x_num)
        == EXPECTED_SEQUENCE_LENGTH,
        (
            "x_num sequence length mismatch: "
            f"expected={EXPECTED_SEQUENCE_LENGTH}, "
            f"actual={len(x_num)}"
        ),
    )


    for timestep, row in enumerate(x_num):

        require(
            isinstance(row, list),
            (
                "x_num row must be list: "
                f"timestep={timestep}"
            ),
        )

        require(
            len(row)
            == EXPECTED_NUM_FEATURES,
            (
                "x_num feature count mismatch: "
                f"timestep={timestep}, "
                f"expected={EXPECTED_NUM_FEATURES}, "
                f"actual={len(row)}"
            ),
        )


        for feature_index, value in enumerate(row):

            require(
                isinstance(
                    value,
                    (int, float),
                )
                and not isinstance(
                    value,
                    bool,
                ),
                (
                    "x_num must be numeric: "
                    f"timestep={timestep}, "
                    f"feature={feature_index}, "
                    f"value={value!r}"
                ),
            )


            number = float(value)


            valid = (
                number == -1.0
                or
                0.0 <= number <= 1.0
            )


            require(
                valid,
                (
                    "x_num outside normalized range: "
                    f"timestep={timestep}, "
                    f"feature={feature_index}, "
                    f"value={number}"
                ),
            )


def validate_gold_record(
    record: dict,
) -> None:
    """
    Validate một GoldSequenceSample thật lấy từ Kafka.

    Inference service sau này sẽ dùng đúng object này.
    """

    require(
        isinstance(record, dict),
        "Gold Kafka value must be JSON object",
    )


    # --------------------------------------------------------
    # VERSION
    # --------------------------------------------------------

    require(
        record.get("schema_version")
        == EXPECTED_SCHEMA_VERSION,
        (
            "schema_version mismatch: "
            f"{record.get('schema_version')!r}"
        ),
    )


    require(
        record.get("feature_version")
        == EXPECTED_FEATURE_VERSION,
        (
            "feature_version mismatch: "
            f"{record.get('feature_version')!r}"
        ),
    )


    # --------------------------------------------------------
    # IDENTITY
    # --------------------------------------------------------

    require(
        bool(record.get("sample_id")),
        "sample_id missing",
    )

    require(
        bool(record.get("ue_key")),
        "ue_key missing",
    )


    # --------------------------------------------------------
    # WINDOW CONTRACT
    # --------------------------------------------------------

    require(
        record.get("sequence_length")
        == EXPECTED_SEQUENCE_LENGTH,
        (
            "sequence_length mismatch: "
            f"{record.get('sequence_length')!r}"
        ),
    )


    require(
        record.get("stride")
        == EXPECTED_STRIDE,
        (
            "stride mismatch: "
            f"{record.get('stride')!r}"
        ),
    )


    # --------------------------------------------------------
    # MODEL INPUT
    # --------------------------------------------------------

    model_input = record.get(
        "model_input"
    )


    require(
        isinstance(model_input, dict),
        "model_input missing or invalid",
    )


    x_cat = model_input.get(
        "x_cat"
    )

    x_num = model_input.get(
        "x_num"
    )


    validate_x_cat(
        x_cat
    )

    validate_x_num(
        x_num
    )


    # --------------------------------------------------------
    # EVIDENCE
    # --------------------------------------------------------
    #
    # Evidence không được đưa vào neural network,
    # nhưng Gold giữ nó để:
    #
    # - audit;
    # - Streamlit;
    # - giải thích anomaly.
    #
    # Không fail nếu evidence có thêm field.
    # --------------------------------------------------------

    evidence = record.get(
        "evidence"
    )


    require(
        isinstance(evidence, dict),
        "evidence missing or invalid",
    )


    events = evidence.get(
        "events"
    )


    if events is not None:

        require(
            isinstance(events, list),
            "evidence.events must be list",
        )

        require(
            len(events)
            == EXPECTED_SEQUENCE_LENGTH,
            (
                "evidence.events length mismatch: "
                f"expected={EXPECTED_SEQUENCE_LENGTH}, "
                f"actual={len(events)}"
            ),
        )


# ============================================================
# DISPLAY
# ============================================================

def print_summary(
    record: dict,
    partition: int,
    offset: int,
) -> None:
    """
    In thông tin vừa đủ để kiểm tra contract,
    không dump toàn bộ 32 evidence events ra terminal.
    """

    model_input = record[
        "model_input"
    ]

    x_cat = model_input[
        "x_cat"
    ]

    x_num = model_input[
        "x_num"
    ]


    print()
    print(
        "============================================"
    )

    print(
        " GOLD CONTRACT PROBE PASS"
    )

    print(
        "============================================"
    )

    print(
        "Kafka partition :",
        partition,
    )

    print(
        "Kafka offset    :",
        offset,
    )

    print(
        "Schema version  :",
        record[
            "schema_version"
        ],
    )

    print(
        "Feature version :",
        record[
            "feature_version"
        ],
    )

    print(
        "Sample ID       :",
        record[
            "sample_id"
        ],
    )

    print(
        "UE key          :",
        record[
            "ue_key"
        ],
    )

    print(
        "IMSI            :",
        record.get(
            "imsi"
        ),
    )

    print(
        "Window start    :",
        record.get(
            "window_start_event_time"
        ),
    )

    print(
        "Window end      :",
        record.get(
            "window_end_event_time"
        ),
    )

    print(
        "Sequence length :",
        record[
            "sequence_length"
        ],
    )

    print(
        "Stride          :",
        record[
            "stride"
        ],
    )

    print(
        "x_cat shape     :",
        (
            len(x_cat),
            len(x_cat[0]),
        ),
    )

    print(
        "x_num shape     :",
        (
            len(x_num),
            len(x_num[0]),
        ),
    )

    print(
        "x_cat[0]        :",
        x_cat[0],
    )

    print(
        "x_num[0]        :",
        x_num[0],
    )

    print(
        "x_cat[-1]       :",
        x_cat[-1],
    )

    print(
        "x_num[-1]       :",
        x_num[-1],
    )


    events = (
        record
        .get(
            "evidence",
            {},
        )
        .get(
            "events",
            [],
        )
    )


    print(
        "Evidence events :",
        len(events),
    )


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    """
    Đọc một Gold sample LỊCH SỬ.

    QUAN TRỌNG:

    auto.offset.reset = earliest

    là chủ ý ở probe này vì chúng ta muốn lấy một Gold
    sample đã tồn tại từ development period.

    Nó KHÔNG phải config sau này của inference runtime.
    """

    consumer = Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                PROBE_GROUP,

            # Đọc historical Gold chỉ để kiểm tra schema.
            "auto.offset.reset":
                "earliest",

            # Không commit offset vì đây chỉ là probe.
            "enable.auto.commit":
                False,

            # Gold sink Flink dùng transaction.
            # Chỉ đọc transaction đã commit.
            "isolation.level":
                "read_committed",
        }
    )


    consumer.subscribe(
        [
            GOLD_TOPIC
        ]
    )


    print(
        "Connecting to Kafka:",
        BOOTSTRAP_SERVERS,
    )

    print(
        "Topic:",
        GOLD_TOPIC,
    )

    print(
        "Probe group:",
        PROBE_GROUP,
    )

    print()
    print(
        "Waiting for one historical Gold sample..."
    )


    try:

        # Khoảng 30 giây.
        #
        # Nếu không tìm được sample thì fail rõ ràng,
        # tránh chương trình chờ vô hạn.
        max_polls = 30


        for _ in range(max_polls):

            message = consumer.poll(
                timeout=1.0
            )


            if message is None:
                continue


            if message.error():

                print(
                    "Kafka error:",
                    message.error(),
                    file=sys.stderr,
                )

                continue


            try:

                raw_value = (
                    message
                    .value()
                    .decode(
                        "utf-8"
                    )
                )


                record = json.loads(
                    raw_value
                )


                validate_gold_record(
                    record
                )


                # --------------------------------------------
                # SAVE SAMPLE
                # --------------------------------------------
                #
                # File này dùng ở checkpoint tiếp theo để
                # test predictor bằng Gold THẬT,
                # không phải sample synthetic.
                # --------------------------------------------

                OUTPUT_FILE.parent.mkdir(
                    parents=True,
                    exist_ok=True,
                )


                OUTPUT_FILE.write_text(
                    json.dumps(
                        record,
                        indent=2,
                        ensure_ascii=False,
                    ),
                    encoding="utf-8",
                )


                print_summary(
                    record=
                        record,

                    partition=
                        message.partition(),

                    offset=
                        message.offset(),
                )


                print()
                print(
                    "Saved sample to:"
                )

                print(
                    OUTPUT_FILE
                )

                print()
                print(
                    "NOTE:"
                )

                print(
                    "This is a historical development "
                    "sample used ONLY for contract testing."
                )

                print(
                    "data/runtime has NOT been consumed."
                )


                return 0


            except Exception as exc:

                # Có thể topic chứa record cũ không cùng contract
                # trong giai đoạn phát triển.
                #
                # Khi đó bỏ record đó và tìm sample tiếp theo.
                print(
                    (
                        "Skip invalid Gold record "
                        f"partition={message.partition()} "
                        f"offset={message.offset()}: "
                        f"{exc}"
                    ),
                    file=sys.stderr,
                )


        print(
            (
                "ERROR: Could not find a valid Gold sample "
                "within 30 seconds."
            ),
            file=sys.stderr,
        )

        return 1


    finally:

        consumer.close()


if __name__ == "__main__":
    raise SystemExit(
        main()
    )