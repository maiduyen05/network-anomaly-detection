from __future__ import annotations

import json
import sys
from pathlib import Path


# ============================================================
# PROJECT PATHS
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

GOLD_SAMPLE_FILE = (
    PROJECT_ROOT
    / "runtime"
    / "inference"
    / "gold_contract_probe_sample.json"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "runtime"
    / "inference"
    / "model_probe_prediction.json"
)


# ============================================================
# IMPORT PRODUCTION PREDICTOR
# ============================================================
#
# predictor.py và model_def.py nằm trong production bundle.
#
# Ta không copy model code sang inference-service/src.
# Production bundle vẫn là source of truth cho inference.
# ============================================================

sys.path.insert(
    0,
    str(MODEL_DIR),
)

from predictor import AnomalyPredictor  # noqa: E402


EXPECTED_FEATURE_VERSION = (
    "gold-ue-sequence-feature-v2"
)

EXPECTED_MODEL = (
    "MixedTransformer"
)

EXPECTED_SEED = 2026


# ============================================================
# HELPERS
# ============================================================

def require(
    condition: bool,
    message: str,
) -> None:
    """
    Fail-fast nếu contract hoặc prediction không đúng.
    """

    if not condition:
        raise ValueError(message)


# ============================================================
# MAIN
# ============================================================

def main() -> int:

    # --------------------------------------------------------
    # 1. CHECK INPUT FILE
    # --------------------------------------------------------

    require(
        GOLD_SAMPLE_FILE.exists(),
        (
            "Gold probe sample not found: "
            f"{GOLD_SAMPLE_FILE}"
        ),
    )


    require(
        MODEL_DIR.exists(),
        (
            "Production model directory not found: "
            f"{MODEL_DIR}"
        ),
    )


    # --------------------------------------------------------
    # 2. READ REAL GOLD SAMPLE
    # --------------------------------------------------------

    gold_record = json.loads(
        GOLD_SAMPLE_FILE.read_text(
            encoding="utf-8"
        )
    )


    print()
    print(
        "============================================"
    )

    print(
        " GOLD -> MODEL PROBE"
    )

    print(
        "============================================"
    )

    print(
        "Gold sample:",
        gold_record["sample_id"],
    )

    print(
        "UE:",
        gold_record["ue_key"],
    )

    print(
        "Feature version:",
        gold_record["feature_version"],
    )


    # --------------------------------------------------------
    # 3. VALIDATE GOLD / MODEL CONTRACT
    # --------------------------------------------------------

    require(
        gold_record.get(
            "feature_version"
        )
        == EXPECTED_FEATURE_VERSION,
        (
            "Unexpected Gold feature version: "
            f"{gold_record.get('feature_version')!r}"
        ),
    )


    # --------------------------------------------------------
    # 4. LOAD PRODUCTION MODEL
    # --------------------------------------------------------
    #
    # Model chỉ load đúng một lần trong process.
    #
    # Sau này inference worker cũng làm như vậy:
    #
    # startup
    #   ↓
    # load model once
    #   ↓
    # consume nhiều Gold samples
    #
    # KHÔNG load model lại cho mỗi prediction.
    # --------------------------------------------------------

    print()
    print(
        "Loading production model..."
    )


    predictor = AnomalyPredictor(
        MODEL_DIR
    )


    print(
        "Model:",
        predictor.model_name,
    )

    print(
        "Selected seed:",
        predictor.selected_seed,
    )

    print(
        "Default threshold:",
        predictor.default_threshold,
    )

    print(
        "Score policy:",
        predictor.score_policy,
    )


    # --------------------------------------------------------
    # 5. VERIFY PRODUCTION MODEL
    # --------------------------------------------------------

    require(
        predictor.model_name
        == EXPECTED_MODEL,
        (
            "Unexpected production model: "
            f"{predictor.model_name!r}"
        ),
    )


    require(
        predictor.selected_seed
        == EXPECTED_SEED,
        (
            "Unexpected selected seed: "
            f"{predictor.selected_seed!r}"
        ),
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
        gold_record[
            "feature_version"
        ]
        == model_feature_version,
        (
            "Gold / model feature version mismatch: "
            f"Gold={gold_record['feature_version']!r}, "
            f"Model={model_feature_version!r}"
        ),
    )


    # --------------------------------------------------------
    # 6. RUN ONE REAL INFERENCE
    # --------------------------------------------------------
    #
    # Quan trọng:
    #
    # KHÔNG:
    #     encode category lại
    #     normalize numeric lại
    #     tạo sequence lại
    #
    # Gold đã làm toàn bộ việc đó.
    #
    # Predictor nhận trực tiếp:
    #
    #     model_input.x_cat
    #     model_input.x_num
    #
    # --------------------------------------------------------

    print()
    print(
        "Running inference..."
    )


    prediction = (
        predictor
        .score_record(
            gold_record
        )
    )


    # --------------------------------------------------------
    # 7. BASIC PREDICTION VALIDATION
    # --------------------------------------------------------

    raw_score = float(
        prediction[
            "raw_score"
        ]
    )

    anomaly_score = float(
        prediction[
            "anomaly_score"
        ]
    )

    anomaly_threshold = float(
        prediction[
            "anomaly_threshold"
        ]
    )


    # --------------------------------------------------------
    # ECDF SCORE CONTRACT
    # --------------------------------------------------------

    require(
        0.0 <= anomaly_score <= 1.0,
        (
            "Invalid anomaly_score: "
            f"{anomaly_score}"
        ),
    )

    require(
        0.0 <= anomaly_threshold <= 1.0,
        (
            "Invalid anomaly_threshold: "
            f"{anomaly_threshold}"
        ),
    )


    # --------------------------------------------------------
    # OLD CONFORMAL CONTRACT MUST BE GONE
    # --------------------------------------------------------

    require(
        "conformal_p_value"
        not in prediction,
        (
            "Old conformal_p_value field "
            "still exists in prediction"
        ),
    )

    require(
        "alpha"
        not in prediction,
        (
            "Old alpha field "
            "still exists in prediction"
        ),
    )


    # --------------------------------------------------------
    # BUSINESS THRESHOLD DECISION
    # --------------------------------------------------------

    expected_flag = (
        anomaly_score
        >=
        anomaly_threshold
    )

    require(
        bool(
            prediction[
                "is_anomaly"
            ]
        )
        ==
        expected_flag,
        (
            "is_anomaly inconsistent with "
            "anomaly_score >= anomaly_threshold"
        ),
    )


    # --------------------------------------------------------
    # VERIFY METADATA
    # --------------------------------------------------------

    score_meta = (
        predictor
        .meta[
            "score"
        ]
    )

    require(
        score_meta[
            "normalization"
        ]
        ==
        "calibration_empirical_cdf",
        (
            "Unexpected score normalization: "
            f"{score_meta['normalization']!r}"
        ),
    )

    require(
        score_meta[
            "is_probability"
        ]
        is False,
        "anomaly_score must not be marked as probability",
    )


    # --------------------------------------------------------
    # 8. DISPLAY RESULT
    # --------------------------------------------------------

    print()
    print(
        "============================================"
    )

    print(
        " MODEL PROBE PASS"
    )

    print(
        "============================================"
    )

    print(
        "Sample ID           :",
        prediction[
            "sample_id"
        ],
    )

    print(
        "UE key              :",
        prediction[
            "ue_key"
        ],
    )

    print(
        "Model               :",
        prediction[
            "model"
        ],
    )

    print(
        "Selected seed       :",
        prediction.get(
            "selected_seed"
        ),
    )

    print(
        "Score policy        :",
        prediction[
            "score_policy"
        ],
    )

    print(
        "Raw score           :",
        raw_score,
    )

    print(
        "Anomaly score       :",
        anomaly_score,
    )

    print(
        "Threshold           :",
        anomaly_threshold,
    )

    print(
        "Is anomaly          :",
        prediction[
            "is_anomaly"
        ],
    )

    print(
        "Top timesteps       :",
        prediction.get(
            "top_timestep_indices",
            [],
        ),
    )

    print(
        "Top contributions   :",
        prediction.get(
            "top_timestep_raw_contributions",
            [],
        ),
    )


    # --------------------------------------------------------
    # 9. SAVE PREDICTION
    # --------------------------------------------------------
    #
    # File này chỉ phục vụ debug.
    #
    # Sau này runtime worker sẽ gửi prediction sang Kafka,
    # không ghi từng prediction ra JSON file.
    # --------------------------------------------------------

    OUTPUT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True,
    )


    OUTPUT_FILE.write_text(
        json.dumps(
            prediction,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


    print()
    print(
        "Prediction saved to:"
    )

    print(
        OUTPUT_FILE
    )


    print()
    print(
        "IMPORTANT:"
    )

    print(
        "This sample belongs to the historical "
        "development period."
    )

    print(
        "It is used ONLY to verify the inference chain."
    )

    print(
        "Runtime holdout data has NOT been replayed."
    )


    return 0


if __name__ == "__main__":

    raise SystemExit(
        main()
    )