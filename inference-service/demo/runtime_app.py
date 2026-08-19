from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from prediction_buffer import PredictionBuffer


# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Hệ thống phát hiện thuê bao bất thường",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# COLOR PALETTE
# ============================================================

# Nền chính
COLOR_BACKGROUND = "#EAF4FF"

# Card
COLOR_CARD = "#FFFFFF"

# KPI vàng pastel
COLOR_KPI = "#FFF3C4"
COLOR_KPI_BORDER = "#F0D77C"

# Xanh chủ đạo
COLOR_PRIMARY = "#78A9EB"
COLOR_PRIMARY_DARK = "#315F91"

# Text - không dùng đen
COLOR_TEXT = "#29445F"
COLOR_TEXT_SECONDARY = "#708399"

# Cảnh báo
COLOR_WARNING = "#E8B93F"
COLOR_ORANGE = "#F2994A"
COLOR_DANGER = "#E96870"

# Border
COLOR_BORDER = "#D4E4F3"

MAX_CHART_POINTS = 80

NICE_BUCKET_SECONDS = (
    1,
    2,
    5,
    10,
    15,
    30,
    60,
    120,
    300,
    600,
)


def choose_chart_bucket_seconds(
    first_time: pd.Timestamp,
    last_time: pd.Timestamp,
    max_points: int = MAX_CHART_POINTS,
) -> int:
    """
    Chọn bucket thời gian để biểu đồ chỉ còn khoảng max_points điểm.
    """

    span_seconds = max(
        (
            last_time
            -
            first_time
        ).total_seconds(),
        1.0,
    )

    required_seconds = (
        span_seconds
        /
        max_points
    )

    for seconds in NICE_BUCKET_SECONDS:

        if seconds >= required_seconds:

            return seconds

    return int(
        math.ceil(
            required_seconds
            /
            600
        )
        *
        600
    )


def build_chart_display_series(
    chart_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """
    Giảm mật độ điểm chỉ để hiển thị, giữ điểm anomaly cao nhất mỗi bucket.
    """

    if len(chart_df) <= MAX_CHART_POINTS:

        return (
            chart_df.copy(),
            0,
        )

    first_time = (
        chart_df[
            "event_datetime"
        ]
        .min()
    )

    last_time = (
        chart_df[
            "event_datetime"
        ]
        .max()
    )

    bucket_seconds = (
        choose_chart_bucket_seconds(
            first_time,
            last_time,
        )
    )

    work_df = chart_df.copy()

    work_df[
        "_display_bucket"
    ] = (
        work_df[
            "event_datetime"
        ]
        .dt
        .floor(
            f"{bucket_seconds}s"
        )
    )

    representative_indices = (
        work_df
        .groupby(
            "_display_bucket",
            sort=True,
        )[
            "Điểm"
        ]
        .idxmax()
    )

    display_df = (
        work_df
        .loc[
            representative_indices
        ]
        .sort_values(
            "event_datetime"
        )
        .drop(
            columns=[
                "_display_bucket"
            ]
        )
        .reset_index(
            drop=True
        )
    )

    return (
        display_df,
        bucket_seconds,
    )


# ============================================================
# CSS
# ============================================================
#
# Chỉ block này dùng HTML để nạp CSS.
#
# Nội dung dashboard phía dưới dùng component native của
# Streamlit để tránh việc <div>, <span> bị hiện thành code.
# ============================================================

st.markdown(
    f"""
    <style>

    /* ========================================================
       GLOBAL
    ======================================================== */

    html,
    body,
    .stApp {{
        color: {COLOR_TEXT};
    }}

    .stApp {{
        background-color: {COLOR_BACKGROUND};
    }}


    /* ========================================================
       HEADER
    ======================================================== */

    [data-testid="stHeader"] {{
        background-color: #DCEEFF;
    }}


    /* ========================================================
       MAIN CONTENT
    ======================================================== */

    .block-container {{
        max-width: 1450px;
        padding-top: 3.2rem;
        padding-bottom: 3rem;
    }}


    /* ========================================================
       TEXT
    ======================================================== */

    h1,
    h2,
    h3,
    h4,
    h5,
    h6,
    p,
    label {{
        color: {COLOR_TEXT};
    }}


    /* ========================================================
       SIDEBAR
    ======================================================== */

    [data-testid="stSidebar"] {{
        background-color: #DCEEFF;
        border-right: 1px solid {COLOR_BORDER};
    }}

    [data-testid="stSidebar"] * {{
        color: {COLOR_TEXT};
    }}

    /* Căn lại sidebar để các phần có khoảng cách đồng đều. */
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] {{
        gap: 0.85rem;
    }}

    [data-testid="stSidebar"] h2 {{
        margin-top: 0;
        margin-bottom: 0.15rem;
    }}

    [data-testid="stSidebar"] h3 {{
        margin-top: 0.35rem;
        margin-bottom: 0.10rem;
    }}

    [data-testid="stSidebar"] [data-testid="stAlert"] {{
        margin-top: 0.10rem;
        margin-bottom: 0.20rem;
    }}

    [data-testid="stSidebar"] [data-testid="stCaptionContainer"] {{
        margin-top: -0.20rem;
    }}


    /* ========================================================
       KPI CARDS
    ======================================================== */

    .st-key-kpi_ue,
    .st-key-kpi_request,
    .st-key-kpi_score {{
        background-color: {COLOR_KPI};

        border:
            1px
            solid
            {COLOR_KPI_BORDER}
            !important;

        border-radius: 18px;

        padding:
            12px
            18px
            10px
            18px;

        min-height: 100px;

        box-shadow:
            0
            5px
            16px
            rgba(120, 110, 70, 0.08);
    }}


    .st-key-kpi_ue [data-testid="stMetricValue"],
    .st-key-kpi_request [data-testid="stMetricValue"],
    .st-key-kpi_score [data-testid="stMetricValue"] {{
        color: {COLOR_PRIMARY_DARK};
        font-weight: 750;
    }}


    .st-key-kpi_ue [data-testid="stMetricLabel"],
    .st-key-kpi_request [data-testid="stMetricLabel"],
    .st-key-kpi_score [data-testid="stMetricLabel"] {{
        color: {COLOR_TEXT};
        font-weight: 650;
    }}


    /* ========================================================
       CHART CARD
    ======================================================== */

    .st-key-chart_card {{
        background-color: {COLOR_CARD};

        border:
            1px
            solid
            {COLOR_BORDER}
            !important;

        border-radius: 18px;

        padding:
            18px
            20px
            16px
            20px;

        box-shadow:
            0
            5px
            18px
            rgba(75, 120, 165, 0.07);
    }}


    .st-key-chart_card [data-testid="stVegaLiteChart"] {{
        background-color: #FFFFFF;
        border-radius: 12px;
    }}


    /* ========================================================
       UE CARDS
    ======================================================== */

    [data-testid="stExpander"] {{
        background-color: #FFFFFF;

        border:
            1px
            solid
            {COLOR_BORDER};

        border-radius: 14px;

        overflow: hidden;

        margin-bottom: 10px;
    }}


    /* ========================================================
       BUTTONS
    ======================================================== */

    div.stButton > button {{
        border-radius: 10px;
        border: 1px solid {COLOR_BORDER};
        color: {COLOR_PRIMARY_DARK};
    }}


    /* ========================================================
       DATA TABLE
    ======================================================== */

    [data-testid="stDataFrame"] {{
        background-color: #FFFFFF;
        border-radius: 12px;
    }}


    /* ========================================================
       FOOTER
    ======================================================== */

    footer {{
        visibility: hidden;
    }}

    </style>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# TITLE
# ============================================================

st.title(
    "📡 Hệ thống phát hiện thuê bao bất thường"
)


# ============================================================
# KAFKA CONFIG
# ============================================================

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

PREDICTION_TOPIC = os.getenv(
    "KAFKA_OUTPUT_TOPIC",
    "anomaly-predictions",
)

DASHBOARD_GROUP = os.getenv(
    "STREAMLIT_KAFKA_GROUP",
    "streamlit-viewer-v2",
)


# ============================================================
# MODEL METADATA
# ============================================================

SERVICE_DIR = (
    Path(__file__)
    .resolve()
    .parents[1]
)

MODEL_DIR = (
    SERVICE_DIR
    / "models"
    / "production"
)

METADATA_FILE = (
    MODEL_DIR
    / "artifact_metadata.json"
)

MODEL_DISPLAY_NAMES = {
    "IsolationForest": "Isolation Forest",
    "MixedTransformer": "Mixed Transformer",
    "MFMT": "MFMT",
}

if METADATA_FILE.exists():

    MODEL_METADATA = json.loads(
        METADATA_FILE.read_text(
            encoding="utf-8"
        )
    )

else:

    MODEL_METADATA = {}


# ------------------------------------------------------------
# Danh sách model lấy trực tiếp từ production bundle.
#
# Fallback về MixedTransformer chỉ để dashboard vẫn khởi động
# nếu metadata tạm thời chưa có.
# ------------------------------------------------------------

DEPLOYMENT = MODEL_METADATA.get(
    "deployment",
    {},
)

MODELS_METADATA = MODEL_METADATA.get(
    "models",
    {},
)

AVAILABLE_MODELS = list(
    DEPLOYMENT.get(
        "available_models",
        MODELS_METADATA.keys(),
    )
)

if not AVAILABLE_MODELS:

    AVAILABLE_MODELS = [
        "MixedTransformer"
    ]


DEFAULT_MODEL = DEPLOYMENT.get(
    "default_model",
    AVAILABLE_MODELS[0],
)

if DEFAULT_MODEL not in AVAILABLE_MODELS:

    DEFAULT_MODEL = AVAILABLE_MODELS[0]


DEFAULT_ANOMALY_THRESHOLD = float(
    MODEL_METADATA
    .get(
        "score",
        {},
    )
    .get(
        "default_threshold",
        0.975,
    )
)


# ============================================================
# BUFFER
# ============================================================

@st.cache_resource
def get_prediction_buffer() -> PredictionBuffer:

    return PredictionBuffer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=PREDICTION_TOPIC,
        group_id=DASHBOARD_GROUP,
        max_records=3000,
    )


prediction_buffer = get_prediction_buffer()


# ============================================================
# SESSION STATE
# ============================================================

if "selected_ue" not in st.session_state:

    # __ALL__ = đang xem toàn mạng
    st.session_state.selected_ue = "__ALL__"


if "selected_model" not in st.session_state:

    st.session_state.selected_model = (
        DEFAULT_MODEL
    )


# ============================================================
# SIDEBAR
# ============================================================
#
# Giữ nguyên các nội dung hiện có:
#   - trạng thái hệ thống
#   - chọn mô hình
#   - chọn ngưỡng
#   - mức cảnh báo
#
# Chỉ sắp xếp lại thứ tự và khoảng cách để sidebar cân đối hơn.
# ============================================================

with st.sidebar:

    # --------------------------------------------------------
    # HEADER + STATUS
    # --------------------------------------------------------

    st.header(
        "📡 Giám sát thuê bao"
    )

    st.success(
        "Hệ thống đang hoạt động"
    )


    # --------------------------------------------------------
    # MODEL
    # --------------------------------------------------------

    st.subheader(
        "Mô hình"
    )

    default_model_index = (
        AVAILABLE_MODELS.index(
            st.session_state.selected_model
        )
        if st.session_state.selected_model in AVAILABLE_MODELS
        else AVAILABLE_MODELS.index(
            DEFAULT_MODEL
        )
    )

    selected_model = st.selectbox(
        label="Mô hình",
        options=AVAILABLE_MODELS,
        index=default_model_index,
        format_func=lambda model_key: (
            MODEL_DISPLAY_NAMES.get(
                model_key,
                model_key,
            )
        ),
        label_visibility="collapsed",
    )

    # Nếu đổi model:
    #   - dashboard chuyển toàn bộ KPI/chart/UE sang model mới;
    #   - reset UE filter để tránh giữ UE của model trước.
    if (
        st.session_state.selected_model
        !=
        selected_model
    ):

        st.session_state.selected_model = (
            selected_model
        )

        st.session_state.selected_ue = (
            "__ALL__"
        )


    # --------------------------------------------------------
    # THRESHOLD
    # --------------------------------------------------------

    st.subheader(
        "Ngưỡng bất thường"
    )

    display_threshold = st.slider(
        label="Ngưỡng bất thường",
        min_value=0.800,
        max_value=1.000,
        value=DEFAULT_ANOMALY_THRESHOLD,
        step=0.001,
        format="%.3f",
        help=(
            "Cửa sổ có điểm bất thường bằng hoặc cao hơn "
            "ngưỡng này sẽ được đánh dấu cảnh báo."
        ),
        label_visibility="collapsed",
    )

    st.caption(
        f"Ngưỡng hiện tại: {display_threshold:.3f}"
    )


    # --------------------------------------------------------
    # ALERT LEVELS
    # --------------------------------------------------------

    st.divider()

    st.subheader(
        "Mức cảnh báo"
    )

    st.markdown(
        """
        🟢 **Bình thường**  
        Chưa ghi nhận dấu hiệu bất thường.

        🟡 **Cần theo dõi**  
        Có ít nhất một cửa sổ vượt ngưỡng.

        🟠 **Cảnh báo lặp lại**  
        Có nhiều cảnh báo liên tiếp.

        🔴 **Cảnh báo cao**  
        Bất thường xuất hiện liên tục.
        """
    )


# ============================================================
# UI CALLBACK
# ============================================================

def select_ue(
    ue_key: str,
) -> None:
    """
    Chọn UE cần hiển thị trên biểu đồ.

    Callback chạy trước rerun nên biểu đồ được cập nhật
    ngay khi người dùng bấm nút.
    """

    st.session_state.selected_ue = ue_key


# ============================================================
# KPI
# ============================================================

def render_kpi(
    title: str,
    value: str,
    key: str,
) -> None:
    """
    KPI dùng component native của Streamlit.
    """

    with st.container(
        border=True,
        key=key,
    ):

        st.metric(
            label=title,
            value=value,
        )


# ============================================================
# LONGEST ANOMALY RUN
# ============================================================

def calculate_longest_anomaly_run(
    rows: list[dict],
) -> int:
    """
    Số Gold window cảnh báo liên tiếp dài nhất của một UE.

    Sử dụng display_is_anomaly vì ngưỡng hiện tại
    do người dùng dashboard lựa chọn.
    """

    ordered = sorted(
        rows,
        key=lambda item: str(
            item.get(
                "window_end_event_time",
                "",
            )
        ),
    )


    longest = 0
    current = 0


    for row in ordered:

        if bool(
            row.get(
                "display_is_anomaly",
                False,
            )
        ):

            current += 1

            longest = max(
                longest,
                current,
            )

        else:

            current = 0


    return longest


# ============================================================
# ANOMALOUS UE SUMMARY
# ============================================================

def build_anomalous_ue_summary(
    rows: list[dict],
) -> list[dict]:
    """
    Liệt kê các UE có ít nhất một Gold window
    vượt ngưỡng dashboard.
    """

    grouped: dict[
        str,
        list[dict],
    ] = defaultdict(
        list
    )


    for row in rows:

        ue_key = str(
            row.get(
                "ue_key",
                "",
            )
        )


        grouped[
            ue_key
        ].append(
            row
        )


    result = []


    for (
        ue_key,
        ue_rows,
    ) in grouped.items():


        anomaly_rows = [
            row
            for row in ue_rows
            if bool(
                row.get(
                    "display_is_anomaly",
                    False,
                )
            )
        ]


        if not anomaly_rows:

            continue


        longest_run = (
            calculate_longest_anomaly_run(
                ue_rows
            )
        )


        max_score = max(
            float(
                row.get(
                    "anomaly_score",
                    0.0,
                )
            )
            for row in anomaly_rows
        )


        highest_row = max(
            anomaly_rows,
            key=lambda row: float(
                row.get(
                    "anomaly_score",
                    0.0,
                )
            ),
        )


        latest_row = max(
            anomaly_rows,
            key=lambda row: str(
                row.get(
                    "window_end_event_time",
                    "",
                )
            ),
        )


        if longest_run >= 3:

            severity = (
                "Cảnh báo cao"
            )

        elif longest_run >= 2:

            severity = (
                "Cảnh báo lặp lại"
            )

        else:

            severity = (
                "Cần theo dõi"
            )


        result.append(
            {
                "ue_key":
                    ue_key,

                "all_rows":
                    ue_rows,

                "anomaly_rows":
                    anomaly_rows,

                "anomaly_count":
                    len(
                        anomaly_rows
                    ),

                "total_count":
                    len(
                        ue_rows
                    ),

                "longest_run":
                    longest_run,

                "max_score":
                    max_score,

                "severity":
                    severity,

                "highest_row":
                    highest_row,

                "latest_row":
                    latest_row,
            }
        )


    return sorted(
        result,

        key=lambda item: (
            item[
                "longest_run"
            ],

            item[
                "max_score"
            ],

            item[
                "anomaly_count"
            ],
        ),

        reverse=True,
    )


# ============================================================
# HELPER
# ============================================================

def first_value(
    record: dict,
    *keys: str,
):
    """
    Trả field đầu tiên tồn tại trong evidence event.
    """

    for key in keys:

        value = record.get(
            key
        )


        if (
            value is not None
            and
            value != ""
        ):

            return value


    return "-"


# ============================================================
# EVIDENCE TABLE
# ============================================================

def evidence_to_dataframe(
    prediction_row: dict,
) -> pd.DataFrame:
    """
    Chuyển top evidence của model thành bảng hành vi.
    """

    top_events = (
        prediction_row.get(
            "top_evidence_events",
            [],
        )
        or []
    )


    output = []


    for item in top_events:

        event = (
            item.get(
                "event",
                {},
            )
            or {}
        )


        event_time = first_value(
            event,
            "event_time",
            "eventTime",
        )


        parsed_time = pd.to_datetime(
            event_time,
            utc=True,
            errors="coerce",
        )


        if not pd.isna(
            parsed_time
        ):

            time_text = (
                parsed_time
                .tz_convert(
                    "Asia/Ho_Chi_Minh"
                )
                .strftime(
                    "%H:%M:%S"
                )
            )

        else:

            time_text = str(
                event_time
            )


        output.append(
            {
                "Thời gian":
                    time_text,

                "Hành vi":
                    first_value(
                        event,
                        "event_name",
                        "eventName",
                        "event_id",
                        "eventId",
                    ),

                "Kết quả":
                    first_value(
                        event,
                        "event_result_label",
                        "eventResultLabel",
                        "event_result",
                        "eventResult",
                    ),

                "Thời lượng (ms)":
                    first_value(
                        event,
                        "duration_ms",
                        "durationMs",
                    ),

                "Mã nguyên nhân":
                    first_value(
                        event,
                        "cause_code",
                        "causeCode",
                        "sub_cause_code",
                        "subCauseCode",
                    ),

                "TAC":
                    first_value(
                        event,
                        "tac",
                    ),

                "ECI":
                    first_value(
                        event,
                        "eci",
                    ),
            }
        )


    return pd.DataFrame(
        output
    )


# ============================================================
# MAIN CHART
# ============================================================

def render_anomaly_chart(
    df: pd.DataFrame,
    threshold: float,
    selected_ue: str,
) -> None:
    """
    Biểu đồ phát hiện bất thường.

    Mỗi điểm = 1 Gold window.

    Mặc định:
        hiển thị toàn bộ Gold window của toàn mạng
        và giảm mật độ điểm chỉ ở tầng hiển thị.

    Khi chọn UE:
        chỉ hiển thị Gold window của UE đó.

    threshold vẫn được dùng để xác định:
        display_is_anomaly

    và được vẽ trên biểu đồ như ngưỡng nghiệp vụ.
    """


    # ========================================================
    # 1. FILTER THEO UE
    # ========================================================

    if selected_ue == "__ALL__":

        chart_df = df.copy()

        chart_title = (
            "Trạng thái của các thuê bao"
        )

    else:

        chart_df = (
            df[
                df["ue_key"]
                ==
                selected_ue
            ]
            .copy()
        )

        chart_title = (
            "Phát hiện bất thường "
            f"— UE {selected_ue}"
        )


    # ========================================================
    # 2. CARD
    # ========================================================

    with st.container(
        border=True,
        key="chart_card",
    ):

        st.subheader(
            chart_title
        )


        # ----------------------------------------------------
        # Nếu đang xem riêng một UE
        # ----------------------------------------------------

        if selected_ue != "__ALL__":

            view_col1, view_col2 = (
                st.columns(
                    [4, 1]
                )
            )

            with view_col1:

                st.info(
                    (
                        "Đang hiển thị dữ liệu của "
                        f"UE {selected_ue}"
                    )
                )

            with view_col2:

                st.button(
                    "Xem toàn mạng",
                    key="chart_show_all",
                    use_container_width=True,
                    on_click=select_ue,
                    args=("__ALL__",),
                )


        # ====================================================
        # 3. LEGEND
        # ========================================================
        #
        # Bỏ hoàn toàn:
        #
        #     Ngưỡng quyết định
        #
        # Chỉ còn 2 loại:
        # - điểm bình thường
        # - điểm cảnh báo
        # ====================================================

        legend1, legend2, legend3 = st.columns(
            3
        )

        with legend1:

            st.markdown(
                "🔵 **Điểm bất thường theo thời gian**"
            )

        with legend2:

            st.markdown(
                "🔴 **Cửa sổ vượt ngưỡng**"
            )

        with legend3:

            st.markdown(
                "🟡 **Ngưỡng nghiệp vụ**"
            )


        # ====================================================
        # 4. PREPARE DATA
        # ====================================================

        chart_df[
            "event_datetime"
        ] = pd.to_datetime(
            chart_df[
                "window_end_event_time"
            ],
            utc=True,
            errors="coerce",
        )


        chart_df[
            "Điểm"
        ] = pd.to_numeric(
            chart_df[
                "anomaly_score"
            ],
            errors="coerce",
        )


        chart_df = (
            chart_df
            .dropna(
                subset=[
                    "event_datetime",
                    "Điểm",
                ]
            )
        )


        if chart_df.empty:

            st.info(
                "Chưa có đủ dữ liệu để hiển thị biểu đồ."
            )

            return


        # ====================================================
        # 5. UTC -> GIỜ VIỆT NAM
        # ====================================================

        chart_df[
            "event_datetime"
        ] = (
            chart_df[
                "event_datetime"
            ]
            .dt
            .tz_convert(
                "Asia/Ho_Chi_Minh"
            )
            .dt
            .tz_localize(
                None
            )
        )


        # ====================================================
        # 6. SORT
        # ========================================================
        #
        # Rất quan trọng:
        #
        # Vì chúng ta muốn nối tất cả Gold window thành
        # MỘT đường duy nhất nên phải sort theo thời gian.
        # ====================================================

        chart_df = (
            chart_df
            .sort_values(
                "event_datetime"
            )
            .reset_index(
                drop=True
            )
        )

        display_df, bucket_seconds = (
            build_chart_display_series(
                chart_df
            )
        )

        if bucket_seconds > 0:

            st.caption(
                (
                    f"Biểu đồ được rút gọn theo khoảng "
                    f"{bucket_seconds} giây để dễ quan sát. "
                    "KPI và cảnh báo vẫn được tính trên toàn bộ dữ liệu."
                )
            )


        chart_df[
            "Thời gian"
        ] = (
            chart_df[
                "event_datetime"
            ]
            .dt
            .strftime(
                "%H:%M:%S"
            )
        )


        # ====================================================
        # 7. X RANGE
        # ====================================================

        first_time = (
            chart_df[
                "event_datetime"
            ]
            .min()
        )

        last_time = (
            chart_df[
                "event_datetime"
            ]
            .max()
        )


        if first_time == last_time:

            padding = pd.Timedelta(
                seconds=10
            )

        else:

            span = (
                last_time
                -
                first_time
            )

            padding = max(
                pd.Timedelta(
                    seconds=5
                ),
                span * 0.03,
            )

        display_start = (
            first_time
            -
            padding
        )

        display_end = (
            last_time
            +
            padding
        )

        time_span = (
            last_time
            -
            first_time
        )

        time_format = (
            "%H:%M"
            if
            time_span
            >=
            pd.Timedelta(
                minutes=10
            )
            else
            "%H:%M:%S"
        )


        # ====================================================
        # 8. X AXIS
        # ====================================================

        x_axis = alt.X(
            "event_datetime:T",

            title="Thời gian",

            scale=alt.Scale(
                domain=[
                    display_start,
                    display_end,
                ]
            ),

            axis=alt.Axis(
                format=time_format,
                tickCount=5,
                labelAngle=0,
                labelPadding=10,
                grid=False,
            ),
        )


        # ====================================================
        # 9. Y AXIS
        # ====================================================

        y_axis = alt.Y(
            "Điểm:Q",

            title="Điểm bất thường",

            scale=alt.Scale(
                domain=[
                    0.0,
                    1.0,
                ]
            ),

            axis=alt.Axis(
                values=[
                    0.0,
                    0.2,
                    0.4,
                    0.6,
                    0.8,
                    1.0,
                ],
                grid=True,
            ),
        )


        # ====================================================
        # 10. MAIN LINE
        # ========================================================
        #
        # Không dùng:
        #
        #     detail="ue_key"
        #
        # nữa.
        #
        # Chỉ downsample dữ liệu của line; alert vẫn lấy dữ liệu gốc.
        # ====================================================

        score_line = (
            alt.Chart(
                display_df
            )
            .mark_line(
                color=COLOR_PRIMARY,
                strokeWidth=2.4,
                opacity=0.90,
            )
            .encode(
                x=x_axis,
                y=y_axis,

                tooltip=[
                    alt.Tooltip(
                        "Thời gian:N",
                        title="Thời gian",
                    ),

                    alt.Tooltip(
                        "Điểm:Q",
                        title="Điểm bất thường",
                        format=".3f",
                    ),
                ],
            )
        )


        # ====================================================
        # 11. ALERT POINTS
        # ====================================================

        alert_df = (
            chart_df[
                chart_df[
                    "display_is_anomaly"
                ]
            ]
        )


        alert_points = (
            alt.Chart(
                alert_df
            )
            .mark_circle(
                size=155,
                color=COLOR_DANGER,
                stroke="#FFFFFF",
                strokeWidth=2.5,
                opacity=1.0,
            )
            .encode(

                x=x_axis,

                y=y_axis,

                tooltip=[
                    alt.Tooltip(
                        "Thời gian:N",
                        title="Thời gian",
                    ),

                    alt.Tooltip(
                        "Điểm:Q",
                        title="Điểm bất thường",
                        format=".3f",
                    ),

                    alt.Tooltip(
                        "ue_key:N",
                        title="UE",
                    ),
                ],
            )
        )


        # ====================================================
        # 12. THRESHOLD LINE
        # ====================================================

        threshold_line = (
            alt.Chart(
                pd.DataFrame(
                    {
                        "Ngưỡng": [
                            threshold,
                        ]
                    }
                )
            )
            .mark_rule(
                color=COLOR_WARNING,
                strokeWidth=2.2,
                strokeDash=[8, 5],
            )
            .encode(
                y=alt.Y(
                    "Ngưỡng:Q"
                )
            )
        )


        # ====================================================
        # 13. FINAL CHART
        # ====================================================

        chart = (
            score_line
            +
            alert_points
            +
            threshold_line
        ).properties(
            height=410,
        ).configure_view(
            stroke=None,
            fill="#FFFFFF",
        ).configure_axis(
            labelColor=COLOR_TEXT_SECONDARY,
            titleColor=COLOR_TEXT,
            domainColor=COLOR_BORDER,
            tickColor=COLOR_BORDER,
            gridColor="#E8F0F7",
            labelFontSize=12,
            titleFontSize=13,
        )


        st.altair_chart(
            chart,
            use_container_width=True,
            theme=None,
        )


# ============================================================
# UE ALERTS
# ============================================================

def render_ue_alerts(
    rows: list[dict],
    threshold: float,
) -> None:
    """
    Danh sách các UE đang vượt ngưỡng.

    Người dùng có thể:
    - mở UE để xem hành vi;
    - chọn UE để lọc biểu đồ chính.
    """


    st.subheader(
        "Thuê bao cần chú ý"
    )


    st.caption(
        (
            "Chọn một thuê bao để xem chi tiết hoặc "
            "hiển thị riêng thuê bao đó trên biểu đồ."
        )
    )


    summaries = (
        build_anomalous_ue_summary(
            rows
        )
    )


    # ========================================================
    # NO ALERT
    # ========================================================

    if not summaries:

        st.success(
            (
                "Chưa phát hiện thuê bao nào "
                "vượt ngưỡng cảnh báo hiện tại."
            )
        )

        return


    # ========================================================
    # SUMMARY TABLE
    # ========================================================

    summary_table = []


    for summary in summaries:

        latest_time = pd.to_datetime(
            summary[
                "latest_row"
            ].get(
                "window_end_event_time"
            ),
            utc=True,
            errors="coerce",
        )


        if not pd.isna(
            latest_time
        ):

            latest_time_text = (
                latest_time
                .tz_convert(
                    "Asia/Ho_Chi_Minh"
                )
                .strftime(
                    "%H:%M:%S"
                )
            )

        else:

            latest_time_text = "-"


        summary_table.append(
            {
                "UE":
                    summary[
                        "ue_key"
                    ],

                "Mức cảnh báo":
                    summary[
                        "severity"
                    ],

                "Số lần cảnh báo":
                    summary[
                        "anomaly_count"
                    ],

                "Điểm cao nhất":
                    round(
                        summary[
                            "max_score"
                        ],
                        3,
                    ),

                "Cảnh báo gần nhất":
                    latest_time_text,
            }
        )


    st.dataframe(
        pd.DataFrame(
            summary_table
        ),
        hide_index=True,
        use_container_width=True,
    )


    st.write("")


    # ========================================================
    # UE DETAILS
    # ========================================================

    for summary in summaries:


        ue_key = (
            summary[
                "ue_key"
            ]
        )


        label = (
            f"{ue_key}"
            f" · "
            f"{summary['severity']}"
            f" · "
            f"Điểm cao nhất "
            f"{summary['max_score']:.3f}"
        )


        with st.expander(
            label,
            expanded=False,
        ):


            # =================================================
            # CHART ACTION
            # =================================================

            action1, action2 = (
                st.columns(
                    [1.4, 3]
                )
            )


            with action1:

                is_selected = (
                    st.session_state.selected_ue
                    ==
                    ue_key
                )


                button_text = (
                    "✓ Đang xem trên biểu đồ"
                    if is_selected
                    else
                    "Xem trên biểu đồ"
                )


                st.button(
                    button_text,
                    key=
                        f"select_ue_{ue_key}",

                    use_container_width=
                        True,

                    on_click=
                        select_ue,

                    args=(
                        ue_key,
                    ),
                )


            with action2:

                st.caption(
                    (
                        "Số Gold window vượt ngưỡng "
                        f"{threshold:.3f}: "
                        f"{summary['anomaly_count']}"
                    )
                )


            # =================================================
            # SEVERITY
            # =================================================

            if (
                summary[
                    "severity"
                ]
                ==
                "Cảnh báo cao"
            ):

                st.error(
                    "🔴 Cảnh báo cao"
                )


            elif (
                summary[
                    "severity"
                ]
                ==
                "Cảnh báo lặp lại"
            ):

                st.warning(
                    "🟠 Cảnh báo lặp lại"
                )


            else:

                st.info(
                    "🟡 Cần theo dõi"
                )


            # =================================================
            # UE KPI
            # =================================================

            detail1, detail2, detail3 = (
                st.columns(
                    3
                )
            )


            detail1.metric(
                "Số lần cảnh báo",
                summary[
                    "anomaly_count"
                ],
            )


            detail2.metric(
                "Điểm cao nhất",
                (
                    f"{summary['max_score']:.3f}"
                ),
            )


            latest_time = pd.to_datetime(
                summary[
                    "latest_row"
                ].get(
                    "window_end_event_time"
                ),
                utc=True,
                errors="coerce",
            )


            if not pd.isna(
                latest_time
            ):

                latest_time_text = (
                    latest_time
                    .tz_convert(
                        "Asia/Ho_Chi_Minh"
                    )
                    .strftime(
                        "%H:%M:%S"
                    )
                )

            else:

                latest_time_text = "-"


            detail3.metric(
                "Cảnh báo gần nhất",
                latest_time_text,
            )


            # =================================================
            # HISTORY
            # =================================================

            st.markdown(
                "#### Lịch sử cảnh báo"
            )


            history = []


            for row in sorted(
                summary[
                    "anomaly_rows"
                ],

                key=lambda item: str(
                    item.get(
                        "window_end_event_time",
                        "",
                    )
                ),
            ):


                event_time = pd.to_datetime(
                    row.get(
                        "window_end_event_time"
                    ),
                    utc=True,
                    errors="coerce",
                )


                if not pd.isna(
                    event_time
                ):

                    event_time_text = (
                        event_time
                        .tz_convert(
                            "Asia/Ho_Chi_Minh"
                        )
                        .strftime(
                            "%H:%M:%S"
                        )
                    )

                else:

                    event_time_text = "-"


                history.append(
                    {
                        "Thời gian":
                            event_time_text,

                        "Điểm bất thường":
                            round(
                                float(
                                    row.get(
                                        "anomaly_score",
                                        0.0,
                                    )
                                ),
                                3,
                            ),
                    }
                )


            st.dataframe(
                pd.DataFrame(
                    history
                ),
                hide_index=True,
                use_container_width=True,
            )


            # =================================================
            # BEHAVIOUR
            # =================================================

            st.markdown(
                "#### Hành vi đáng chú ý"
            )


            evidence_df = (
                evidence_to_dataframe(
                    summary[
                        "highest_row"
                    ]
                )
            )


            if evidence_df.empty:

                st.caption(
                    "Chưa có dữ liệu hành vi chi tiết."
                )

            else:

                st.dataframe(
                    evidence_df,
                    hide_index=True,
                    use_container_width=True,
                )


# ============================================================
# LIVE DASHBOARD
# ============================================================

@st.fragment(
    run_every=1.0
)
def render_live_dashboard() -> None:


    rows = (
        prediction_buffer
        .snapshot()
    )


    # ========================================================
    # WAITING
    # ========================================================

    if not rows:

        st.info(
            (
                "Hệ thống đang hoạt động "
                "và chờ dữ liệu thuê bao."
            )
        )

        return


    # ========================================================
    # DATAFRAME
    # ========================================================

    df = pd.DataFrame(
        rows
    )

    if "model" not in df.columns:
        st.warning(
            "Prediction chưa có thông tin model."
        )
        return

    selected_model = st.session_state.get(
        "selected_model",
        "MixedTransformer",
    )

    df = df[
        df["model"] == selected_model
    ].copy()

    if df.empty:
        st.info(
            (
                "Đang chờ prediction từ mô hình "
                f"{MODEL_DISPLAY_NAMES.get(selected_model, selected_model)}."
            )
        )
        return

    numeric_columns = [
        "raw_score",
        "anomaly_score",
        "anomaly_threshold",
    ]


    for column in numeric_columns:

        if column in df.columns:

            df[
                column
            ] = pd.to_numeric(
                df[
                    column
                ],
                errors="coerce",
            )


    # ========================================================
    # DASHBOARD THRESHOLD
    # ========================================================
    #
    # Model prediction gốc:
    #
    #     is_anomaly
    #
    # vẫn được giữ nguyên trong Kafka.
    #
    # Dashboard tạo thêm:
    #
    #     display_is_anomaly
    #
    # để cho phép người dùng thay đổi threshold.
    # ========================================================

    df[
        "display_is_anomaly"
    ] = (
        df[
            "anomaly_score"
        ]
        >=
        display_threshold
    )


    # ========================================================
    # RESET UE FILTER IF NECESSARY
    # ========================================================

    observed_ues = set(
        df[
            "ue_key"
        ]
        .astype(
            str
        )
        .tolist()
    )


    if (
        st.session_state.selected_ue
        !=
        "__ALL__"
        and
        st.session_state.selected_ue
        not in
        observed_ues
    ):

        st.session_state.selected_ue = (
            "__ALL__"
        )


    # ========================================================
    # KPI 1 - UE bất thường
    # ========================================================

    total_ues = int(
        df[
            "ue_key"
        ]
        .nunique()
    )


    anomalous_ues = int(
        df.loc[
            df[
                "display_is_anomaly"
            ],
            "ue_key",
        ]
        .nunique()
    )


    # ========================================================
    # KPI 2 - tỷ lệ window cảnh báo
    # ========================================================

    total_windows = len(
        df
    )


    anomaly_windows = int(
        df[
            "display_is_anomaly"
        ]
        .sum()
    )


    anomaly_request_rate = (
        anomaly_windows
        /
        total_windows
        if total_windows
        else
        0.0
    )


    # ========================================================
    # KPI 3 - score trung bình
    # ========================================================

    average_score = float(
        df[
            "anomaly_score"
        ]
        .mean()
    )


    # ========================================================
    # KPI UI
    # ========================================================

    kpi1, kpi2, kpi3 = (
        st.columns(
            3
        )
    )


    with kpi1:

        render_kpi(
            title=
                "Thuê bao có ít nhất 1 cảnh báo",

            value=
                (
                    f"{anomalous_ues}"
                    f"/"
                    f"{total_ues}"
                ),

            key=
                "kpi_ue",
        )


    with kpi2:

        render_kpi(
            title=
                "Tỷ lệ cửa sổ bất thường",

            value=
                f"{anomaly_request_rate:.2%}",

            key=
                "kpi_request",
        )


    with kpi3:

        render_kpi(
            title=
                "Điểm bất thường trung bình",

            value=
                f"{average_score:.3f}",

            key=
                "kpi_score",
        )


    st.write("")


    # ========================================================
    # CHART
    # ========================================================

    render_anomaly_chart(
        df=
            df,

        threshold=
            display_threshold,

        selected_ue=
            st.session_state.selected_ue,
    )


    st.write("")


    # ========================================================
    # UE ALERTS
    # ========================================================

    display_rows = (
        df.to_dict(
            "records"
        )
    )


    render_ue_alerts(
        rows=
            display_rows,

        threshold=
            display_threshold,
    )


# ============================================================
# RENDER
# ============================================================

render_live_dashboard()