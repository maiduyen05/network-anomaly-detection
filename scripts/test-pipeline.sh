#!/usr/bin/env bash

set -Eeuo pipefail


# ============================================================
# PROJECT PATHS
# ============================================================

SCRIPT_DIRECTORY="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
  pwd
)"

PROJECT_DIRECTORY="$(
  cd -- "${SCRIPT_DIRECTORY}/.."
  pwd
)"

cd "${PROJECT_DIRECTORY}"


# ============================================================
# RUNTIME CONFIGURATION
# ============================================================

KAFKA_CONTAINER="a-kafka"
FLINK_JOBMANAGER_CONTAINER="a-flink-jobmanager"

KAFKA_BOOTSTRAP="kafka:29092"

KAFKA_BIN="/opt/kafka/bin"

SMOKE_DATA_SCRIPT="${PROJECT_DIRECTORY}/scripts/create-gold-smoke-data.sh"

SMOKE_DIRECTORY="${PROJECT_DIRECTORY}/data/smoke-gold"

SMOKE_FILE="${SMOKE_DIRECTORY}/gold-smoke.log"


# ============================================================
# TOPICS
# ============================================================

RAW_TOPIC="raw.ue.log.line"
BRONZE_TOPIC="bronze.ue.event"
SILVER_TOPIC="silver.ue.event"
GOLD_TOPIC="gold.ue.sequence"

BRONZE_DLQ_TOPIC="dlq.ue.log.line"

SILVER_INVALID_IDENTITY_TOPIC="invalid-identity"
SILVER_UNSUPPORTED_EVENT_TOPIC="unsupported-event"
SILVER_LATE_EVENT_TOPIC="late-ue-event"

GOLD_TOO_LATE_TOPIC="gold-too-late-event"
GOLD_INVALID_FEATURE_TOPIC="invalid-gold-feature"


# ============================================================
# EXPECTED RESULT
# ============================================================

EXPECTED_RAW_COUNT=41
EXPECTED_BRONZE_COUNT=41
EXPECTED_SILVER_COUNT=41
EXPECTED_GOLD_COUNT=2

EXPECTED_SEQUENCE_LENGTH=32
EXPECTED_SEQUENCE_STRIDE=8

EXPECTED_CAT_FEATURE_COUNT=4
EXPECTED_NUM_FEATURE_COUNT=2

EXPECTED_FEATURE_VERSION="gold-ue-sequence-feature-v2"


# ============================================================
# WAIT CONFIGURATION
# ============================================================
#
# Bronze / Silver / Gold đều dùng EXACTLY_ONCE Kafka sink.
#
# Record chỉ visible với read_committed sau checkpoint.
#
# Pipeline có nhiều tầng nên cho phép tối đa 6 phút.
# ============================================================

GOLD_WAIT_TIMEOUT_SECONDS=360
POLL_INTERVAL_SECONDS=10

CONSUMER_TIMEOUT_MS=3000


# ============================================================
# TEMPORARY TEST DIRECTORY
# ============================================================

TEST_RUN_DIRECTORY="$(
  mktemp -d /tmp/network-anomaly-smoke.XXXXXX
)"

OFFSET_DIRECTORY="${TEST_RUN_DIRECTORY}/offsets"
RECORD_DIRECTORY="${TEST_RUN_DIRECTORY}/records"

mkdir -p \
  "${OFFSET_DIRECTORY}" \
  "${RECORD_DIRECTORY}"


# ============================================================
# CLEANUP
# ============================================================

cleanup() {

  local exit_code=$?

  echo

  if [[ "${exit_code}" -eq 0 ]]; then

    rm -rf "${TEST_RUN_DIRECTORY}"

  else

    echo "================================================"
    echo " SMOKE TEST FAILED"
    echo "================================================"
    echo
    echo "Diagnostic files được giữ tại:"
    echo
    echo "  ${TEST_RUN_DIRECTORY}"
    echo
  fi
}

trap cleanup EXIT


# ============================================================
# HELPERS
# ============================================================

fail() {

  echo
  echo "ERROR: $*" >&2
  echo

  exit 1
}


require_command() {

  local command_name="$1"

  if ! command -v "${command_name}" >/dev/null 2>&1; then
    fail "Không tìm thấy command: ${command_name}"
  fi
}


# ============================================================
# PRE-CHECK: HOST COMMANDS
# ============================================================

require_command docker
require_command mvn
require_command python3
require_command awk
require_command grep
require_command wc


# ============================================================
# PRE-CHECK: CONTAINERS
# ============================================================

echo
echo "================================================"
echo " CHECK RUNTIME"
echo "================================================"
echo

for container_name in \
  "${KAFKA_CONTAINER}" \
  "${FLINK_JOBMANAGER_CONTAINER}"
do

  if ! docker inspect \
      "${container_name}" \
      >/dev/null 2>&1; then

    fail "Không tìm thấy container ${container_name}"
  fi


  container_running="$(
    docker inspect \
      -f '{{.State.Running}}' \
      "${container_name}"
  )"

  if [[ "${container_running}" != "true" ]]; then
    fail "Container ${container_name} chưa RUNNING"
  fi

done


# ============================================================
# PRE-CHECK: KAFKA TOOLS
# ============================================================

if ! docker exec \
    "${KAFKA_CONTAINER}" \
    test -x "${KAFKA_BIN}/kafka-get-offsets.sh"; then

  fail "Không tìm thấy kafka-get-offsets.sh trong Kafka container"
fi


if ! docker exec \
    "${KAFKA_CONTAINER}" \
    test -x "${KAFKA_BIN}/kafka-console-consumer.sh"; then

  fail "Không tìm thấy kafka-console-consumer.sh"
fi


# ============================================================
# FLINK JOB CHECK
# ============================================================

running_jobs() {

  docker exec \
    "${FLINK_JOBMANAGER_CONTAINER}" \
    flink list -r \
    2>/dev/null
}


count_running_job() {

  local job_name="$1"

  running_jobs |
    grep -F -c \
      "${job_name} (RUNNING)" \
      || true
}


verify_pipeline_running() {

  local bronze_count
  local silver_count
  local gold_count

  bronze_count="$(
    count_running_job "flink-bronze-v1"
  )"

  silver_count="$(
    count_running_job "flink-silver-v1"
  )"

  gold_count="$(
    count_running_job "flink-gold-v1"
  )"


  if [[ "${bronze_count}" -ne 1 ]]; then
    fail "Bronze RUNNING count = ${bronze_count}, expected 1"
  fi


  if [[ "${silver_count}" -ne 1 ]]; then
    fail "Silver RUNNING count = ${silver_count}, expected 1"
  fi


  if [[ "${gold_count}" -ne 1 ]]; then
    fail "Gold RUNNING count = ${gold_count}, expected 1"
  fi
}


verify_pipeline_running

echo "Bronze RUNNING: PASS"
echo "Silver RUNNING: PASS"
echo "Gold RUNNING:   PASS"


# ============================================================
# CHECK TOPICS
# ============================================================

echo
echo "================================================"
echo " CHECK KAFKA TOPICS"
echo "================================================"
echo

TOPIC_LIST="$(
  docker exec \
    "${KAFKA_CONTAINER}" \
    "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
    --list
)"


require_topic() {

  local topic="$1"

  if ! printf '%s\n' "${TOPIC_LIST}" |
      grep -Fxq "${topic}"; then

    fail "Kafka topic không tồn tại: ${topic}"
  fi

  echo "PASS: ${topic}"
}


require_topic "${RAW_TOPIC}"
require_topic "${BRONZE_TOPIC}"
require_topic "${SILVER_TOPIC}"
require_topic "${GOLD_TOPIC}"

require_topic "${BRONZE_DLQ_TOPIC}"

require_topic "${SILVER_INVALID_IDENTITY_TOPIC}"
require_topic "${SILVER_UNSUPPORTED_EVENT_TOPIC}"
require_topic "${SILVER_LATE_EVENT_TOPIC}"

require_topic "${GOLD_TOO_LATE_TOPIC}"
require_topic "${GOLD_INVALID_FEATURE_TOPIC}"


# ============================================================
# CREATE SMOKE DATA
# ============================================================

echo
echo "================================================"
echo " CREATE SMOKE DATA"
echo "================================================"
echo

"${SMOKE_DATA_SCRIPT}"


if [[ ! -f "${SMOKE_FILE}" ]]; then
  fail "Không tạo được ${SMOKE_FILE}"
fi


# ============================================================
# INPUT DIRECTORY SAFETY
# ============================================================
#
# LogProducerApplication đọc TẤT CẢ regular file trong directory.
#
# Smoke directory phải chỉ có một file để tránh vô tình gửi
# dữ liệu cũ vào Kafka.
# ============================================================

SMOKE_REGULAR_FILE_COUNT="$(
  find "${SMOKE_DIRECTORY}" \
    -maxdepth 1 \
    -type f \
    | wc -l
)"


if [[ "${SMOKE_REGULAR_FILE_COUNT}" -ne 1 ]]; then

  echo
  echo "Các file hiện có:" >&2

  find "${SMOKE_DIRECTORY}" \
    -maxdepth 1 \
    -type f \
    -printf '  %f\n' >&2

  fail "data/smoke-gold phải chỉ chứa đúng gold-smoke.log"
fi


# ============================================================
# READ UNIQUE IMSI
# ============================================================

SMOKE_IMSI="$(
  awk -F';' '
    NR == 1 {
      print $7
      exit
    }
  ' "${SMOKE_FILE}"
)"


if [[ -z "${SMOKE_IMSI}" ]]; then
  fail "Không đọc được IMSI từ smoke file"
fi


echo
echo "Smoke IMSI:"
echo "  ${SMOKE_IMSI}"
echo


# ============================================================
# VERIFY GENERATED INPUT
# ============================================================

RAW_INPUT_COUNT="$(
  wc -l < "${SMOKE_FILE}"
)"


if [[ "${RAW_INPUT_COUNT}" -ne "${EXPECTED_RAW_COUNT}" ]]; then

  fail \
    "Smoke input có ${RAW_INPUT_COUNT} dòng, expected ${EXPECTED_RAW_COUNT}"
fi


INVALID_INPUT_ROWS="$(
  awk -F';' '
    NF != 52 {
      count++
    }

    END {
      print count + 0
    }
  ' "${SMOKE_FILE}"
)"


if [[ "${INVALID_INPUT_ROWS}" -ne 0 ]]; then
  fail "Smoke input có row không đủ 52 field"
fi


echo "Smoke input records: ${RAW_INPUT_COUNT}"
echo "52-field validation: PASS"


# ============================================================
# OFFSET HELPERS
# ============================================================
#
# Capture Kafka end offset TRƯỚC khi gửi smoke data.
#
# Sau đó consumer chỉ đọc từ các offset này trở đi.
# ============================================================

capture_offsets() {

  local topic="$1"
  local output_file="$2"

  docker exec \
    "${KAFKA_CONTAINER}" \
    "${KAFKA_BIN}/kafka-get-offsets.sh" \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
    --topic "${topic}" \
    --time -1 \
    | tr -d '\r' \
    > "${output_file}"


  if [[ ! -s "${output_file}" ]]; then
    fail "Không capture được baseline offset của ${topic}"
  fi
}


echo
echo "================================================"
echo " CAPTURE KAFKA BASELINE OFFSETS"
echo "================================================"
echo


capture_offsets \
  "${RAW_TOPIC}" \
  "${OFFSET_DIRECTORY}/raw.offsets"

capture_offsets \
  "${BRONZE_TOPIC}" \
  "${OFFSET_DIRECTORY}/bronze.offsets"

capture_offsets \
  "${SILVER_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver.offsets"

capture_offsets \
  "${GOLD_TOPIC}" \
  "${OFFSET_DIRECTORY}/gold.offsets"


capture_offsets \
  "${BRONZE_DLQ_TOPIC}" \
  "${OFFSET_DIRECTORY}/bronze-dlq.offsets"

capture_offsets \
  "${SILVER_INVALID_IDENTITY_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver-invalid.offsets"

capture_offsets \
  "${SILVER_UNSUPPORTED_EVENT_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver-unsupported.offsets"

capture_offsets \
  "${SILVER_LATE_EVENT_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver-late.offsets"

capture_offsets \
  "${GOLD_TOO_LATE_TOPIC}" \
  "${OFFSET_DIRECTORY}/gold-too-late.offsets"

capture_offsets \
  "${GOLD_INVALID_FEATURE_TOPIC}" \
  "${OFFSET_DIRECTORY}/gold-invalid-feature.offsets"


echo "Baseline captured."


# ============================================================
# CONSUME SINCE BASELINE
# ============================================================
#
# Mỗi dòng baseline:
#
# topic:partition:offset
#
# Consumer sử dụng:
#
# isolation.level=read_committed
#
# nên chỉ nhìn thấy transaction đã commit của Flink
# EXACTLY_ONCE Kafka sinks.
# ============================================================

consume_since_baseline() {

  local topic="$1"
  local baseline_file="$2"
  local output_file="$3"

  : > "${output_file}"


  while IFS=: read -r \
      baseline_topic \
      partition \
      offset
  do

    [[ -n "${baseline_topic}" ]] || continue

    if [[ "${baseline_topic}" != "${topic}" ]]; then
      continue
    fi


    docker exec \
      "${KAFKA_CONTAINER}" \
      "${KAFKA_BIN}/kafka-console-consumer.sh" \
      --bootstrap-server "${KAFKA_BOOTSTRAP}" \
      --topic "${topic}" \
      --partition "${partition}" \
      --offset "${offset}" \
      --timeout-ms "${CONSUMER_TIMEOUT_MS}" \
      --consumer-property isolation.level=read_committed \
      >> "${output_file}" \
      2>/dev/null \
      || true

  done < "${baseline_file}"
}


count_smoke_records() {

  local file="$1"

  grep \
    -F \
    -c \
    -- "${SMOKE_IMSI}" \
    "${file}" \
    2>/dev/null \
    || true
}


# ============================================================
# RUN PRODUCER
# ============================================================

echo
echo "================================================"
echo " SEND 41 RAW RECORDS TO KAFKA"
echo "================================================"
echo


mvn \
  -q \
  -f log-producer/pom.xml \
  -DskipTests \
  -Dexec.mainClass=com.network.producer.LogProducerApplication \
  -Dexec.args="${SMOKE_DIRECTORY}" \
  exec:java


echo
echo "Producer command completed."


# ============================================================
# WAIT FOR GOLD
# ============================================================
#
# Chỉ poll Gold.
#
# Nếu 2 Gold sample đã committed thì Raw -> Bronze -> Silver
# đã đi qua pipeline.
#
# Sau đó mới kiểm tra chính xác từng tầng.
# ============================================================

echo
echo "================================================"
echo " WAIT FOR GOLD OUTPUT"
echo "================================================"
echo

GOLD_RECORD_FILE="${RECORD_DIRECTORY}/gold.jsonl"

elapsed=0


while true; do

  verify_pipeline_running


  consume_since_baseline \
    "${GOLD_TOPIC}" \
    "${OFFSET_DIRECTORY}/gold.offsets" \
    "${GOLD_RECORD_FILE}"


  gold_count="$(
    count_smoke_records \
      "${GOLD_RECORD_FILE}"
  )"


  echo \
    "Gold smoke samples: ${gold_count}/${EXPECTED_GOLD_COUNT} | elapsed=${elapsed}s"


  if [[ "${gold_count}" -eq "${EXPECTED_GOLD_COUNT}" ]]; then
    break
  fi


  if [[ "${gold_count}" -gt "${EXPECTED_GOLD_COUNT}" ]]; then

    fail \
      "Gold tạo ${gold_count} samples, expected đúng ${EXPECTED_GOLD_COUNT}"
  fi


  if [[ "${elapsed}" -ge "${GOLD_WAIT_TIMEOUT_SECONDS}" ]]; then

    fail \
      "Timeout sau ${GOLD_WAIT_TIMEOUT_SECONDS}s khi chờ Gold"
  fi


  sleep "${POLL_INTERVAL_SECONDS}"

  elapsed=$((elapsed + POLL_INTERVAL_SECONDS))

done


# ============================================================
# READ RAW / BRONZE / SILVER
# ============================================================

echo
echo "================================================"
echo " VERIFY PIPELINE COUNTS"
echo "================================================"
echo


RAW_RECORD_FILE="${RECORD_DIRECTORY}/raw.jsonl"
BRONZE_RECORD_FILE="${RECORD_DIRECTORY}/bronze.jsonl"
SILVER_RECORD_FILE="${RECORD_DIRECTORY}/silver.jsonl"


consume_since_baseline \
  "${RAW_TOPIC}" \
  "${OFFSET_DIRECTORY}/raw.offsets" \
  "${RAW_RECORD_FILE}"


consume_since_baseline \
  "${BRONZE_TOPIC}" \
  "${OFFSET_DIRECTORY}/bronze.offsets" \
  "${BRONZE_RECORD_FILE}"


consume_since_baseline \
  "${SILVER_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver.offsets" \
  "${SILVER_RECORD_FILE}"


raw_count="$(
  count_smoke_records "${RAW_RECORD_FILE}"
)"

bronze_count="$(
  count_smoke_records "${BRONZE_RECORD_FILE}"
)"

silver_count="$(
  count_smoke_records "${SILVER_RECORD_FILE}"
)"

gold_count="$(
  count_smoke_records "${GOLD_RECORD_FILE}"
)"


printf '%-10s %s\n' \
  "Raw:" \
  "${raw_count}/${EXPECTED_RAW_COUNT}"

printf '%-10s %s\n' \
  "Bronze:" \
  "${bronze_count}/${EXPECTED_BRONZE_COUNT}"

printf '%-10s %s\n' \
  "Silver:" \
  "${silver_count}/${EXPECTED_SILVER_COUNT}"

printf '%-10s %s\n' \
  "Gold:" \
  "${gold_count}/${EXPECTED_GOLD_COUNT}"


if [[ "${raw_count}" -ne "${EXPECTED_RAW_COUNT}" ]]; then
  fail "Raw count mismatch"
fi


if [[ "${bronze_count}" -ne "${EXPECTED_BRONZE_COUNT}" ]]; then
  fail "Bronze count mismatch"
fi


if [[ "${silver_count}" -ne "${EXPECTED_SILVER_COUNT}" ]]; then
  fail "Silver count mismatch"
fi


if [[ "${gold_count}" -ne "${EXPECTED_GOLD_COUNT}" ]]; then
  fail "Gold count mismatch"
fi


# ============================================================
# VALIDATE GOLD MODEL CONTRACT
# ============================================================

echo
echo "================================================"
echo " VALIDATE GOLD MODEL CONTRACT"
echo "================================================"
echo


python3 \
  - "${GOLD_RECORD_FILE}" \
  "${SMOKE_IMSI}" \
  "${EXPECTED_GOLD_COUNT}" \
  "${EXPECTED_SEQUENCE_LENGTH}" \
  "${EXPECTED_SEQUENCE_STRIDE}" \
  "${EXPECTED_CAT_FEATURE_COUNT}" \
  "${EXPECTED_NUM_FEATURE_COUNT}" \
  "${EXPECTED_FEATURE_VERSION}" <<'PY'
import json
import sys


(
    file_path,
    smoke_imsi,
    expected_sample_count,
    expected_sequence_length,
    expected_stride,
    expected_cat_features,
    expected_num_features,
    expected_feature_version,
) = sys.argv[1:]


expected_sample_count = int(expected_sample_count)
expected_sequence_length = int(expected_sequence_length)
expected_stride = int(expected_stride)
expected_cat_features = int(expected_cat_features)
expected_num_features = int(expected_num_features)


samples = []


with open(file_path, "r", encoding="utf-8") as file:
    for line_number, line in enumerate(file, start=1):

        line = line.strip()

        if not line:
            continue

        if smoke_imsi not in line:
            continue

        try:
            sample = json.loads(line)
        except json.JSONDecodeError as exception:
            raise AssertionError(
                f"Gold record line {line_number} is not valid JSON: {exception}"
            ) from exception

        samples.append(sample)


if len(samples) != expected_sample_count:
    raise AssertionError(
        f"Expected {expected_sample_count} Gold samples, got {len(samples)}"
    )


sample_ids = set()


for index, sample in enumerate(samples, start=1):

    if sample.get("imsi") != smoke_imsi:
        raise AssertionError(
            f"Sample {index}: unexpected imsi={sample.get('imsi')}"
        )


    if sample.get("feature_version") != expected_feature_version:
        raise AssertionError(
            f"Sample {index}: feature_version="
            f"{sample.get('feature_version')}, "
            f"expected={expected_feature_version}"
        )


    if sample.get("sequence_length") != expected_sequence_length:
        raise AssertionError(
            f"Sample {index}: sequence_length="
            f"{sample.get('sequence_length')}"
        )


    if sample.get("stride") != expected_stride:
        raise AssertionError(
            f"Sample {index}: stride={sample.get('stride')}"
        )


    sample_id = sample.get("sample_id")

    if not sample_id:
        raise AssertionError(
            f"Sample {index}: missing sample_id"
        )

    if sample_id in sample_ids:
        raise AssertionError(
            f"Duplicate sample_id: {sample_id}"
        )

    sample_ids.add(sample_id)


    model_input = sample.get("model_input")

    if not isinstance(model_input, dict):
        raise AssertionError(
            f"Sample {index}: model_input is missing"
        )


    # --------------------------------------------------------
    # Exact JSON names required by model contract.
    # --------------------------------------------------------

    if "x_cat" not in model_input:
        raise AssertionError(
            f"Sample {index}: model_input.x_cat missing"
        )

    if "x_num" not in model_input:
        raise AssertionError(
            f"Sample {index}: model_input.x_num missing"
        )


    # Old incorrect JSON names must not appear.
    if "xcat" in model_input:
        raise AssertionError(
            f"Sample {index}: old field xcat still exists"
        )

    if "xnum" in model_input:
        raise AssertionError(
            f"Sample {index}: old field xnum still exists"
        )


    x_cat = model_input["x_cat"]
    x_num = model_input["x_num"]


    if not isinstance(x_cat, list):
        raise AssertionError(
            f"Sample {index}: x_cat is not a list"
        )


    if len(x_cat) != expected_sequence_length:
        raise AssertionError(
            f"Sample {index}: x_cat rows={len(x_cat)}, "
            f"expected={expected_sequence_length}"
        )


    for row_index, row in enumerate(x_cat):

        if not isinstance(row, list):
            raise AssertionError(
                f"Sample {index}: x_cat[{row_index}] is not a list"
            )

        if len(row) != expected_cat_features:
            raise AssertionError(
                f"Sample {index}: "
                f"x_cat[{row_index}] width={len(row)}, "
                f"expected={expected_cat_features}"
            )

        if not all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in row
        ):
            raise AssertionError(
                f"Sample {index}: "
                f"x_cat[{row_index}] contains non-integer value"
            )


    if not isinstance(x_num, list):
        raise AssertionError(
            f"Sample {index}: x_num is not a list"
        )


    if len(x_num) != expected_sequence_length:
        raise AssertionError(
            f"Sample {index}: x_num rows={len(x_num)}, "
            f"expected={expected_sequence_length}"
        )


    for row_index, row in enumerate(x_num):

        if not isinstance(row, list):
            raise AssertionError(
                f"Sample {index}: x_num[{row_index}] is not a list"
            )

        if len(row) != expected_num_features:
            raise AssertionError(
                f"Sample {index}: "
                f"x_num[{row_index}] width={len(row)}, "
                f"expected={expected_num_features}"
            )

        if not all(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            for value in row
        ):
            raise AssertionError(
                f"Sample {index}: "
                f"x_num[{row_index}] contains non-numeric value"
            )


print("Gold JSON syntax: PASS")
print("feature_version: PASS")
print("sequence_length=32: PASS")
print("stride=8: PASS")
print("x_cat[32][4]: PASS")
print("x_num[32][2]: PASS")
print("x_cat/x_num JSON naming: PASS")
print("sample_id uniqueness: PASS")
PY


# ============================================================
# SIDE OUTPUT VALIDATION
# ============================================================

echo
echo "================================================"
echo " VERIFY SIDE OUTPUTS"
echo "================================================"
echo


check_side_output_zero() {

  local label="$1"
  local topic="$2"
  local baseline_file="$3"

  local output_file

  output_file="$(
    printf '%s/%s.jsonl' \
      "${RECORD_DIRECTORY}" \
      "${label}"
  )"


  consume_since_baseline \
    "${topic}" \
    "${baseline_file}" \
    "${output_file}"


  local count

  count="$(
    count_smoke_records \
      "${output_file}"
  )"


  if [[ "${count}" -ne 0 ]]; then

    fail \
      "${topic} contains ${count} record(s) for smoke IMSI"
  fi


  printf '%-30s PASS\n' "${topic}"
}


check_side_output_zero \
  "bronze-dlq" \
  "${BRONZE_DLQ_TOPIC}" \
  "${OFFSET_DIRECTORY}/bronze-dlq.offsets"


check_side_output_zero \
  "silver-invalid" \
  "${SILVER_INVALID_IDENTITY_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver-invalid.offsets"


check_side_output_zero \
  "silver-unsupported" \
  "${SILVER_UNSUPPORTED_EVENT_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver-unsupported.offsets"


check_side_output_zero \
  "silver-late" \
  "${SILVER_LATE_EVENT_TOPIC}" \
  "${OFFSET_DIRECTORY}/silver-late.offsets"


check_side_output_zero \
  "gold-too-late" \
  "${GOLD_TOO_LATE_TOPIC}" \
  "${OFFSET_DIRECTORY}/gold-too-late.offsets"


check_side_output_zero \
  "gold-invalid-feature" \
  "${GOLD_INVALID_FEATURE_TOPIC}" \
  "${OFFSET_DIRECTORY}/gold-invalid-feature.offsets"


# ============================================================
# FINAL RUNTIME CHECK
# ============================================================

verify_pipeline_running


# ============================================================
# SUCCESS
# ============================================================

echo
echo "================================================"
echo " END-TO-END SMOKE TEST PASSED"
echo "================================================"
echo
echo "Smoke IMSI:"
echo "  ${SMOKE_IMSI}"
echo
echo "Pipeline:"
echo
echo "  Raw     ${EXPECTED_RAW_COUNT}/${EXPECTED_RAW_COUNT} PASS"
echo "  Bronze  ${EXPECTED_BRONZE_COUNT}/${EXPECTED_BRONZE_COUNT} PASS"
echo "  Silver  ${EXPECTED_SILVER_COUNT}/${EXPECTED_SILVER_COUNT} PASS"
echo "  Gold    ${EXPECTED_GOLD_COUNT}/${EXPECTED_GOLD_COUNT} PASS"
echo
echo "Model contract:"
echo
echo "  x_cat[32][4] PASS"
echo "  x_num[32][2] PASS"
echo
echo "Side outputs:"
echo
echo "  PASS"
echo