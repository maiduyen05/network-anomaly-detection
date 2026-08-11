#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# PROJECT
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
# FLINK
# ============================================================

JOBMANAGER_CONTAINER="a-flink-jobmanager"

FLINK_JAR="/opt/flink/usrlib/flink-preprocess-1.0.0-SNAPSHOT.jar"


# ============================================================
# JOB DEFINITIONS
# ============================================================
#
# Submit downstream -> upstream:
#
# Gold
#   ↑
# Silver
#   ↑
# Bronze
#
# Như vậy downstream đã sẵn sàng trước khi upstream bắt đầu
# đẩy dữ liệu.
# ============================================================

GOLD_JOB_NAME="flink-gold-v1"
GOLD_JOB_CLASS="com.network.preprocess.gold.GoldJob"

SILVER_JOB_NAME="flink-silver-v1"
SILVER_JOB_CLASS="com.network.preprocess.silver.SilverJob"

BRONZE_JOB_NAME="flink-bronze-v1"
BRONZE_JOB_CLASS="com.network.preprocess.bronze.BronzeJob"


# ============================================================
# CHECK JOBMANAGER
# ============================================================

if ! docker inspect "${JOBMANAGER_CONTAINER}" >/dev/null 2>&1; then
  echo "ERROR: Không tìm thấy container ${JOBMANAGER_CONTAINER}" >&2
  exit 1
fi

if [[ "$(
  docker inspect \
    -f '{{.State.Running}}' \
    "${JOBMANAGER_CONTAINER}"
)" != "true" ]]; then

  echo "ERROR: ${JOBMANAGER_CONTAINER} chưa RUNNING." >&2
  exit 1
fi


# ============================================================
# CHECK JAR
# ============================================================

if ! docker exec \
    "${JOBMANAGER_CONTAINER}" \
    test -f "${FLINK_JAR}"; then

  echo "ERROR: Không tìm thấy Flink JAR:" >&2
  echo "  ${FLINK_JAR}" >&2
  echo
  echo "Hãy chạy:"
  echo "  ./scripts/build-flink-job.sh"
  exit 1
fi


# ============================================================
# FUNCTIONS
# ============================================================

list_running_jobs() {
  docker exec \
    "${JOBMANAGER_CONTAINER}" \
    flink list -r 2>/dev/null
}


count_running_job() {

  local job_name="$1"

  list_running_jobs |
    awk \
      -F ' : ' \
      -v expected="${job_name} (RUNNING)" \
      '
        $3 == expected {
          count++
        }

        END {
          print count + 0
        }
      '
}


wait_until_running() {

  local job_name="$1"

  for attempt in $(seq 1 30); do

    local count

    count="$(count_running_job "${job_name}")"

    if [[ "${count}" -eq 1 ]]; then
      echo "RUNNING: ${job_name}"
      return 0
    fi

    if [[ "${count}" -gt 1 ]]; then
      echo "ERROR: Có ${count} instance của ${job_name} đang RUNNING." >&2
      return 1
    fi

    echo "Đang chờ ${job_name} RUNNING: ${attempt}/30"

    sleep 2
  done

  echo "ERROR: ${job_name} không chuyển sang RUNNING sau 60 giây." >&2

  return 1
}


submit_if_missing() {

  local job_name="$1"
  local job_class="$2"

  local count

  count="$(count_running_job "${job_name}")"


  # ----------------------------------------------------------
  # Đã có đúng một job.
  # ----------------------------------------------------------

  if [[ "${count}" -eq 1 ]]; then

    echo
    echo "SKIP: ${job_name} đã RUNNING."

    return 0
  fi


  # ----------------------------------------------------------
  # Có nhiều hơn một job.
  #
  # Không tự cancel vì đây là stateful streaming job.
  # Cần người vận hành kiểm tra trước.
  # ----------------------------------------------------------

  if [[ "${count}" -gt 1 ]]; then

    echo
    echo "ERROR: Phát hiện ${count} job '${job_name}' đang RUNNING." >&2
    echo "Script sẽ KHÔNG tự động cancel job." >&2

    exit 1
  fi


  # ----------------------------------------------------------
  # Chưa có job -> submit.
  # ----------------------------------------------------------

  echo
  echo "=============================================="
  echo " SUBMIT ${job_name}"
  echo "=============================================="

  docker exec \
    "${JOBMANAGER_CONTAINER}" \
    flink run \
    -d \
    -c "${job_class}" \
    "${FLINK_JAR}"

  wait_until_running "${job_name}"
}


# ============================================================
# SUBMIT DOWNSTREAM -> UPSTREAM
# ============================================================

submit_if_missing \
  "${GOLD_JOB_NAME}" \
  "${GOLD_JOB_CLASS}"

submit_if_missing \
  "${SILVER_JOB_NAME}" \
  "${SILVER_JOB_CLASS}"

submit_if_missing \
  "${BRONZE_JOB_NAME}" \
  "${BRONZE_JOB_CLASS}"


# ============================================================
# FINAL CHECK
# ============================================================

echo
echo "=============================================="
echo " RUNNING FLINK JOBS"
echo "=============================================="
echo

list_running_jobs