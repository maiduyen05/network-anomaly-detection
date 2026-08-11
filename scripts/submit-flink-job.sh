#!/usr/bin/env bash

set -Eeuo pipefail


# ============================================================
# PROJECT PATH
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
# CONFIGURATION
# ============================================================

JOBMANAGER_CONTAINER="a-flink-jobmanager"

FLINK_JAR="/opt/flink/usrlib/flink-preprocess-1.0.0-SNAPSHOT.jar"

RESTORE_MANIFEST="${PROJECT_DIRECTORY}/runtime/flink/restore-manifest.env"

# ============================================================
# JOB DEFINITIONS
# ============================================================

GOLD_JOB_NAME="flink-gold-v1"
GOLD_JOB_CLASS="com.network.preprocess.gold.GoldJob"

SILVER_JOB_NAME="flink-silver-v1"
SILVER_JOB_CLASS="com.network.preprocess.silver.SilverJob"

BRONZE_JOB_NAME="flink-bronze-v1"
BRONZE_JOB_CLASS="com.network.preprocess.bronze.BronzeJob"


# ============================================================
# PRE-CHECK
# ============================================================

if ! docker inspect \
    "${JOBMANAGER_CONTAINER}" \
    >/dev/null 2>&1; then

  echo "ERROR: Không tìm thấy ${JOBMANAGER_CONTAINER}" >&2
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


if ! docker exec \
    "${JOBMANAGER_CONTAINER}" \
    test -f "${FLINK_JAR}"; then

  echo "ERROR: Không tìm thấy JAR:" >&2
  echo "  ${FLINK_JAR}" >&2

  exit 1
fi


# ============================================================
# HELPERS
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
      -v job_name="${job_name}" \
      '
        index($3, job_name) == 1 &&
        index($3, "(RUNNING)") > 0 {
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

    count="$(
      count_running_job "${job_name}"
    )"

    if [[ "${count}" -eq 1 ]]; then

      echo "RUNNING: ${job_name}"
      return 0
    fi


    if [[ "${count}" -gt 1 ]]; then

      echo "ERROR: ${count} instance của ${job_name} đang RUNNING." >&2
      return 1
    fi


    echo "Đang chờ ${job_name}: ${attempt}/30"

    sleep 2
  done


  echo "ERROR: ${job_name} không RUNNING sau 60 giây." >&2

  return 1
}


verify_savepoint() {

  local savepoint_path="$1"

  if [[ -z "${savepoint_path}" ]]; then

    echo "ERROR: Savepoint path rỗng." >&2
    return 1
  fi


  local container_path

  container_path="${savepoint_path#file:}"


  if ! docker exec \
      "${JOBMANAGER_CONTAINER}" \
      test -f "${container_path}/_metadata"; then

    echo "ERROR: Không tìm thấy savepoint _metadata:" >&2
    echo "  ${savepoint_path}" >&2

    return 1
  fi
}


submit_fresh() {

  local job_name="$1"
  local job_class="$2"

  echo
  echo "================================================"
  echo " FRESH SUBMIT: ${job_name}"
  echo "================================================"
  echo

  docker exec \
    "${JOBMANAGER_CONTAINER}" \
    flink run \
    -d \
    -c "${job_class}" \
    "${FLINK_JAR}"

  wait_until_running "${job_name}"
}


restore_job() {

  local job_name="$1"
  local job_class="$2"
  local savepoint_path="$3"

  verify_savepoint "${savepoint_path}"

  echo
  echo "================================================"
  echo " RESTORE: ${job_name}"
  echo "================================================"
  echo
  echo "Savepoint:"
  echo "  ${savepoint_path}"
  echo


  docker exec \
    "${JOBMANAGER_CONTAINER}" \
    flink run \
    -d \
    -s "${savepoint_path}" \
    -c "${job_class}" \
    "${FLINK_JAR}"


  wait_until_running "${job_name}"
}


# ============================================================
# CHECK CURRENT TOPOLOGY
# ============================================================

GOLD_COUNT="$(
  count_running_job "${GOLD_JOB_NAME}"
)"

SILVER_COUNT="$(
  count_running_job "${SILVER_JOB_NAME}"
)"

BRONZE_COUNT="$(
  count_running_job "${BRONZE_JOB_NAME}"
)"


# ============================================================
# DUPLICATE PROTECTION
# ============================================================

if [[ "${GOLD_COUNT}" -gt 1 ]] ||
   [[ "${SILVER_COUNT}" -gt 1 ]] ||
   [[ "${BRONZE_COUNT}" -gt 1 ]]; then

  echo "ERROR: Phát hiện duplicate Flink job." >&2

  list_running_jobs >&2

  exit 1
fi


# ============================================================
# ALL THREE ALREADY RUNNING
# ============================================================

if [[ "${GOLD_COUNT}" -eq 1 ]] &&
   [[ "${SILVER_COUNT}" -eq 1 ]] &&
   [[ "${BRONZE_COUNT}" -eq 1 ]]; then

  echo
  echo "Bronze / Silver / Gold đều đã RUNNING."
  echo "Không submit thêm job."
  echo

  list_running_jobs

  exit 0
fi


# ============================================================
# PARTIAL TOPOLOGY PROTECTION
# ============================================================
#
# Ví dụ:
#
# Gold   RUNNING
# Silver RUNNING
# Bronze missing
#
# Không tự submit/restore Bronze.
#
# Đây có thể là crash/recovery case.
# Tự động dùng savepoint cũ ở đây có thể rollback state.
# ============================================================

TOTAL_RUNNING="$(
  (
    echo "${GOLD_COUNT}"
    echo "${SILVER_COUNT}"
    echo "${BRONZE_COUNT}"
  ) |
    awk '{ total += $1 } END { print total }'
)"


if [[ "${TOTAL_RUNNING}" -gt 0 ]] &&
   [[ "${TOTAL_RUNNING}" -lt 3 ]]; then

  echo
  echo "ERROR: Flink topology đang ở trạng thái PARTIAL." >&2
  echo >&2
  echo "Gold:   ${GOLD_COUNT}" >&2
  echo "Silver: ${SILVER_COUNT}" >&2
  echo "Bronze: ${BRONZE_COUNT}" >&2
  echo >&2
  echo "Script sẽ không tự động sửa topology này." >&2
  echo >&2

  list_running_jobs >&2

  exit 1
fi


# ============================================================
# ZERO JOBS RUNNING
# ============================================================
#
# Có hai trường hợp:
#
# A. restore-manifest tồn tại
#    -> restore state-safe
#
# B. không có manifest
#    -> deployment mới/fresh
# ============================================================


# ============================================================
# RESTORE MODE
# ============================================================

if [[ -f "${RESTORE_MANIFEST}" ]]; then

  echo
  echo "================================================"
  echo " RESTORE MANIFEST FOUND"
  echo "================================================"
  echo

  cat "${RESTORE_MANIFEST}"

  echo


  # shellcheck disable=SC1090
  source "${RESTORE_MANIFEST}"


  if [[ "${RESTORE_READY:-false}" != "true" ]]; then

    echo "ERROR: restore manifest không ở trạng thái READY." >&2

    exit 1
  fi


  verify_savepoint "${GOLD_SAVEPOINT:-}"
  verify_savepoint "${SILVER_SAVEPOINT:-}"
  verify_savepoint "${BRONZE_SAVEPOINT:-}"


  # ----------------------------------------------------------
  # Restore downstream -> upstream.
  #
  # Gold trước.
  # Silver sau.
  # Bronze cuối.
  #
  # Khi producer upstream resume thì downstream consumer
  # đã sẵn sàng.
  # ----------------------------------------------------------

  restore_job \
    "${GOLD_JOB_NAME}" \
    "${GOLD_JOB_CLASS}" \
    "${GOLD_SAVEPOINT}"


  restore_job \
    "${SILVER_JOB_NAME}" \
    "${SILVER_JOB_CLASS}" \
    "${SILVER_SAVEPOINT}"


  restore_job \
    "${BRONZE_JOB_NAME}" \
    "${BRONZE_JOB_CLASS}" \
    "${BRONZE_SAVEPOINT}"


  # ----------------------------------------------------------
  # Sau khi cả ba restore thành công,
  # manifest không còn được dùng cho lần start tiếp theo.
  # ----------------------------------------------------------

  USED_MANIFEST="$(
    printf '%s.used.%s' \
      "${RESTORE_MANIFEST}" \
      "$(date '+%Y%m%d-%H%M%S')"
  )"

  mv \
    "${RESTORE_MANIFEST}" \
    "${USED_MANIFEST}"


  echo
  echo "================================================"
  echo " RESTORE COMPLETED"
  echo "================================================"
  echo
  echo "Manifest đã archive:"
  echo "  ${USED_MANIFEST}"
  echo


# ============================================================
# FRESH MODE
# ============================================================

else

  echo
  echo "================================================"
  echo " NO RESTORE MANIFEST"
  echo " FRESH SUBMISSION"
  echo "================================================"
  echo


  submit_fresh \
    "${GOLD_JOB_NAME}" \
    "${GOLD_JOB_CLASS}"


  submit_fresh \
    "${SILVER_JOB_NAME}" \
    "${SILVER_JOB_CLASS}"


  submit_fresh \
    "${BRONZE_JOB_NAME}" \
    "${BRONZE_JOB_CLASS}"

fi


# ============================================================
# FINAL CHECK
# ============================================================

echo
echo "================================================"
echo " FINAL FLINK TOPOLOGY"
echo "================================================"
echo

list_running_jobs