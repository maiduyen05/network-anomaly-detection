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

SAVEPOINT_DIRECTORY_CONTAINER="/opt/flink/runtime/savepoints"

SAVEPOINT_DIRECTORY_URI="file://${SAVEPOINT_DIRECTORY_CONTAINER}"

SAVEPOINT_DIRECTORY_HOST="${PROJECT_DIRECTORY}/runtime/flink/savepoints"

# File manifest do shell trên HOST quản lý.
# Không đặt bên trong thư mục savepoints vì thư mục đó
# được Flink container sử dụng và có thể mang UID/GID
# khác với user trên host.
RESTORE_MANIFEST="${PROJECT_DIRECTORY}/runtime/flink/restore-manifest.env"

RESTORE_MANIFEST_TMP="${RESTORE_MANIFEST}.tmp"


# ============================================================
# JOB NAMES
# ============================================================

BRONZE_JOB_NAME="flink-bronze-v1"
SILVER_JOB_NAME="flink-silver-v1"
GOLD_JOB_NAME="flink-gold-v1"


# ============================================================
# PRE-CHECK
# ============================================================

if ! docker inspect \
    "${JOBMANAGER_CONTAINER}" \
    >/dev/null 2>&1; then

  echo
  echo "Flink JobManager không tồn tại."
  echo "Không có Flink job để stop."
  echo

  docker compose stop
  exit 0
fi


if [[ "$(
  docker inspect \
    -f '{{.State.Running}}' \
    "${JOBMANAGER_CONTAINER}"
)" != "true" ]]; then

  echo
  echo "Flink JobManager hiện không RUNNING."
  echo

  docker compose stop
  exit 0
fi


mkdir -p "${SAVEPOINT_DIRECTORY_HOST}"


# ============================================================
# FLINK JOB HELPERS
# ============================================================

list_running_jobs() {

  docker exec \
    "${JOBMANAGER_CONTAINER}" \
    flink list -r 2>/dev/null
}


find_job_id() {

  local job_name="$1"

  list_running_jobs |
    awk \
      -F ' : ' \
      -v job_name="${job_name}" \
      '
        index($3, job_name) == 1 &&
        index($3, "(RUNNING)") > 0 {
          print $2
        }
      '
}


require_single_job() {

  local job_name="$1"

  mapfile -t ids < <(
    find_job_id "${job_name}"
  )

  if [[ "${#ids[@]}" -eq 0 ]]; then

    echo "ERROR: Không tìm thấy ${job_name} đang RUNNING." >&2
    return 1

  fi

  if [[ "${#ids[@]}" -gt 1 ]]; then

    echo "ERROR: Có nhiều hơn một ${job_name} đang RUNNING." >&2

    printf '  %s\n' "${ids[@]}" >&2

    return 1
  fi

  printf '%s\n' "${ids[0]}"
}


# ============================================================
# RESOLVE CURRENT JOB IDS
# ============================================================

echo
echo "================================================"
echo " CURRENT FLINK JOBS"
echo "================================================"
echo

list_running_jobs

echo


BRONZE_JOB_ID="$(
  require_single_job "${BRONZE_JOB_NAME}"
)"

SILVER_JOB_ID="$(
  require_single_job "${SILVER_JOB_NAME}"
)"

GOLD_JOB_ID="$(
  require_single_job "${GOLD_JOB_NAME}"
)"


echo "Bronze Job ID: ${BRONZE_JOB_ID}"
echo "Silver Job ID: ${SILVER_JOB_ID}"
echo "Gold Job ID:   ${GOLD_JOB_ID}"


# ============================================================
# STOP JOB WITH SAVEPOINT
# ============================================================
#
# Không dùng flink cancel.
#
# flink stop --savepointPath:
#
# 1. tạo savepoint nhất quán;
# 2. hoàn thành savepoint;
# 3. stop job.
#
# Path trả về sẽ được lưu để start.sh /
# submit-flink-job.sh có thể restore.
# ============================================================

stop_with_savepoint() {

  local job_name="$1"
  local job_id="$2"

  echo
  echo "================================================"
  echo " STOP WITH SAVEPOINT: ${job_name}"
  echo "================================================"
  echo
  echo "Job ID:"
  echo "  ${job_id}"
  echo

  local command_output

  command_output="$(
    docker exec \
      "${JOBMANAGER_CONTAINER}" \
      flink stop \
      --savepointPath "${SAVEPOINT_DIRECTORY_URI}" \
      "${job_id}" \
      2>&1 |
      tee /dev/stderr
  )"


  # ----------------------------------------------------------
  # Extract savepoint path.
  #
  # Expected:
  #
  # file:/opt/flink/runtime/savepoints/savepoint-xxxx-xxxx
  # ----------------------------------------------------------

  local savepoint_path

  savepoint_path="$(
    printf '%s\n' "${command_output}" |
      grep -Eo \
        'file:/opt/flink/runtime/savepoints/savepoint-[^[:space:]]+' |
      tail -n 1
  )"


  if [[ -z "${savepoint_path}" ]]; then

    echo
    echo "ERROR: Không đọc được savepoint path của ${job_name}." >&2

    return 1
  fi


  # ----------------------------------------------------------
  # Verify _metadata exists.
  # ----------------------------------------------------------

  local container_path

  container_path="${savepoint_path#file:}"

  if ! docker exec \
      "${JOBMANAGER_CONTAINER}" \
      test -f "${container_path}/_metadata"; then

    echo
    echo "ERROR: Savepoint không có _metadata:" >&2
    echo "  ${savepoint_path}" >&2

    return 1
  fi


  echo
  echo "SAVEPOINT OK:"
  echo "  ${savepoint_path}"
  echo


  printf '%s\n' "${savepoint_path}"
}


# ============================================================
# STOP ORDER
# ============================================================
#
# Stop upstream -> downstream:
#
# Bronze
#   ↓
# Silver
#   ↓
# Gold
#
# Sau khi Bronze dừng sẽ không có raw record mới đi tiếp.
#
# Kafka vẫn giữ intermediate messages chưa consume,
# vì vậy Silver/Gold có thể tiếp tục từ state + Kafka offsets
# sau khi restore.
# ============================================================

BRONZE_SAVEPOINT="$(
  stop_with_savepoint \
    "${BRONZE_JOB_NAME}" \
    "${BRONZE_JOB_ID}" |
    tail -n 1
)"

SILVER_SAVEPOINT="$(
  stop_with_savepoint \
    "${SILVER_JOB_NAME}" \
    "${SILVER_JOB_ID}" |
    tail -n 1
)"

GOLD_SAVEPOINT="$(
  stop_with_savepoint \
    "${GOLD_JOB_NAME}" \
    "${GOLD_JOB_ID}" |
    tail -n 1
)"


# ============================================================
# WRITE RESTORE MANIFEST
# ============================================================
#
# Chỉ tạo manifest cuối cùng nếu CẢ BA savepoint đều thành công.
#
# File này nằm trong runtime/flink/savepoints và bị Git ignore.
# ============================================================

cat > "${RESTORE_MANIFEST_TMP}" <<EOF
RESTORE_READY=true
BRONZE_SAVEPOINT=${BRONZE_SAVEPOINT}
SILVER_SAVEPOINT=${SILVER_SAVEPOINT}
GOLD_SAVEPOINT=${GOLD_SAVEPOINT}
EOF

mv \
  "${RESTORE_MANIFEST_TMP}" \
  "${RESTORE_MANIFEST}"


echo
echo "================================================"
echo " RESTORE MANIFEST CREATED"
echo "================================================"
echo
echo "${RESTORE_MANIFEST}"
echo

cat "${RESTORE_MANIFEST}"


# ============================================================
# VERIFY NO FLINK JOB IS STILL RUNNING
# ============================================================

echo
echo "================================================"
echo " VERIFY FLINK JOBS STOPPED"
echo "================================================"
echo

REMAINING_JOBS="$(
  list_running_jobs || true
)"

if printf '%s\n' "${REMAINING_JOBS}" |
    grep -q '(RUNNING)'; then

  echo "ERROR: Vẫn còn Flink job RUNNING:" >&2
  echo >&2
  echo "${REMAINING_JOBS}" >&2

  exit 1
fi

echo "Không còn Bronze/Silver/Gold RUNNING."


# ============================================================
# STOP CONTAINERS
# ============================================================

echo
echo "================================================"
echo " STOP DOCKER SERVICES"
echo "================================================"
echo

docker compose stop


echo
echo "================================================"
echo " STATE-SAFE STOP COMPLETED"
echo "================================================"
echo
echo "Savepoint manifest:"
echo "  ${RESTORE_MANIFEST}"
echo
echo "Lần start tiếp theo phải restore từ các savepoint này."