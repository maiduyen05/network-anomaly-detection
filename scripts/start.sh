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
# STEP 1 - START INFRASTRUCTURE
# ============================================================

echo
echo "=============================================="
echo " START KAFKA + FLINK"
echo "=============================================="
echo

docker compose up -d \
  kafka \
  flink-jobmanager \
  flink-taskmanager


# ============================================================
# STEP 2 - WAIT FOR FLINK JOBMANAGER
# ============================================================

FLINK_READY="false"

for attempt in $(seq 1 30); do

  if docker exec \
      a-flink-jobmanager \
      flink list >/dev/null 2>&1; then

    FLINK_READY="true"
    break
  fi

  echo "Đang chờ Flink JobManager: ${attempt}/30"

  sleep 2
done

if [[ "${FLINK_READY}" != "true" ]]; then
  echo "ERROR: Flink JobManager chưa sẵn sàng sau 60 giây." >&2
  exit 1
fi


# ============================================================
# STEP 3 - CREATE KAFKA TOPICS
# ============================================================

echo
echo "=============================================="
echo " CREATE / VERIFY KAFKA TOPICS"
echo "=============================================="
echo

"${SCRIPT_DIRECTORY}/create-topics.sh"


# ============================================================
# STEP 4 - BUILD FLINK JAR IF MISSING
# ============================================================
#
# Không tự build lại nếu JAR deploy đã tồn tại.
#
# Lý do:
# nếu Flink jobs đang RUNNING, thay JAR trên filesystem
# không làm running job tự cập nhật code.
#
# Khi thay đổi Java code, người vận hành phải chủ động chạy:
#
# ./scripts/build-flink-job.sh
#
# rồi thực hiện deployment có kiểm soát.
# ============================================================

DEPLOY_JAR="${PROJECT_DIRECTORY}/runtime/flink/usrlib/flink-preprocess-1.0.0-SNAPSHOT.jar"

if [[ ! -f "${DEPLOY_JAR}" ]]; then

  echo
  echo "Không tìm thấy deployed Flink JAR."
  echo "Tiến hành build..."
  echo

  "${SCRIPT_DIRECTORY}/build-flink-job.sh"

else

  echo
  echo "Flink JAR đã tồn tại:"
  echo "  ${DEPLOY_JAR}"
  echo
  echo "SKIP build."

fi


# ============================================================
# STEP 5 - SUBMIT MISSING JOBS
# ============================================================

echo
echo "=============================================="
echo " SUBMIT FLINK JOBS"
echo "=============================================="
echo

"${SCRIPT_DIRECTORY}/submit-flink-job.sh"


# ============================================================
# DONE
# ============================================================

echo
echo "=============================================="
echo " PIPELINE STARTED"
echo "=============================================="
echo

docker exec \
  a-flink-jobmanager \
  flink list -r