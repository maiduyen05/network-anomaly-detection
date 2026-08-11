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

FLINK_MODULE_DIRECTORY="${PROJECT_DIRECTORY}/flink-preprocess"

SOURCE_JAR="${FLINK_MODULE_DIRECTORY}/target/flink-preprocess-1.0.0-SNAPSHOT.jar"

DEPLOY_DIRECTORY="${PROJECT_DIRECTORY}/runtime/flink/usrlib"

DEPLOY_JAR="${DEPLOY_DIRECTORY}/flink-preprocess-1.0.0-SNAPSHOT.jar"

cd "${PROJECT_DIRECTORY}"


# ============================================================
# PRE-CHECK
# ============================================================

if ! command -v mvn >/dev/null 2>&1; then
  echo "ERROR: Maven chưa được cài đặt hoặc không có trong PATH." >&2
  exit 1
fi

if [[ ! -f "${FLINK_MODULE_DIRECTORY}/pom.xml" ]]; then
  echo "ERROR: Không tìm thấy flink-preprocess/pom.xml" >&2
  exit 1
fi


# ============================================================
# BUILD
# ============================================================

echo
echo "=============================================="
echo " BUILD FLINK PREPROCESS"
echo "=============================================="
echo

mvn \
  -f "${FLINK_MODULE_DIRECTORY}/pom.xml" \
  clean package


# ============================================================
# VERIFY BUILD OUTPUT
# ============================================================

if [[ ! -f "${SOURCE_JAR}" ]]; then
  echo "ERROR: Maven hoàn thành nhưng không tìm thấy JAR:" >&2
  echo "${SOURCE_JAR}" >&2
  exit 1
fi


# ============================================================
# DEPLOY JAR
# ============================================================
#
# runtime/flink/usrlib nằm ngoài target nên:
#
# mvn clean
#
# sẽ không xóa JAR triển khai.
#
# Docker Compose mount thư mục này vào:
#
# /opt/flink/usrlib
#
# của JobManager và TaskManager.
# ============================================================

mkdir -p "${DEPLOY_DIRECTORY}"

TEMP_JAR="${DEPLOY_JAR}.tmp"

cp "${SOURCE_JAR}" "${TEMP_JAR}"

mv "${TEMP_JAR}" "${DEPLOY_JAR}"


# ============================================================
# RESULT
# ============================================================

echo
echo "=============================================="
echo " BUILD SUCCESS"
echo "=============================================="
echo
echo "Source JAR:"
echo "  ${SOURCE_JAR}"
echo
echo "Deployed JAR:"
echo "  ${DEPLOY_JAR}"
echo