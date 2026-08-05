#!/usr/bin/env bash

# Dừng script ngay khi:
# - một command trả về mã lỗi;
# - sử dụng biến chưa được khai báo;
# - một command trong pipeline bị lỗi.
set -Eeuo pipefail

# ============================================================
# XÁC ĐỊNH ĐƯỜNG DẪN DỰ ÁN
# ============================================================

# Thư mục đang chứa create-topics.sh.
SCRIPT_DIRECTORY="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
  pwd
)"

# Thư mục gốc repository nằm phía trên thư mục scripts.
PROJECT_DIRECTORY="$(
  cd -- "${SCRIPT_DIRECTORY}/.."
  pwd
)"

# File cấu hình là nguồn duy nhất chứa danh sách topic.
TOPICS_CONFIG_FILE="${PROJECT_DIRECTORY}/config/kafka/topics.yaml"

# Đường dẫn Kafka CLI bên trong container Kafka.
KAFKA_BIN_DIRECTORY="/opt/kafka/bin"

# Script chạy lệnh bên trong container Kafka nên kết nối listener nội bộ
# của chính container qua localhost:29092.
KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:29092}"

# Chuyển working directory về repository root để Docker Compose
# luôn tìm đúng docker-compose.yml.
cd "${PROJECT_DIRECTORY}"

# ============================================================
# KIỂM TRA FILE CẤU HÌNH
# ============================================================

if [[ ! -f "${TOPICS_CONFIG_FILE}" ]]; then
  echo "Không tìm thấy file cấu hình: ${TOPICS_CONFIG_FILE}" >&2
  exit 1
fi

# ============================================================
# HÀM ĐỌC GIÁ TRỊ YAML ĐƠN GIẢN
# ============================================================
#
# File topics.yaml hiện chỉ sử dụng scalar đơn giản cho:
# - partitions;
# - replication_factor.
#
# Vì vậy checkpoint này chưa cần bổ sung dependency yq.

read_yaml_scalar() {
  local requested_key="$1"

  awk -v requested_key="${requested_key}" '
    {
      line = $0

      # Loại bỏ ký tự carriage return nếu file dùng CRLF.
      sub(/\r$/, "", line)

      pattern = "^[[:space:]]*" requested_key ":[[:space:]]*"

      if (line ~ pattern) {
        # Loại bỏ phần "key:" ở đầu dòng.
        sub(pattern, "", line)

        # Loại bỏ comment nằm sau giá trị.
        sub(/[[:space:]]*#.*/, "", line)

        # Loại bỏ khoảng trắng hai đầu.
        gsub(/^[[:space:]]+/, "", line)
        gsub(/[[:space:]]+$/, "", line)

        print line
        exit
      }
    }
  ' "${TOPICS_CONFIG_FILE}"
}

# Đọc cấu hình mặc định.
PARTITIONS="$(read_yaml_scalar "partitions")"
REPLICATION_FACTOR="$(read_yaml_scalar "replication_factor")"

if [[ -z "${PARTITIONS}" ]]; then
  echo "Không đọc được defaults.partitions" >&2
  exit 1
fi

if [[ -z "${REPLICATION_FACTOR}" ]]; then
  echo "Không đọc được defaults.replication_factor" >&2
  exit 1
fi

# ============================================================
# ĐỌC DANH SÁCH TOPIC
# ============================================================
#
# Chỉ lấy các dòng có đúng mức thụt lề:
#
# topics:
#   topic_key:
#     name: actual-topic-name

mapfile -t TOPICS < <(
  awk '
    {
      line = $0
      sub(/\r$/, "", line)

      if (line ~ /^    name:[[:space:]]*/) {
        sub(/^    name:[[:space:]]*/, "", line)
        sub(/[[:space:]]*#.*/, "", line)

        # Hỗ trợ cả giá trị có hoặc không có dấu nháy.
        gsub(/"/, "", line)
        gsub(/\047/, "", line)

        gsub(/^[[:space:]]+/, "", line)
        gsub(/[[:space:]]+$/, "", line)

        if (line != "") {
          print line
        }
      }
    }
  ' "${TOPICS_CONFIG_FILE}"
)

if [[ "${#TOPICS[@]}" -eq 0 ]]; then
  echo "Không tìm thấy Kafka topic trong ${TOPICS_CONFIG_FILE}" >&2
  exit 1
fi

# ============================================================
# CHỜ KAFKA SẴN SÀNG
# ============================================================

KAFKA_READY="false"

for attempt in $(seq 1 30); do
  if docker compose exec -T kafka \
      "${KAFKA_BIN_DIRECTORY}/kafka-topics.sh" \
      --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
      --list >/dev/null 2>&1; then

    KAFKA_READY="true"
    break
  fi

  echo "Đang chờ Kafka sẵn sàng: lần ${attempt}/30"
  sleep 2
done

if [[ "${KAFKA_READY}" != "true" ]]; then
  echo "Kafka chưa sẵn sàng sau 60 giây" >&2
  exit 1
fi

# ============================================================
# TẠO TOPIC
# ============================================================

for topic in "${TOPICS[@]}"; do
  echo "Đang bảo đảm topic tồn tại: ${topic}"

  docker compose exec -T kafka \
    "${KAFKA_BIN_DIRECTORY}/kafka-topics.sh" \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"
done

echo "Đã hoàn tất tạo ${#TOPICS[@]} Kafka topic."
echo "Partitions: ${PARTITIONS}"
echo "Replication factor: ${REPLICATION_FACTOR}"