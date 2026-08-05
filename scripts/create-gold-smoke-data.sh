#!/usr/bin/env bash

set -Eeuo pipefail

# ============================================================
# CẤU HÌNH SMOKE TEST
# ============================================================

SCRIPT_DIRECTORY="$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
  pwd
)"

PROJECT_DIRECTORY="$(
  cd -- "${SCRIPT_DIRECTORY}/.."
  pwd
)"

# Producer sẽ đọc riêng thư mục này.
OUTPUT_DIRECTORY="${PROJECT_DIRECTORY}/data/smoke-gold"

# File chứa 40 raw log line.
OUTPUT_FILE="${OUTPUT_DIRECTORY}/gold-smoke.log"

# IMSI riêng giúp dễ tìm sample smoke test trong Gold topic.
SMOKE_IMSI="452049999999999"

# Với sequence length 32 và stride 8:
#
# 40 event tạo ra:
# - sample 1: event 1..32;
# - sample 2: event 9..40.
EVENT_COUNT=40

# Bronze hiểu EVENT_TIME theo Asia/Ho_Chi_Minh.
#
# Lấy phút hiện tại giúp lần smoke test mới có timestamp lớn hơn
# lần test trước, tránh bị watermark coi là event quá trễ.
EVENT_MINUTE="$(
  TZ=Asia/Ho_Chi_Minh date '+%Y-%m-%d %H:%M'
)"

# DATE_HOUR có định dạng yyyyMMddHH.
DATE_HOUR="$(
  TZ=Asia/Ho_Chi_Minh date '+%Y%m%d%H'
)"

mkdir -p "${OUTPUT_DIRECTORY}"

# ============================================================
# TẠO 40 RAW LOG CÓ ĐÚNG 52 FIELD
# ============================================================

awk \
  -v event_count="${EVENT_COUNT}" \
  -v event_minute="${EVENT_MINUTE}" \
  -v date_hour="${DATE_HOUR}" \
  -v smoke_imsi="${SMOKE_IMSI}" '
  BEGIN {
    # Chín EVENT_ID đã được Silver và Gold hỗ trợ.
    supported_event[0] = "l_service_request"
    supported_event[1] = "l_tau"
    supported_event[2] = "l_handover"
    supported_event[3] = "l_attach"
    supported_event[4] = "l_pdn_connect"
    supported_event[5] = "l_bearer_modify"
    supported_event[6] = "l_dedicated_bearer_activate"
    supported_event[7] = "l_dedicated_bearer_deactivate"
    supported_event[8] = "l_detach"

    for (row_index = 0;
         row_index < event_count;
         row_index++) {

      # Mỗi vòng lặp tạo lại đủ 52 field rỗng.
      for (field_index = 1;
           field_index <= 52;
           field_index++) {
        field[field_index] = ""
      }

      # ------------------------------------------------------
      # Các vị trí dưới đây là 1-based theo raw log contract.
      # ------------------------------------------------------

      # 1: EVENT_ID.
      field[1] = supported_event[row_index % 9]

      # 2: EVENT_RESULT.
      field[2] = "success"

      # 3: DURATION.
      field[3] = 1000 + row_index

      # 4: REQUEST_RETRIES.
      field[4] = row_index % 3

      # 5: SUB_TYPE.
      field[5] = ""

      # 6: MSISDN.
      field[6] = "84999999999"

      # 7: IMSI.
      field[7] = smoke_imsi

      # 8: MTMSI.
      field[8] = sprintf("SMOKE%04d", row_index)

      # 9: IMEISV.
      field[9] = "3599999999999999"

      # 10..13: thông tin location.
      field[10] = "100"
      field[11] = "10"
      field[12] = "200"
      field[13] = "300"

      # 16: L_CAUSE_PROT_TYPE.
      # 17: CAUSE_CODE.
      # 18: SUB_CAUSE_CODE.
      #
      # Giữ rỗng vì chuỗi rỗng là category hợp lệ trong contract.
      field[16] = ""
      field[17] = ""
      field[18] = ""

      # 49: EVENT_TIME.
      #
      # Các event có timestamp tăng dần:
      # HH:mm:00, HH:mm:01, ..., HH:mm:39.
      field[49] = sprintf("%s:%02d", event_minute, row_index)

      # 50: PAGING_ATTEMPTS.
      field[50] = "0"

      # 52: DATE_HOUR.
      field[52] = date_hour

      # Ghi field đầu tiên.
      printf "%s", field[1]

      # Ghi 51 field còn lại, mỗi field có một dấu ";" phía trước.
      for (field_index = 2;
           field_index <= 52;
           field_index++) {
        printf ";%s", field[field_index]
      }

      printf "\n"
    }
  }
' > "${OUTPUT_FILE}"

# ============================================================
# KIỂM TRA FILE VỪA TẠO
# ============================================================

ACTUAL_LINE_COUNT="$(
  wc -l < "${OUTPUT_FILE}"
)"

INVALID_FIELD_COUNT_LINES="$(
  awk -F';' '
    NF != 52 {
      invalid_count++
    }

    END {
      print invalid_count + 0
    }
  ' "${OUTPUT_FILE}"
)"

if [[ "${ACTUAL_LINE_COUNT}" -ne "${EVENT_COUNT}" ]]; then
  echo "Sai số dòng: ${ACTUAL_LINE_COUNT}" >&2
  exit 1
fi

if [[ "${INVALID_FIELD_COUNT_LINES}" -ne 0 ]]; then
  echo "Có dòng không đủ 52 field" >&2
  exit 1
fi

echo "Đã tạo smoke data thành công."
echo "File: ${OUTPUT_FILE}"
echo "Số dòng: ${ACTUAL_LINE_COUNT}"
echo "IMSI: ${SMOKE_IMSI}"
echo "Event minute: ${EVENT_MINUTE}"