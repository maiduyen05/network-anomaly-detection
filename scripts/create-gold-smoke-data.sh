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


# ============================================================
# OUTPUT
# ============================================================

OUTPUT_DIRECTORY="${PROJECT_DIRECTORY}/data/smoke-gold"
OUTPUT_FILE="${OUTPUT_DIRECTORY}/gold-smoke.log"

mkdir -p "${OUTPUT_DIRECTORY}"


# ============================================================
# UNIQUE IMSI
# ============================================================
#
# Mỗi lần smoke test dùng một IMSI mới.
#
# 45204 + 10 số = IMSI 15 chữ số.
# ============================================================

SMOKE_SUFFIX="$(
  date +%s%N |
    tail -c 11
)"

SMOKE_IMSI="45204${SMOKE_SUFFIX}"


# ============================================================
# EVENT PLAN
# ============================================================
#
# 40 target events:
#
#   event 1..32  -> Gold window 1
#   event 9..40  -> Gold window 2
#
# Event 41 nằm ở t + 70 giây để đẩy watermark.
# ============================================================

TARGET_EVENT_COUNT=40
FLUSH_OFFSET_SECONDS=70
TOTAL_EVENT_COUNT=$((TARGET_EVENT_COUNT + 1))


# ============================================================
# EVENT TIME
# ============================================================

BASE_MINUTE="$(
  TZ=Asia/Ho_Chi_Minh \
    date '+%Y-%m-%d %H:%M'
)"

BASE_EPOCH="$(
  TZ=Asia/Ho_Chi_Minh \
    date \
      -d "${BASE_MINUTE}:00" \
      '+%s'
)"

BASE_DATE_HOUR="$(
  TZ=Asia/Ho_Chi_Minh \
    date \
      -d "@${BASE_EPOCH}" \
      '+%Y%m%d%H'
)"

FLUSH_EPOCH=$((BASE_EPOCH + FLUSH_OFFSET_SECONDS))

FLUSH_EVENT_TIME="$(
  TZ=Asia/Ho_Chi_Minh \
    date \
      -d "@${FLUSH_EPOCH}" \
      '+%Y-%m-%d %H:%M:%S'
)"

FLUSH_DATE_HOUR="$(
  TZ=Asia/Ho_Chi_Minh \
    date \
      -d "@${FLUSH_EPOCH}" \
      '+%Y%m%d%H'
)"


# ============================================================
# SUPPORTED EVENT IDs
# ============================================================

SUPPORTED_EVENTS=(
  "l_service_request"
  "l_tau"
  "l_handover"
  "l_attach"
  "l_pdn_connect"
  "l_bearer_modify"
  "l_dedicated_bearer_activate"
  "l_dedicated_bearer_deactivate"
  "l_detach"
)


# ============================================================
# CREATE EMPTY OUTPUT FILE
# ============================================================

: > "${OUTPUT_FILE}"


# ============================================================
# GENERATE 41 RAW RECORDS
# ============================================================
#
# Bash array sử dụng index 0-based:
#
# fields[0]  = raw field 1
# fields[48] = raw field 49
# fields[51] = raw field 52
# ============================================================

for ((row_index = 0; row_index < TOTAL_EVENT_COUNT; row_index++)); do

  # ----------------------------------------------------------
  # Khởi tạo đúng 52 field rỗng
  # ----------------------------------------------------------

  fields=()

  for ((field_index = 0; field_index < 52; field_index++)); do
    fields[field_index]=""
  done


  # ----------------------------------------------------------
  # FIELD 1: EVENT_ID
  # ----------------------------------------------------------

  fields[0]="${SUPPORTED_EVENTS[$((row_index % 9))]}"


  # ----------------------------------------------------------
  # FIELD 2: EVENT_RESULT
  # ----------------------------------------------------------

  fields[1]="success"


  # ----------------------------------------------------------
  # FIELD 3: DURATION
  # ----------------------------------------------------------

  fields[2]="$((1000 + row_index))"


  # ----------------------------------------------------------
  # FIELD 4: REQUEST_RETRIES
  # ----------------------------------------------------------

  fields[3]="$((row_index % 3))"


  # ----------------------------------------------------------
  # FIELD 5: SUB_TYPE
  # ----------------------------------------------------------

  fields[4]=""


  # ----------------------------------------------------------
  # FIELD 6: MSISDN
  # ----------------------------------------------------------

  fields[5]="84999999999"


  # ----------------------------------------------------------
  # FIELD 7: IMSI
  # ----------------------------------------------------------

  fields[6]="${SMOKE_IMSI}"


  # ----------------------------------------------------------
  # FIELD 8: MTMSI
  # ----------------------------------------------------------

  printf -v fields[7] \
    'SMOKE%04d' \
    "${row_index}"


  # ----------------------------------------------------------
  # FIELD 9: IMEISV
  # ----------------------------------------------------------

  fields[8]="3599999999999999"


  # ----------------------------------------------------------
  # NETWORK / LOCATION
  # ----------------------------------------------------------

  fields[9]="100"
  fields[10]="10"
  fields[11]="200"
  fields[12]="300"


  # ----------------------------------------------------------
  # FIELD 16, 17, 18
  #
  # cause / sub-cause giữ rỗng.
  # Empty string là category hợp lệ trong feature contract.
  # ----------------------------------------------------------

  fields[15]=""
  fields[16]=""
  fields[17]=""


  # ----------------------------------------------------------
  # FIELD 49: EVENT_TIME
  # FIELD 52: DATE_HOUR
  # ----------------------------------------------------------

  if ((row_index < TARGET_EVENT_COUNT)); then

    printf -v fields[48] \
      '%s:%02d' \
      "${BASE_MINUTE}" \
      "${row_index}"

    fields[51]="${BASE_DATE_HOUR}"

  else

    fields[48]="${FLUSH_EVENT_TIME}"
    fields[51]="${FLUSH_DATE_HOUR}"

  fi


  # ----------------------------------------------------------
  # FIELD 50: PAGING_ATTEMPTS
  # ----------------------------------------------------------

  fields[49]="0"


  # ==========================================================
  # WRITE EXACTLY 52 FIELDS
  # ==========================================================

  {
    printf '%s' "${fields[0]}"

    for ((field_index = 1; field_index < 52; field_index++)); do
      printf ';%s' "${fields[field_index]}"
    done

    printf '\n'

  } >> "${OUTPUT_FILE}"

done


# ============================================================
# VALIDATION
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


if [[ "${ACTUAL_LINE_COUNT}" -ne "${TOTAL_EVENT_COUNT}" ]]; then

  echo
  echo "ERROR: Sai số lượng raw record." >&2
  echo "Expected: ${TOTAL_EVENT_COUNT}" >&2
  echo "Actual:   ${ACTUAL_LINE_COUNT}" >&2

  exit 1
fi


if [[ "${INVALID_FIELD_COUNT_LINES}" -ne 0 ]]; then

  echo
  echo "ERROR: Có raw record không đủ đúng 52 field." >&2
  echo "Invalid rows: ${INVALID_FIELD_COUNT_LINES}" >&2

  exit 1
fi


# ============================================================
# RESULT
# ============================================================

echo
echo "================================================"
echo " GOLD SMOKE DATA CREATED"
echo "================================================"
echo
echo "File:"
echo "  ${OUTPUT_FILE}"
echo
echo "Unique IMSI:"
echo "  ${SMOKE_IMSI}"
echo
echo "Target events:"
echo "  ${TARGET_EVENT_COUNT}"
echo
echo "Flush events:"
echo "  1"
echo
echo "Total raw events:"
echo "  ${TOTAL_EVENT_COUNT}"
echo
echo "Target event time:"
echo "  ${BASE_MINUTE}:00 .. ${BASE_MINUTE}:39"
echo
echo "Flush event time:"
echo "  ${FLUSH_EVENT_TIME}"
echo
echo "Expected isolated Gold windows:"
echo "  2"
echo