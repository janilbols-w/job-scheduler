#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if command -v python >/dev/null 2>&1; then
  DEFAULT_PYTHON_BIN="python"
elif command -v python3 >/dev/null 2>&1; then
  DEFAULT_PYTHON_BIN="python3"
else
  echo "[quickstart] error: neither 'python' nor 'python3' is available"
  exit 1
fi

PYTHON_BIN="${QUICKSTART_PYTHON_BIN:-${DEFAULT_PYTHON_BIN}}"

SCHEDULER_HOST="${SCHEDULER_HOST:-127.0.0.1}"
SCHEDULER_PORT="${SCHEDULER_PORT:-8000}"
SCHEDULER_URL="${SCHEDULER_URL:-http://${SCHEDULER_HOST}:${SCHEDULER_PORT}}"

MOCK_MEAN_WAKEUP_SECONDS="${MOCK_MEAN_WAKEUP_SECONDS:-8.0}"
MOCK_ACTION_SUCCESS_RATE="${MOCK_ACTION_SUCCESS_RATE:-0.8}"
SIMULATION_DURATION="${SIMULATION_DURATION:-60}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SIMULATION_OUTPUT_DIR="${SIMULATION_OUTPUT_DIR:-${ROOT_DIR}/outputs/simulation_${TIMESTAMP}}"
DISABLE_CORE_TRACE="${DISABLE_CORE_TRACE:-0}"
QUICKSTART_CONFIG="${QUICKSTART_CONFIG:-}"

EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      if [[ $# -lt 2 ]]; then
        echo "[quickstart] error: --config requires a value"
        exit 1
      fi
      QUICKSTART_CONFIG="$2"
      shift 2
      ;;
    --config=*)
      QUICKSTART_CONFIG="${1#*=}"
      shift
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

cmd=(
  "${PYTHON_BIN}" "${ROOT_DIR}/examples/quickstart/simulation.py"
  --scheduler-host "${SCHEDULER_HOST}"
  --scheduler-port "${SCHEDULER_PORT}"
  --scheduler-url "${SCHEDULER_URL}"
  --simulation-duration "${SIMULATION_DURATION}"
  --mock-mean-wakeup-seconds "${MOCK_MEAN_WAKEUP_SECONDS}"
  --mock-action-success-rate "${MOCK_ACTION_SUCCESS_RATE}"
  --output-dir "${SIMULATION_OUTPUT_DIR}"
)

if [[ "${DISABLE_CORE_TRACE}" == "1" ]]; then
  cmd+=(--disable-core-trace)
fi

if [[ -n "${QUICKSTART_CONFIG}" ]]; then
  cmd+=(--config "${QUICKSTART_CONFIG}")
fi

if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
  cmd+=("${EXTRA_ARGS[@]}")
fi

exec "${cmd[@]}"
