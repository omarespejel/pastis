#!/usr/bin/env bash
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  echo "error: curl is required" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

UPSTREAM_RPC_URL="${STARKNET_RPC_URL:-${PASTIS_DEMO_UPSTREAM_RPC_URL:-https://rpc.starknet.lava.build}}"
DAEMON_BIND="${PASTIS_DEMO_RPC_BIND:-127.0.0.1:9545}"
DAEMON_TOKEN="${PASTIS_DEMO_RPC_AUTH_TOKEN:-pastis-demo-token}"
POLL_MS="${PASTIS_DEMO_POLL_MS:-1500}"
STATE_DIR="${PASTIS_DEMO_STATE_DIR:-${ROOT_DIR}/.pastis/demo-daemon}"
KEEP_RUNNING="${PASTIS_DEMO_KEEP_RUNNING:-0}"
CLEAN_START="${PASTIS_DEMO_CLEAN_START:-1}"

CHECKPOINT_PATH="${STATE_DIR}/replay-checkpoint.json"
JOURNAL_PATH="${STATE_DIR}/local-journal.jsonl"
SNAPSHOT_PATH="${STATE_DIR}/storage.snapshot"
LOG_PATH="${STATE_DIR}/daemon.log"

mkdir -p "${STATE_DIR}"
rm -f "${LOG_PATH}"
if [[ "${CLEAN_START}" == "1" ]]; then
  rm -f "${CHECKPOINT_PATH}" "${JOURNAL_PATH}" "${SNAPSHOT_PATH}"
fi

json_rpc_call() {
  local endpoint="$1"
  local payload="$2"
  curl -sS -X POST "${endpoint}" \
    -H 'content-type: application/json' \
    --data "${payload}"
}

discover_chain_id() {
  local upstream="$1"
  local response
  response="$(json_rpc_call "${upstream}" '{"jsonrpc":"2.0","id":1,"method":"starknet_chainId","params":[]}' )"
  local result
  result="$(printf '%s' "${response}" | jq -r '.result // empty')"
  if [[ -z "${result}" ]]; then
    local error
    error="$(printf '%s' "${response}" | jq -c '.error // "unknown error"')"
    echo "error: failed to query upstream chain id from ${upstream}: ${error}" >&2
    exit 1
  fi
  printf '%s' "${result}"
}

normalize_chain_id() {
  local raw="${1}"
  local upper
  upper="$(printf '%s' "${raw}" | tr '[:lower:]' '[:upper:]')"
  case "${upper}" in
    SN_MAIN|0X534E5F4D41494E)
      printf 'SN_MAIN'
      ;;
    SN_SEPOLIA|0X534E5F5345504F4C4941)
      printf 'SN_SEPOLIA'
      ;;
    *)
      if [[ -n "${PASTIS_DEMO_CHAIN_ID:-}" ]]; then
        printf '%s' "${PASTIS_DEMO_CHAIN_ID}"
      else
        echo "error: unsupported upstream chain id '${raw}'. Set PASTIS_DEMO_CHAIN_ID explicitly." >&2
        exit 1
      fi
      ;;
  esac
}

wait_for_http_endpoint() {
  local url="$1"
  local attempts="$2"
  local sleep_seconds="$3"

  local code
  for _ in $(seq 1 "${attempts}"); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "${url}" || true)"
    if [[ "${code}" != "000" ]]; then
      printf '%s' "${code}"
      return 0
    fi
    sleep "${sleep_seconds}"
  done
  return 1
}

assert_local_rpc_success() {
  local payload="$1"
  local response
  response="$(curl -sS -X POST "http://${DAEMON_BIND}/" \
    -H "authorization: Bearer ${DAEMON_TOKEN}" \
    -H 'content-type: application/json' \
    --data "${payload}")"
  if ! printf '%s' "${response}" | jq -e '.jsonrpc == "2.0" and (.error | not)' >/dev/null; then
    echo "error: local RPC returned failure for payload: ${payload}" >&2
    echo "response: ${response}" >&2
    exit 1
  fi
  printf '%s' "${response}"
}

local_rpc_call() {
  local payload="$1"
  curl -sS -X POST "http://${DAEMON_BIND}/" \
    -H "authorization: Bearer ${DAEMON_TOKEN}" \
    -H 'content-type: application/json' \
    --data "${payload}"
}

upstream_chain_raw="$(discover_chain_id "${UPSTREAM_RPC_URL}")"
daemon_chain_id="$(normalize_chain_id "${upstream_chain_raw}")"

echo "starting daemon smoke run"
echo "upstream_rpc_url: ${UPSTREAM_RPC_URL}"
echo "upstream_chain_id(raw): ${upstream_chain_raw}"
echo "daemon_chain_id: ${daemon_chain_id}"
echo "daemon_bind: ${DAEMON_BIND}"
echo "state_dir: ${STATE_DIR}"
echo "clean_start: ${CLEAN_START}"

cargo run -p starknet-node --bin starknet-node -- \
  --upstream-rpc-url "${UPSTREAM_RPC_URL}" \
  --chain-id "${daemon_chain_id}" \
  --rpc-bind "${DAEMON_BIND}" \
  --rpc-auth-token "${DAEMON_TOKEN}" \
  --poll-ms "${POLL_MS}" \
  --replay-checkpoint "${CHECKPOINT_PATH}" \
  --local-journal "${JOURNAL_PATH}" \
  --storage-snapshot "${SNAPSHOT_PATH}" \
  --storage-snapshot-interval-blocks 32 \
  >"${LOG_PATH}" 2>&1 &
daemon_pid=$!

cleanup() {
  if kill -0 "${daemon_pid}" >/dev/null 2>&1; then
    kill "${daemon_pid}" >/dev/null 2>&1 || true
    wait "${daemon_pid}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

health_code="$(wait_for_http_endpoint "http://${DAEMON_BIND}/healthz" 60 1 || true)"
if [[ -z "${health_code}" ]]; then
  echo "error: timed out waiting for http://${DAEMON_BIND}/healthz to become reachable" >&2
  echo "daemon log tail:" >&2
  tail -n 120 "${LOG_PATH}" >&2 || true
  exit 1
fi
if [[ "${health_code}" != "200" && "${health_code}" != "503" ]]; then
  echo "error: unexpected /healthz status code: ${health_code}" >&2
  echo "daemon log tail:" >&2
  tail -n 120 "${LOG_PATH}" >&2 || true
  exit 1
fi

ready_code="$(curl -s -o /dev/null -w '%{http_code}' "http://${DAEMON_BIND}/readyz" || true)"

status_json=""
for _ in $(seq 1 45); do
  candidate="$(curl -sS "http://${DAEMON_BIND}/status" -H "authorization: Bearer ${DAEMON_TOKEN}")"
  if ! printf '%s' "${candidate}" | jq -e '.chain_id and .current_block >= 0 and .highest_block >= 0 and .consecutive_failures >= 0' >/dev/null; then
    echo "error: invalid /status payload: ${candidate}" >&2
    exit 1
  fi
  status_json="${candidate}"
  if printf '%s' "${candidate}" | jq -e '.highest_block > 0 or .current_block > 0 or .consecutive_failures > 0' >/dev/null; then
    break
  fi
  sleep 1
done

chain_resp="$(assert_local_rpc_success '{"jsonrpc":"2.0","id":1,"method":"starknet_chainId","params":[]}' )"
syncing_resp="$(assert_local_rpc_success '{"jsonrpc":"2.0","id":3,"method":"starknet_syncing","params":[]}' )"

block_resp=""
block_ready=0
for _ in $(seq 1 45); do
  candidate="$(local_rpc_call '{"jsonrpc":"2.0","id":2,"method":"starknet_blockNumber","params":[]}' )"
  if printf '%s' "${candidate}" | jq -e '.jsonrpc == "2.0" and (.error | not) and (.result != null)' >/dev/null; then
    block_resp="${candidate}"
    block_ready=1
    break
  fi
  if printf '%s' "${candidate}" | jq -e '.error.code == 32' >/dev/null; then
    sleep 1
    continue
  fi
  echo "error: local RPC returned unexpected response for starknet_blockNumber: ${candidate}" >&2
  exit 1
done

if [[ "${block_ready}" == "1" ]]; then
  assert_local_rpc_success '{"jsonrpc":"2.0","id":4,"method":"starknet_blockHashAndNumber","params":[]}' >/dev/null
  assert_local_rpc_success '{"jsonrpc":"2.0","id":5,"method":"starknet_getBlockWithTxs","params":["latest"]}' >/dev/null
fi

metrics_payload="$(curl -sS "http://${DAEMON_BIND}/metrics" -H "authorization: Bearer ${DAEMON_TOKEN}")"
if ! printf '%s' "${metrics_payload}" | grep -q 'pastis_rpc_requests_total'; then
  echo "error: /metrics payload missing expected counters" >&2
  exit 1
fi

local_chain="$(printf '%s' "${chain_resp}" | jq -r '.result')"
local_syncing="$(printf '%s' "${syncing_resp}" | jq -c '.result')"
status_current="$(printf '%s' "${status_json}" | jq -r '.current_block')"
status_highest="$(printf '%s' "${status_json}" | jq -r '.highest_block')"
if [[ "${block_ready}" == "1" ]]; then
  local_block="$(printf '%s' "${block_resp}" | jq -r '.result')"
else
  local_block="N/A (no committed local block yet)"
fi
status_failures="$(printf '%s' "${status_json}" | jq -r '.consecutive_failures')"

echo
echo "demo smoke run: PASS"
echo "healthz_http: ${health_code} (200=healthy, 503=sync degraded but serving)"
echo "readyz_http: ${ready_code} (200=fully caught up, 503=syncing)"
echo "status.current_block: ${status_current}"
echo "status.highest_block: ${status_highest}"
echo "status.consecutive_failures: ${status_failures}"
echo "rpc.chain_id: ${local_chain}"
echo "rpc.block_number: ${local_block}"
echo "rpc.syncing: ${local_syncing}"
if [[ "${block_ready}" != "1" ]]; then
  echo "warning: block number stayed unavailable during warmup window; daemon is still starting/syncing"
fi
echo "daemon_log: ${LOG_PATH}"

if [[ "${KEEP_RUNNING}" == "1" ]]; then
  trap - EXIT
  echo
  echo "PASTIS_DEMO_KEEP_RUNNING=1 set; daemon continues running under PID ${daemon_pid}"
  echo "press Ctrl+C to stop tail and terminate daemon"
  tail -f "${LOG_PATH}"
fi
