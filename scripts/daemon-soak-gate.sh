#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: daemon-soak-gate.sh --rpc-url <url> [options]

Runs a long-lived real-data daemon soak with pass/fail gates.

Options:
  --rpc-url <url>                  Upstream Starknet RPC URL (or STARKNET_RPC_URL)
  --duration-minutes <n>           Soak duration in minutes (default: 60)
  --poll-seconds <n>               Poll interval for health/status checks (default: 15)
  --daemon-bind <host:port>        Daemon bind address (default: 127.0.0.1:9545)
  --auth-token <token>             Daemon bearer token (default: pastis-soak-token)
  --state-dir <path>               State directory (default: .pastis/soak-daemon)
  --allow-synthetic-fallback       Explicitly allow synthetic fallback (default: disabled)
  --clean-start                    Remove prior state/checkpoints before run (default: enabled)
  --no-clean-start                 Keep prior state/checkpoints
  --help                           Show help

Environment overrides:
  STARKNET_RPC_URL
  PASTIS_SOAK_DURATION_MINUTES
  PASTIS_SOAK_POLL_SECONDS
  PASTIS_SOAK_DAEMON_BIND
  PASTIS_SOAK_AUTH_TOKEN
  PASTIS_SOAK_STATE_DIR
  PASTIS_SOAK_ALLOW_SYNTHETIC_FALLBACK (true/false)
  PASTIS_SOAK_CLEAN_START (true/false)
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: required command not found: $cmd" >&2
    exit 1
  fi
}

parse_bool() {
  local raw="${1:-}"
  local normalized
  normalized="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  case "$normalized" in
    1|true|yes|on) printf '1' ;;
    0|false|no|off) printf '0' ;;
    *)
      echo "error: invalid boolean value '$raw'" >&2
      exit 1
      ;;
  esac
}

redact_url() {
  local raw="$1"
  if [[ "$raw" =~ ^([a-zA-Z][a-zA-Z0-9+.-]*)://([^/@]+@)?([^/:?#]+)(:([0-9]+))?.*$ ]]; then
    local scheme="${BASH_REMATCH[1]}"
    local host="${BASH_REMATCH[3]}"
    local port="${BASH_REMATCH[5]:-}"
    if [[ -n "$port" ]]; then
      printf '%s://%s:%s' "$scheme" "$host" "$port"
    else
      printf '%s://%s' "$scheme" "$host"
    fi
    return 0
  fi
  printf '<invalid-rpc-url>'
}

iso8601_utc_from_epoch() {
  local epoch="$1"
  if date -u -d "@$epoch" '+%Y-%m-%dT%H:%M:%SZ' >/dev/null 2>&1; then
    date -u -d "@$epoch" '+%Y-%m-%dT%H:%M:%SZ'
    return 0
  fi
  if date -u -r "$epoch" '+%Y-%m-%dT%H:%M:%SZ' >/dev/null 2>&1; then
    date -u -r "$epoch" '+%Y-%m-%dT%H:%M:%SZ'
    return 0
  fi
  printf '<invalid-timestamp>'
}

json_rpc_call() {
  local endpoint="$1"
  local payload="$2"
  curl -sS -X POST "$endpoint" \
    -H 'content-type: application/json' \
    --connect-timeout 5 \
    --max-time 15 \
    --data "$payload"
}

discover_chain_id() {
  local upstream="$1"
  local response
  response="$(json_rpc_call "$upstream" '{"jsonrpc":"2.0","id":1,"method":"starknet_chainId","params":[]}')"
  local result
  result="$(printf '%s' "$response" | jq -r '.result // empty')"
  if [[ -z "$result" ]]; then
    local err
    err="$(printf '%s' "$response" | jq -c '.error // "unknown error"')"
    echo "error: failed to query upstream chain id from $upstream: $err" >&2
    exit 1
  fi
  printf '%s' "$result"
}

normalize_chain_id() {
  local raw="$1"
  local upper
  upper="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$upper" in
    SN_MAIN|0X534E5F4D41494E) printf 'SN_MAIN' ;;
    SN_SEPOLIA|0X534E5F5345504F4C4941) printf 'SN_SEPOLIA' ;;
    *)
      echo "error: unsupported upstream chain id '$raw'; set PASTIS_SOAK_CHAIN_ID explicitly if needed" >&2
      exit 1
      ;;
  esac
}

http_code() {
  local url="$1"
  curl -s -o /dev/null -w '%{http_code}' \
    --connect-timeout 3 \
    --max-time 5 \
    "$url" || true
}

require_cmd curl
require_cmd jq
require_cmd cargo

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

UPSTREAM_RPC_URL="${STARKNET_RPC_URL:-}"
DURATION_MINUTES="${PASTIS_SOAK_DURATION_MINUTES:-60}"
POLL_SECONDS="${PASTIS_SOAK_POLL_SECONDS:-15}"
DAEMON_BIND="${PASTIS_SOAK_DAEMON_BIND:-127.0.0.1:9545}"
DAEMON_TOKEN="${PASTIS_SOAK_AUTH_TOKEN:-pastis-soak-token}"
STATE_DIR="${PASTIS_SOAK_STATE_DIR:-${ROOT_DIR}/.pastis/soak-daemon}"
ALLOW_SYNTHETIC_FALLBACK="$(parse_bool "${PASTIS_SOAK_ALLOW_SYNTHETIC_FALLBACK:-false}")"
CLEAN_START="$(parse_bool "${PASTIS_SOAK_CLEAN_START:-true}")"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rpc-url)
      [[ $# -ge 2 ]] || { echo "--rpc-url requires a value" >&2; exit 1; }
      UPSTREAM_RPC_URL="$2"
      shift 2
      ;;
    --duration-minutes)
      [[ $# -ge 2 ]] || { echo "--duration-minutes requires a value" >&2; exit 1; }
      DURATION_MINUTES="$2"
      shift 2
      ;;
    --poll-seconds)
      [[ $# -ge 2 ]] || { echo "--poll-seconds requires a value" >&2; exit 1; }
      POLL_SECONDS="$2"
      shift 2
      ;;
    --daemon-bind)
      [[ $# -ge 2 ]] || { echo "--daemon-bind requires a value" >&2; exit 1; }
      DAEMON_BIND="$2"
      shift 2
      ;;
    --auth-token)
      [[ $# -ge 2 ]] || { echo "--auth-token requires a value" >&2; exit 1; }
      DAEMON_TOKEN="$2"
      shift 2
      ;;
    --state-dir)
      [[ $# -ge 2 ]] || { echo "--state-dir requires a value" >&2; exit 1; }
      STATE_DIR="$2"
      shift 2
      ;;
    --allow-synthetic-fallback)
      ALLOW_SYNTHETIC_FALLBACK=1
      shift
      ;;
    --clean-start)
      CLEAN_START=1
      shift
      ;;
    --no-clean-start)
      CLEAN_START=0
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$UPSTREAM_RPC_URL" ]]; then
  echo "error: missing upstream RPC URL; pass --rpc-url or set STARKNET_RPC_URL" >&2
  exit 1
fi
if [[ ! "$UPSTREAM_RPC_URL" =~ ^https?:// ]]; then
  echo "error: upstream RPC URL must use http/https: $UPSTREAM_RPC_URL" >&2
  exit 1
fi
if [[ ! "$DURATION_MINUTES" =~ ^[0-9]+$ ]] || [[ "$DURATION_MINUTES" -eq 0 ]]; then
  echo "error: --duration-minutes must be a positive integer" >&2
  exit 1
fi
if [[ ! "$POLL_SECONDS" =~ ^[0-9]+$ ]] || [[ "$POLL_SECONDS" -eq 0 ]]; then
  echo "error: --poll-seconds must be a positive integer" >&2
  exit 1
fi

mkdir -p "$STATE_DIR"
CHECKPOINT_PATH="$STATE_DIR/replay-checkpoint.json"
JOURNAL_PATH="$STATE_DIR/local-journal.jsonl"
SNAPSHOT_PATH="$STATE_DIR/storage.snapshot"
LOG_PATH="$STATE_DIR/daemon.log"
REPORT_PATH="$STATE_DIR/soak-report.json"
TIMELINE_PATH="$STATE_DIR/soak-timeline.jsonl"

rm -f "$LOG_PATH" "$REPORT_PATH" "$TIMELINE_PATH"
if [[ "$CLEAN_START" == "1" ]]; then
  rm -f "$CHECKPOINT_PATH" "$JOURNAL_PATH" "$SNAPSHOT_PATH"
fi

CHAIN_ID_RAW="$(discover_chain_id "$UPSTREAM_RPC_URL")"
CHAIN_ID="$(normalize_chain_id "$CHAIN_ID_RAW")"

echo "starting daemon soak gate"
echo "upstream_rpc_url: $(redact_url "$UPSTREAM_RPC_URL")"
echo "upstream_chain_id(raw): $CHAIN_ID_RAW"
echo "daemon_chain_id: $CHAIN_ID"
echo "daemon_bind: $DAEMON_BIND"
echo "duration_minutes: $DURATION_MINUTES"
echo "poll_seconds: $POLL_SECONDS"
echo "allow_synthetic_fallback: $ALLOW_SYNTHETIC_FALLBACK"
echo "state_dir: $STATE_DIR"

DAEMON_ARGS=(
  --upstream-rpc-url "$UPSTREAM_RPC_URL"
  --chain-id "$CHAIN_ID"
  --rpc-bind "$DAEMON_BIND"
  --rpc-auth-token "$DAEMON_TOKEN"
  --poll-ms 1500
  --replay-checkpoint "$CHECKPOINT_PATH"
  --local-journal "$JOURNAL_PATH"
  --storage-snapshot "$SNAPSHOT_PATH"
  --storage-snapshot-interval-blocks 32
)
if [[ "$ALLOW_SYNTHETIC_FALLBACK" == "1" ]]; then
  DAEMON_ARGS+=(--allow-synthetic-execution-fallback)
else
  DAEMON_ARGS+=(--no-allow-synthetic-execution-fallback)
fi

cargo run --locked --release -p starknet-node --bin starknet-node --features production-adapters -- "${DAEMON_ARGS[@]}" >"$LOG_PATH" 2>&1 &
DAEMON_PID=$!

cleanup() {
  if kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill "$DAEMON_PID" >/dev/null 2>&1 || true
    wait "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# Wait for endpoint reachability.
for _ in $(seq 1 120); do
  if [[ "$(http_code "http://$DAEMON_BIND/healthz")" != "000" ]]; then
    break
  fi
  if ! kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    echo "error: daemon exited before becoming reachable" >&2
    tail -n 120 "$LOG_PATH" >&2 || true
    exit 1
  fi
  sleep 1
done

if [[ "$(http_code "http://$DAEMON_BIND/healthz")" == "000" ]]; then
  echo "error: daemon endpoint not reachable after startup window" >&2
  tail -n 120 "$LOG_PATH" >&2 || true
  exit 1
fi

start_epoch="$(date +%s)"
start_iso="$(iso8601_utc_from_epoch "$start_epoch")"
end_epoch="$((start_epoch + DURATION_MINUTES * 60))"

samples=0
health_failures=0
ready_failures=0
status_failures=0
max_consecutive_failures=0
max_sync_lag=0
max_flaps_recent=0
block_advance_events=0
last_current_block=""
last_highest_block=""
last_health_code=""
last_ready_code=""
last_status_json='{}'

while [[ "$(date +%s)" -lt "$end_epoch" ]]; do
  if ! kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    echo "error: daemon exited during soak" >&2
    tail -n 120 "$LOG_PATH" >&2 || true
    break
  fi

  now_epoch="$(date +%s)"
  now_iso="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  health_code="$(http_code "http://$DAEMON_BIND/healthz")"
  ready_code="$(http_code "http://$DAEMON_BIND/readyz")"
  status_json="$(
    curl -sS "http://$DAEMON_BIND/status" \
      -H "authorization: Bearer $DAEMON_TOKEN" \
      --connect-timeout 3 \
      --max-time 5 || true
  )"

  samples=$((samples + 1))
  last_health_code="$health_code"
  last_ready_code="$ready_code"

  if [[ "$health_code" != "200" ]]; then
    health_failures=$((health_failures + 1))
  fi
  if [[ "$ready_code" != "200" ]]; then
    ready_failures=$((ready_failures + 1))
  fi

  status_ok=0
  current_block=0
  highest_block=0
  consecutive_failures=0
  flaps_recent=0
  if printf '%s' "$status_json" | jq -e '.current_block >= 0 and .highest_block >= 0 and .consecutive_failures >= 0' >/dev/null 2>&1; then
    status_ok=1
    current_block="$(printf '%s' "$status_json" | jq -r '.current_block')"
    highest_block="$(printf '%s' "$status_json" | jq -r '.highest_block')"
    consecutive_failures="$(printf '%s' "$status_json" | jq -r '.consecutive_failures')"
    flaps_recent="$(printf '%s' "$status_json" | jq -r '.peer_health.flap_transitions_recent // 0')"
    last_status_json="$status_json"

    if [[ -n "$last_current_block" ]] && [[ "$current_block" -gt "$last_current_block" ]]; then
      block_advance_events=$((block_advance_events + 1))
    fi
    last_current_block="$current_block"
    last_highest_block="$highest_block"

    sync_lag=$((highest_block - current_block))
    if [[ "$sync_lag" -gt "$max_sync_lag" ]]; then
      max_sync_lag="$sync_lag"
    fi
    if [[ "$consecutive_failures" -gt "$max_consecutive_failures" ]]; then
      max_consecutive_failures="$consecutive_failures"
    fi
    if [[ "$flaps_recent" -gt "$max_flaps_recent" ]]; then
      max_flaps_recent="$flaps_recent"
    fi
  else
    status_failures=$((status_failures + 1))
  fi

  jq -cn \
    --arg ts "$now_iso" \
    --argjson epoch "$now_epoch" \
    --arg health_code "$health_code" \
    --arg ready_code "$ready_code" \
    --argjson status_ok "$status_ok" \
    --argjson current_block "$current_block" \
    --argjson highest_block "$highest_block" \
    --argjson consecutive_failures "$consecutive_failures" \
    --argjson flap_transitions_recent "$flaps_recent" \
    '{ts:$ts,epoch:$epoch,health_code:$health_code,ready_code:$ready_code,status_ok:($status_ok==1),current_block:$current_block,highest_block:$highest_block,consecutive_failures:$consecutive_failures,flap_transitions_recent:$flap_transitions_recent}' \
    >>"$TIMELINE_PATH"

  sleep "$POLL_SECONDS"
done

runtime_alive=0
if kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
  runtime_alive=1
fi

pass=1
reasons=()
if [[ "$runtime_alive" -ne 1 ]]; then
  pass=0
  reasons+=("daemon_process_exited")
fi
if [[ "$health_failures" -gt 0 ]]; then
  pass=0
  reasons+=("healthz_non_200_observed")
fi
if [[ "$status_failures" -gt 0 ]]; then
  pass=0
  reasons+=("status_endpoint_failures")
fi
if [[ "$block_advance_events" -eq 0 ]]; then
  pass=0
  reasons+=("no_block_progress_observed")
fi
if [[ "$max_consecutive_failures" -gt 3 ]]; then
  pass=0
  reasons+=("max_consecutive_failures_exceeded")
fi

jq -cn \
  --arg started_at "$start_iso" \
  --arg finished_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
  --arg upstream_rpc_url "$(redact_url "$UPSTREAM_RPC_URL")" \
  --arg daemon_bind "$DAEMON_BIND" \
  --arg daemon_chain_id "$CHAIN_ID" \
  --argjson duration_minutes "$DURATION_MINUTES" \
  --argjson poll_seconds "$POLL_SECONDS" \
  --argjson samples "$samples" \
  --argjson health_failures "$health_failures" \
  --argjson ready_failures "$ready_failures" \
  --argjson status_failures "$status_failures" \
  --argjson block_advance_events "$block_advance_events" \
  --argjson max_sync_lag "$max_sync_lag" \
  --argjson max_consecutive_failures "$max_consecutive_failures" \
  --argjson max_flap_transitions_recent "$max_flaps_recent" \
  --argjson runtime_alive "$runtime_alive" \
  --argjson passed "$pass" \
  --argjson reasons "$(
    if [[ "${#reasons[@]}" -eq 0 ]]; then
      printf '[]'
    else
      printf '%s\n' "${reasons[@]}" | jq -R . | jq -s .
    fi
  )" \
  --argjson last_status "$last_status_json" \
  '{started_at:$started_at,finished_at:$finished_at,upstream_rpc_url:$upstream_rpc_url,daemon_bind:$daemon_bind,daemon_chain_id:$daemon_chain_id,duration_minutes:$duration_minutes,poll_seconds:$poll_seconds,samples:$samples,health_failures:$health_failures,ready_failures:$ready_failures,status_failures:$status_failures,block_advance_events:$block_advance_events,max_sync_lag:$max_sync_lag,max_consecutive_failures:$max_consecutive_failures,max_flap_transitions_recent:$max_flap_transitions_recent,runtime_alive:($runtime_alive==1),passed:($passed==1),failure_reasons:$reasons,last_status:$last_status}' \
  >"$REPORT_PATH"

echo
echo "soak report: $REPORT_PATH"
cat "$REPORT_PATH" | jq .
echo
if [[ "$pass" -eq 1 ]]; then
  echo "daemon soak gate: PASS"
else
  echo "daemon soak gate: FAIL"
  exit 1
fi
