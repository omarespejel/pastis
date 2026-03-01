#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: daemon-soak-gate.sh --rpc-url <url> [options]

Runs a long-lived real-data daemon soak with pass/fail gates.

Options:
  --rpc-url <url>                  Upstream Starknet RPC URL (or STARKNET_RPC_URL)
  --chain-id <id>                  Override daemon chain id (e.g. SN_MAIN, SN_SEPOLIA, SN_FOO)
  --duration-minutes <n>           Soak duration in minutes (default: 60)
  --poll-seconds <n>               Poll interval for health/status checks (default: 15)
  --startup-timeout-seconds <n>    Max wait for daemon endpoint startup (default: 900)
  --daemon-bind <host:port>        Daemon bind address (default: 127.0.0.1:9545)
  --auth-token <token>             Daemon bearer token (auto-generated on loopback binds)
  --state-dir <path>               State directory (default: .pastis/soak-daemon)
  --allow-http-upstream            Allow plain HTTP upstream RPC URL (disabled by default)
  --allow-private-rpc-url          Allow private/localhost upstream RPC hosts
  --allow-synthetic-fallback       Explicitly allow synthetic fallback (default: disabled)
  --clean-start                    Remove prior state/checkpoints before run (default: enabled)
  --no-clean-start                 Keep prior state/checkpoints
  --help                           Show help

Environment overrides:
  STARKNET_RPC_URL
  PASTIS_SOAK_CHAIN_ID
  PASTIS_SOAK_DURATION_MINUTES
  PASTIS_SOAK_POLL_SECONDS
  PASTIS_SOAK_STARTUP_TIMEOUT_SECONDS
  PASTIS_SOAK_DAEMON_BIND
  PASTIS_SOAK_AUTH_TOKEN
  PASTIS_SOAK_STATE_DIR
  PASTIS_SOAK_ALLOW_HTTP_UPSTREAM (true/false)
  PASTIS_SOAK_ALLOW_PRIVATE_RPC_URL (true/false)
  PASTIS_SOAK_ALLOW_UNSAFE_STATE_DIR (true/false)
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

resolve_existing_path() {
  local raw="$1"
  if command -v realpath >/dev/null 2>&1; then
    realpath "$raw"
    return 0
  fi
  if command -v readlink >/dev/null 2>&1; then
    readlink -f "$raw"
    return 0
  fi
  echo "error: either realpath or readlink is required for secure path resolution" >&2
  exit 1
}

canonicalize_candidate_path() {
  local raw="$1"
  local normalized="$raw"
  if [[ "$normalized" == "~/"* ]]; then
    normalized="${HOME}/${normalized#~/}"
  fi
  if [[ "$normalized" != /* ]]; then
    normalized="${ROOT_DIR}/${normalized}"
  fi
  local parent
  parent="$(dirname "$normalized")"
  local parent_resolved
  parent_resolved="$(resolve_existing_path "$parent")"
  printf '%s/%s' "$parent_resolved" "$(basename "$normalized")"
}

enforce_state_dir_policy() {
  local state_dir="$1"
  local root_dir="$2"
  local tmp_dir="$3"
  local allow_unsafe="$4"
  if [[ "$allow_unsafe" == "1" ]]; then
    return 0
  fi
  case "$state_dir" in
    "$root_dir"/*|"$tmp_dir"/*) ;;
    *)
      echo "error: state-dir '$state_dir' is blocked by default; use a path under '$root_dir' or '$tmp_dir', or set PASTIS_SOAK_ALLOW_UNSAFE_STATE_DIR=true to override" >&2
      exit 1
      ;;
  esac
}

extract_bind_host() {
  local bind="$1"
  local host="$bind"
  if [[ "$host" =~ ^\[([^]]+)\](:[0-9]+)?$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  if [[ "$host" =~ ^([^:]+):[0-9]+$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  printf '%s' "$host"
}

extract_url_host() {
  local raw="$1"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  local rest="${lowered#*://}"
  rest="${rest%%/*}"
  rest="${rest##*@}"
  if [[ "$rest" =~ ^\[([^]]+)\](:[0-9]+)?$ ]]; then
    printf '%s' "${BASH_REMATCH[1]}"
    return 0
  fi
  printf '%s' "${rest%%:*}"
}

validate_chain_id_literal() {
  local raw="$1"
  if [[ ! "$raw" =~ ^SN_[A-Z0-9_]+$ ]]; then
    echo "error: invalid chain id '$raw'; expected format SN_[A-Z0-9_]+" >&2
    exit 1
  fi
}

validate_bearer_token() {
  local token="$1"
  if [[ -z "$token" ]]; then
    echo "error: bearer token must not be empty" >&2
    exit 1
  fi
  if [[ "$token" =~ [[:space:][:cntrl:]] ]]; then
    echo "error: bearer token must not contain whitespace/control characters" >&2
    exit 1
  fi
}

is_loopback_bind() {
  local bind="$1"
  local host
  host="$(extract_bind_host "$bind")"
  case "$host" in
    127.0.0.1|localhost|::1) return 0 ;;
    *) return 1 ;;
  esac
}

generate_token() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 24
  else
    printf 'soak-%s-%s-%s' "$(date +%s)" "$$" "$RANDOM"
  fi
}

validate_upstream_rpc_url() {
  local raw="$1"
  local allow_private="$2"
  local allow_http="$3"
  local lowered
  lowered="$(printf '%s' "$raw" | tr '[:upper:]' '[:lower:]')"
  if [[ "$lowered" =~ ^https:// ]]; then
    :
  elif [[ "$lowered" =~ ^http:// ]]; then
    if [[ "$allow_http" != "1" ]]; then
      echo "error: upstream RPC URL must use https:// by default; pass --allow-http-upstream or set PASTIS_SOAK_ALLOW_HTTP_UPSTREAM=true to allow http:// for non-production testing" >&2
      exit 1
    fi
  else
    echo "error: upstream RPC URL must use http/https: $(redact_url "$raw")" >&2
    exit 1
  fi
  local host
  host="$(extract_url_host "$raw")"
  if [[ -z "$host" ]]; then
    echo "error: upstream RPC URL host is invalid: $(redact_url "$raw")" >&2
    exit 1
  fi
  case "$host" in
    localhost|127.*|0.0.0.0|::1|169.254.169.254)
      if [[ "$allow_private" != "1" ]]; then
        echo "error: upstream RPC URL host '$host' is blocked by default; pass --allow-private-rpc-url to override" >&2
        exit 1
      fi
      ;;
  esac
  if [[ "$host" =~ ^10\. ]] || [[ "$host" =~ ^192\.168\. ]] || [[ "$host" =~ ^172\.(1[6-9]|2[0-9]|3[0-1])\. ]]; then
    if [[ "$allow_private" != "1" ]]; then
      echo "error: private upstream RPC host '$host' is blocked by default; pass --allow-private-rpc-url to override" >&2
      exit 1
    fi
  fi
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
    echo "error: failed to query upstream chain id from $(redact_url "$upstream"): $err" >&2
    exit 1
  fi
  printf '%s' "$result"
}

normalize_chain_id() {
  local raw="$1"
  local override="${2:-}"
  local upper
  upper="$(printf '%s' "$raw" | tr '[:lower:]' '[:upper:]')"
  case "$upper" in
    SN_MAIN|0X534E5F4D41494E) printf 'SN_MAIN' ;;
    SN_SEPOLIA|0X534E5F5345504F4C4941) printf 'SN_SEPOLIA' ;;
    SN_[A-Z0-9_]*) printf '%s' "$upper" ;;
    *)
      if [[ -n "$override" ]]; then
        validate_chain_id_literal "$override"
        printf '%s' "$override"
      else
        echo "error: unsupported upstream chain id '$raw'; pass --chain-id or set PASTIS_SOAK_CHAIN_ID explicitly" >&2
        exit 1
      fi
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
CHAIN_ID_OVERRIDE="${PASTIS_SOAK_CHAIN_ID:-}"
DURATION_MINUTES="${PASTIS_SOAK_DURATION_MINUTES:-60}"
POLL_SECONDS="${PASTIS_SOAK_POLL_SECONDS:-15}"
STARTUP_TIMEOUT_SECONDS="${PASTIS_SOAK_STARTUP_TIMEOUT_SECONDS:-900}"
DAEMON_BIND="${PASTIS_SOAK_DAEMON_BIND:-127.0.0.1:9545}"
DAEMON_TOKEN="${PASTIS_SOAK_AUTH_TOKEN:-}"
STATE_DIR="${PASTIS_SOAK_STATE_DIR:-${ROOT_DIR}/.pastis/soak-daemon}"
ALLOW_HTTP_UPSTREAM="$(parse_bool "${PASTIS_SOAK_ALLOW_HTTP_UPSTREAM:-false}")"
ALLOW_PRIVATE_RPC_URL="$(parse_bool "${PASTIS_SOAK_ALLOW_PRIVATE_RPC_URL:-false}")"
ALLOW_UNSAFE_STATE_DIR="$(parse_bool "${PASTIS_SOAK_ALLOW_UNSAFE_STATE_DIR:-false}")"
ALLOW_SYNTHETIC_FALLBACK="$(parse_bool "${PASTIS_SOAK_ALLOW_SYNTHETIC_FALLBACK:-false}")"
CLEAN_START="$(parse_bool "${PASTIS_SOAK_CLEAN_START:-true}")"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rpc-url)
      [[ $# -ge 2 ]] || { echo "--rpc-url requires a value" >&2; exit 1; }
      UPSTREAM_RPC_URL="$2"
      shift 2
      ;;
    --chain-id)
      [[ $# -ge 2 ]] || { echo "--chain-id requires a value" >&2; exit 1; }
      CHAIN_ID_OVERRIDE="$2"
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
    --startup-timeout-seconds)
      [[ $# -ge 2 ]] || { echo "--startup-timeout-seconds requires a value" >&2; exit 1; }
      STARTUP_TIMEOUT_SECONDS="$2"
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
    --allow-http-upstream)
      ALLOW_HTTP_UPSTREAM=1
      shift
      ;;
    --allow-private-rpc-url)
      ALLOW_PRIVATE_RPC_URL=1
      shift
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
if [[ -n "$CHAIN_ID_OVERRIDE" ]]; then
  validate_chain_id_literal "$CHAIN_ID_OVERRIDE"
fi
validate_upstream_rpc_url "$UPSTREAM_RPC_URL" "$ALLOW_PRIVATE_RPC_URL" "$ALLOW_HTTP_UPSTREAM"
if [[ ! "$DURATION_MINUTES" =~ ^[0-9]+$ ]] || [[ "$DURATION_MINUTES" -eq 0 ]]; then
  echo "error: --duration-minutes must be a positive integer" >&2
  exit 1
fi
if [[ ! "$POLL_SECONDS" =~ ^[0-9]+$ ]] || [[ "$POLL_SECONDS" -eq 0 ]]; then
  echo "error: --poll-seconds must be a positive integer" >&2
  exit 1
fi
if [[ ! "$STARTUP_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || [[ "$STARTUP_TIMEOUT_SECONDS" -eq 0 ]]; then
  echo "error: --startup-timeout-seconds must be a positive integer" >&2
  exit 1
fi

CANONICAL_ROOT_DIR="$(resolve_existing_path "$ROOT_DIR")"
CANONICAL_TMP_DIR="$(resolve_existing_path "/tmp")"
STATE_DIR="$(canonicalize_candidate_path "$STATE_DIR")"
enforce_state_dir_policy "$STATE_DIR" "$CANONICAL_ROOT_DIR" "$CANONICAL_TMP_DIR" "$ALLOW_UNSAFE_STATE_DIR"
if [[ -z "$DAEMON_TOKEN" ]]; then
  if is_loopback_bind "$DAEMON_BIND"; then
    DAEMON_TOKEN="$(generate_token)"
  else
    echo "error: --auth-token is required when daemon bind is not loopback" >&2
    exit 1
  fi
fi
validate_bearer_token "$DAEMON_TOKEN"

mkdir -p "$STATE_DIR"
STATE_DIR="$(resolve_existing_path "$STATE_DIR")"
enforce_state_dir_policy "$STATE_DIR" "$CANONICAL_ROOT_DIR" "$CANONICAL_TMP_DIR" "$ALLOW_UNSAFE_STATE_DIR"
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
CHAIN_ID="$(normalize_chain_id "$CHAIN_ID_RAW" "$CHAIN_ID_OVERRIDE")"

echo "starting daemon soak gate"
echo "upstream_rpc_url: $(redact_url "$UPSTREAM_RPC_URL")"
echo "upstream_chain_id(raw): $CHAIN_ID_RAW"
echo "daemon_chain_id: $CHAIN_ID"
echo "daemon_bind: $DAEMON_BIND"
echo "duration_minutes: $DURATION_MINUTES"
echo "poll_seconds: $POLL_SECONDS"
echo "startup_timeout_seconds: $STARTUP_TIMEOUT_SECONDS"
echo "allow_synthetic_fallback: $ALLOW_SYNTHETIC_FALLBACK"
echo "state_dir: $STATE_DIR"

DAEMON_ARGS=(
  --upstream-rpc-url "$UPSTREAM_RPC_URL"
  --chain-id "$CHAIN_ID"
  --rpc-bind "$DAEMON_BIND"
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

PASTIS_RPC_AUTH_TOKEN="$DAEMON_TOKEN" cargo run --locked --release -p starknet-node --bin starknet-node --features production-adapters -- "${DAEMON_ARGS[@]}" >"$LOG_PATH" 2>&1 &
DAEMON_PID=$!

cleanup() {
  if kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    kill "$DAEMON_PID" >/dev/null 2>&1 || true
    wait "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# Wait for endpoint reachability.
startup_attempt=0
while (( startup_attempt < STARTUP_TIMEOUT_SECONDS )); do
  if [[ "$(http_code "http://$DAEMON_BIND/healthz")" != "000" ]]; then
    break
  fi
  if ! kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    echo "error: daemon exited before becoming reachable" >&2
    tail -n 120 "$LOG_PATH" >&2 || true
    exit 1
  fi
  startup_attempt=$((startup_attempt + 1))
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
if [[ "$ready_failures" -gt 0 ]]; then
  pass=0
  reasons+=("readyz_non_200_observed")
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
jq . "$REPORT_PATH"
echo
if [[ "$pass" -eq 1 ]]; then
  echo "daemon soak gate: PASS"
else
  echo "daemon soak gate: FAIL"
  exit 1
fi
