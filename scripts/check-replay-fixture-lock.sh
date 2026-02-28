#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE_DIR="$ROOT_DIR/crates/node/tests/fixtures/replay/rpc"
LOCK_FILE="$FIXTURE_DIR/fixtures.lock"

if [[ ! -d "$FIXTURE_DIR" ]]; then
  echo "replay fixture directory is missing: $FIXTURE_DIR" >&2
  exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
  hash_file() {
    sha256sum "$1" | awk '{print $1}'
  }
elif command -v shasum >/dev/null 2>&1; then
  hash_file() {
    shasum -a 256 "$1" | awk '{print $1}'
  }
else
  echo "no SHA-256 utility found (expected sha256sum or shasum)" >&2
  exit 1
fi

TMP_LOCK="$(mktemp)"
trap 'rm -f "$TMP_LOCK"' EXIT

(
  cd "$FIXTURE_DIR"
  while IFS= read -r filename; do
    hash="$(hash_file "$filename")"
    printf "%s  %s\n" "$hash" "$filename"
  done < <(find . -maxdepth 1 -type f -name '*.json' -exec basename {} \; | LC_ALL=C sort)
) > "$TMP_LOCK"

if [[ "${PASTIS_FIXTURE_LOCK_UPDATE:-0}" == "1" ]]; then
  mv "$TMP_LOCK" "$LOCK_FILE"
  trap - EXIT
  echo "updated replay fixture lock file: $LOCK_FILE"
  exit 0
fi

if [[ ! -f "$LOCK_FILE" ]]; then
  echo "missing replay fixture lock file: $LOCK_FILE" >&2
  echo "run: PASTIS_FIXTURE_LOCK_UPDATE=1 ./scripts/check-replay-fixture-lock.sh" >&2
  exit 1
fi

if ! diff -u "$LOCK_FILE" "$TMP_LOCK"; then
  echo "replay fixture lock mismatch; rerun capture/update process and refresh lock file" >&2
  echo "run: PASTIS_FIXTURE_LOCK_UPDATE=1 ./scripts/check-replay-fixture-lock.sh" >&2
  exit 1
fi

echo "replay fixture lock verified."
