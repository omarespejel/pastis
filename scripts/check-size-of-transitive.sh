#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TREE_OUTPUT="$(
  cd "${ROOT_DIR}"
  cargo tree -p starknet-node-storage --features papyrus-adapter -e normal
)"

if ! grep -q "size-of v" <<< "${TREE_OUTPUT}"; then
  echo "size-of is no longer a transitive dependency; remove vendoring patch." >&2
  exit 1
fi

echo "size-of remains a required transitive dependency."
