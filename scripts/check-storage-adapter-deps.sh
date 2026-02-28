#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TREE_OUTPUT="$(
  cd "${ROOT_DIR}"
  cargo tree -p starknet-node-storage --features apollo-adapter -e normal
)"

if ! grep -q "apollo_storage v" <<< "${TREE_OUTPUT}"; then
  echo "apollo_storage dependency missing from apollo-adapter build." >&2
  exit 1
fi

if grep -q "papyrus_storage v" <<< "${TREE_OUTPUT}"; then
  echo "archived papyrus_storage detected in apollo-adapter dependency graph." >&2
  exit 1
fi

if ! grep -q "sequencer?rev=" <<< "${TREE_OUTPUT}"; then
  echo "apollo_storage is not sourced from pinned starkware-libs/sequencer revision." >&2
  exit 1
fi

echo "apollo-adapter dependency graph is pinned to sequencer and free of papyrus_storage."
