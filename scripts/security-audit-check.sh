#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Temporary allowlist for unsound advisories that are currently transitively pinned
# by upstream Starkware dependencies and not directly patchable in this workspace.
# Revisit each entry regularly and remove when upstream provides a compatible fix.
KNOWN_UNSOUND_IGNORES=(
  "RUSTSEC-2026-0002" # lru 0.12.5 via cairo-vm -> num-prime (^0.12.2 constraint)
)

IGNORE_ARGS=()
for advisory in "${KNOWN_UNSOUND_IGNORES[@]}"; do
  IGNORE_ARGS+=(--ignore "${advisory}")
done

(
  cd "${ROOT_DIR}"
  cargo audit --deny unsound "${IGNORE_ARGS[@]}"
)

echo "security audit passed (unsound advisories gated)."
