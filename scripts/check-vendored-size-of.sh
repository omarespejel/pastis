#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENDOR_DIR="${ROOT_DIR}/vendor/size-of"
MANIFEST_PATH="${VENDOR_DIR}/VENDOR_SHA256"

if [[ ! -f "${MANIFEST_PATH}" ]]; then
  echo "missing vendor integrity manifest: ${MANIFEST_PATH}" >&2
  exit 1
fi

if git -C "${ROOT_DIR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  FILES=$(
    git -C "${ROOT_DIR}" ls-files vendor/size-of \
      | awk '$0 != "vendor/size-of/VENDOR_SHA256"' \
      | LC_ALL=C sort
  )
else
  FILES=$(
    find "${VENDOR_DIR}" -type f ! -name 'VENDOR_SHA256' \
      | sed "s#^${ROOT_DIR}/##" \
      | LC_ALL=C sort
  )
fi

if [[ -z "${FILES}" ]]; then
  echo "no vendored files found in ${VENDOR_DIR}" >&2
  exit 1
fi

ACTUAL_HASH=$(
  while IFS= read -r relative_path; do
    shasum -a 256 "${ROOT_DIR}/${relative_path}"
  done <<< "${FILES}" \
    | shasum -a 256 \
    | awk '{print $1}'
)
EXPECTED_HASH="$(tr -d '[:space:]' < "${MANIFEST_PATH}")"

if [[ "${ACTUAL_HASH}" != "${EXPECTED_HASH}" ]]; then
  echo "vendored size-of integrity check failed" >&2
  echo "expected: ${EXPECTED_HASH}" >&2
  echo "actual:   ${ACTUAL_HASH}" >&2
  echo "if changes are intentional, update ${MANIFEST_PATH}" >&2
  exit 1
fi

echo "vendored size-of integrity verified: ${ACTUAL_HASH}"
