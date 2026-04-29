#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/categorize_products.sh <max_products>
# Optional env:
#   SUPPLIER=tigre|deca|votoran (default: tigre)
#   DRY_RUN=1 (default: 0)
#   OPENAI_MODEL=gpt-4o-mini
#   OPENAI_API_KEY=...

if [[ $# -lt 1 ]]; then
  echo "Uso: $0 <max_products>" >&2
  exit 1
fi

MAX_PRODUCTS="$1"
if ! [[ "${MAX_PRODUCTS}" =~ ^[0-9]+$ ]]; then
  echo "Erro: <max_products> deve ser inteiro >= 0" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

SUPPLIER="${SUPPLIER:-tigre}"
DRY_RUN="${DRY_RUN:-0}"

CMD=(
  python3 "${ROOT_DIR}/scripts/categorize_products.py"
  --supplier "${SUPPLIER}"
  --limit "${MAX_PRODUCTS}"
)

if [[ "${DRY_RUN}" == "1" ]]; then
  CMD+=(--dry-run)
fi

"${CMD[@]}"

