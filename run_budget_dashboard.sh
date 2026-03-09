#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if command -v uv >/dev/null 2>&1; then
  uv run 05_budget_dashboard.py "$@"
else
  python3 05_budget_dashboard.py "$@"
fi
