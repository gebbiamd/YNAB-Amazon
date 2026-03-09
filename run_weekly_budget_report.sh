#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if command -v uv >/dev/null 2>&1; then
  if [[ -n "${REPORT_EMAIL_TO:-}" ]]; then
    uv run 04_weekly_budget_report.py --email-to "${REPORT_EMAIL_TO}" "$@"
  else
    uv run 04_weekly_budget_report.py "$@"
  fi
else
  if [[ -n "${REPORT_EMAIL_TO:-}" ]]; then
    python3 04_weekly_budget_report.py --email-to "${REPORT_EMAIL_TO}" "$@"
  else
    python3 04_weekly_budget_report.py "$@"
  fi
fi
