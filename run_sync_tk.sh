#!/bin/zsh
set -euo pipefail
cd "/Users/stephengebbiaii/Projects/YNAB Amazon"

if [[ -x ".venv/bin/python" ]]; then
  exec ".venv/bin/python" "run_sync_tk.py"
elif command -v python3 >/dev/null 2>&1; then
  exec python3 "run_sync_tk.py"
else
  echo "No Python interpreter found (.venv/bin/python or python3)." >&2
  exit 127
fi
