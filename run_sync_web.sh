#!/bin/zsh
set -euo pipefail
cd "/Users/stephengebbiaii/Projects/YNAB Amazon"
source .venv/bin/activate
python run_sync_web.py
