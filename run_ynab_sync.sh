#!/bin/zsh
set -euo pipefail

PROJECT_DIR="/Users/stephengebbiaii/Projects/YNAB Amazon"
LOG_DIR="$PROJECT_DIR/logs"

mkdir -p "$LOG_DIR"

cd "$PROJECT_DIR"
source .venv/bin/activate

# Automated run applies updates to YNAB.
python 03_sync_amazon_to_ynab.py --months-back 1 --coverage gaps --depth deep --apply
