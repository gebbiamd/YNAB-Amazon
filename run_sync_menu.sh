#!/bin/zsh
set -euo pipefail

PROJECT_DIR="/Users/stephengebbiaii/Projects/YNAB Amazon"
cd "$PROJECT_DIR"
source .venv/bin/activate

popup_mode="false"
if [[ "${1:-}" == "--popup" ]]; then
  popup_mode="true"
fi

print_menu() {
  command -v clear >/dev/null 2>&1 && clear
  echo
  echo "========================================"
  echo "          YNAB Amazon Sync Menu         "
  echo "========================================"
  echo
  echo "  1) Dry-run  | 1 month  | gaps | normal"
  echo "  2) Dry-run  | 6 months | all  | deep"
  echo "  3) Apply    | 1 month  | gaps | normal"
  echo "  4) Apply    | 6 months | all  | deep"
  echo "  5) Custom"
  echo
}

if [[ "$popup_mode" == "true" ]]; then
  runmode="$(osascript <<'APPLESCRIPT'
set options to {"Dry-run", "Apply"}
set selected to choose from list options with title "YNAB Amazon Sync" with prompt "Step 1 of 5: Choose run mode" default items {"Dry-run"}
if selected is false then
  return "CANCEL"
end if
return item 1 of selected
APPLESCRIPT
)"
  if [[ "$runmode" == "CANCEL" ]]; then
    echo "Cancelled."
    exit 0
  fi

  months="$(osascript <<'APPLESCRIPT'
set options to {"1", "3", "6"}
set selected to choose from list options with title "YNAB Amazon Sync" with prompt "Step 2 of 5: Choose months back" default items {"1"}
if selected is false then
  return "CANCEL"
end if
return item 1 of selected
APPLESCRIPT
)"
  if [[ "$months" == "CANCEL" ]]; then
    echo "Cancelled."
    exit 0
  fi

  coverage="$(osascript <<'APPLESCRIPT'
set options to {"gaps", "all"}
set selected to choose from list options with title "YNAB Amazon Sync" with prompt "Step 3 of 5: Coverage" default items {"gaps"}
if selected is false then
  return "CANCEL"
end if
return item 1 of selected
APPLESCRIPT
)"
  if [[ "$coverage" == "CANCEL" ]]; then
    echo "Cancelled."
    exit 0
  fi

  depth="$(osascript <<'APPLESCRIPT'
set options to {"normal", "deep"}
set selected to choose from list options with title "YNAB Amazon Sync" with prompt "Step 4 of 5: Depth" default items {"normal"}
if selected is false then
  return "CANCEL"
end if
return item 1 of selected
APPLESCRIPT
)"
  if [[ "$depth" == "CANCEL" ]]; then
    echo "Cancelled."
    exit 0
  fi

  confirm="$(osascript <<APPLESCRIPT
display dialog "Step 5 of 5: Confirm run\\n\\nMode: $runmode\\nMonths: $months\\nCoverage: $coverage\\nDepth: $depth" buttons {"Cancel", "Run"} default button "Run" with title "YNAB Amazon Sync"
return button returned of result
APPLESCRIPT
)"
  if [[ "$confirm" != "Run" ]]; then
    echo "Cancelled."
    exit 0
  fi

  choice="__popup_done__"
else
  print_menu
  read "choice?Select option [1-5]: "
fi

months="1"
coverage="gaps"
depth="normal"
apply_flag=""

case "$choice" in
  __popup_done__)
    if [[ "${runmode:l}" == "apply" ]]; then
      apply_flag="--apply"
    fi
    ;;
  1)
    ;;
  2)
    months="6"
    coverage="all"
    depth="deep"
    ;;
  3)
    apply_flag="--apply"
    ;;
  4)
    months="6"
    coverage="all"
    depth="deep"
    apply_flag="--apply"
    ;;
  5)
    echo
    read "months?Months back [1/3/6]: "
    read "coverage?Coverage [gaps/all]: "
    read "depth?Depth [normal/deep]: "
    read "runmode?Mode [dry/apply]: "
    if [[ "${runmode:l}" == "apply" ]]; then
      apply_flag="--apply"
    fi
    ;;
  *)
    echo "Invalid choice."
    exit 1
    ;;
esac

echo
echo "----------------------------------------"
echo "Running:"
echo "python 03_sync_amazon_to_ynab.py --months-back $months --coverage $coverage --depth $depth $apply_flag"
echo "----------------------------------------"
echo

python 03_sync_amazon_to_ynab.py --months-back "$months" --coverage "$coverage" --depth "$depth" ${apply_flag}
