#!/usr/bin/env python3
import argparse
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib import error, request


def load_dotenv(path: str = ".env"):
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def ynab_api(token: str, url: str, method: str = "GET", payload: dict | None = None) -> dict:
    body = None
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"YNAB API error {e.code}: {detail}") from e


def env_first(*names: str, default: str | None = None) -> str | None:
    for name in names:
        val = os.getenv(name)
        if val:
            return val
    return default


def list_recent_amazon_transactions(token: str, budget_id: str, days_back: int) -> list[dict]:
    since = (datetime.now(UTC) - timedelta(days=days_back)).date().isoformat()
    url = f"https://api.ynab.com/v1/budgets/{budget_id}/transactions?since_date={since}"
    data = ynab_api(token, url)
    txs = data.get("data", {}).get("transactions", []) or []
    out = []
    for t in txs:
        payee = (t.get("payee_name") or "").lower()
        if "amazon" in payee and not t.get("deleted"):
            out.append(t)
    out.sort(key=lambda x: (x.get("date") or "", x.get("id") or ""), reverse=True)
    return out


def update_memo(token: str, budget_id: str, tx_id: str, memo: str):
    url = f"https://api.ynab.com/v1/budgets/{budget_id}/transactions/{tx_id}"
    payload = {"transaction": {"id": tx_id, "memo": memo}}
    ynab_api(token, url, method="PUT", payload=payload)


def main():
    parser = argparse.ArgumentParser(description="Write newline-format probe memos into recent Amazon transactions.")
    parser.add_argument("--apply", action="store_true", help="Actually write probe memos.")
    parser.add_argument("--restore", action="store_true", help="Restore memos from backup file.")
    parser.add_argument("--days-back", type=int, default=30, help="How far back to look for candidate transactions.")
    parser.add_argument("--backup-file", default="logs/newline_probe_backup.json", help="Backup file path.")
    args = parser.parse_args()

    load_dotenv()
    token = env_first("ynab_api_key", "YNAB_API_KEY")
    budget_id = env_first("ynab_budget_id", "YNAB_BUDGET_ID")
    if not token or not budget_id:
        raise SystemExit("Missing YNAB credentials in .env (ynab_api_key / ynab_budget_id).")

    backup_path = Path(args.backup_file)

    if args.restore:
        if not backup_path.exists():
            raise SystemExit(f"No backup file found: {backup_path}")
        data = json.loads(backup_path.read_text(encoding="utf-8"))
        restored = 0
        for row in data.get("transactions", []):
            update_memo(token, budget_id, row["id"], row.get("original_memo") or "")
            restored += 1
        print(f"Restored memos: {restored}")
        return

    variants = [
        ("LF", "Probe A\nSecond line\nThird line"),
        ("CRLF", "Probe A\r\nSecond line\r\nThird line"),
        ("CR", "Probe A\rSecond line\rThird line"),
        ("LS", "Probe A\u2028Second line\u2028Third line"),
        ("PS", "Probe A\u2029Second line\u2029Third line"),
    ]

    txs = list_recent_amazon_transactions(token, budget_id, args.days_back)
    if len(txs) < len(variants):
        raise SystemExit(
            f"Need at least {len(variants)} recent Amazon transactions; found {len(txs)} in last {args.days_back} days."
        )

    chosen = txs[: len(variants)]
    print("Selected transactions:")
    for i, tx in enumerate(chosen):
        print(f"{i+1}. {tx.get('date')} | {tx.get('amount',0)/1000:.2f} | {tx.get('id')}")

    backup = {
        "created_at": datetime.now().isoformat(),
        "variants": [v[0] for v in variants],
        "transactions": [
            {
                "id": tx.get("id"),
                "date": tx.get("date"),
                "amount": tx.get("amount"),
                "payee_name": tx.get("payee_name"),
                "original_memo": tx.get("memo"),
                "variant": variants[i][0],
            }
            for i, tx in enumerate(chosen)
        ],
    }

    print("\nPlanned probe memo writes:")
    for i, tx in enumerate(chosen):
        label, text = variants[i]
        print(f"- {tx.get('id')} <= [{label}] {text!r}")

    if not args.apply:
        print("\nDry run only. Re-run with --apply to write probe memos.")
        return

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.write_text(json.dumps(backup, ensure_ascii=False, indent=2), encoding="utf-8")

    for i, tx in enumerate(chosen):
        label, text = variants[i]
        payload = f"[{label}] {text}"
        update_memo(token, budget_id, tx["id"], payload)

    print(f"\nApplied probe memos to {len(chosen)} transactions.")
    print(f"Backup saved to: {backup_path}")
    print("After checking YNAB, restore originals with: python probe_memo_newlines.py --restore")


if __name__ == "__main__":
    main()
