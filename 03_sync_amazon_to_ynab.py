import argparse
import base64
import hashlib
import json
import os
import re
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib import error, parse, request

from bs4 import BeautifulSoup
from google.auth.exceptions import RefreshError, TransportError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
DEFAULT_QUERY = (
    'in:"YNAB Amazon" '
    '(from:amazon OR from:order-update@amazon.com OR subject:("order" OR "shipped" OR "delivered" OR "receipt")) '
    "newer_than:30d"
)
CACHE_VERSION = "v2"
SPLIT_PARENT_MEMO = "[Split Charge]"


class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data: str):
        for s in self.streams:
            s.write(data)
        return len(data)

    def flush(self):
        for s in self.streams:
            s.flush()


def default_log_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path("logs") / f"ynab_sync_{stamp}.log"


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")


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


def env_first(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def env_bool(*names: str, default: bool = False) -> bool:
    value = env_first(*names)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "y"}


def get_gmail_service():
    creds = None
    token_path = Path("token.json")
    creds_path = Path("credentials.json")

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except (RefreshError, TransportError):
                # Token refresh can fail after revocation/expiry or transient OAuth
                # transport issues. Fall back to interactive re-auth.
                creds = None
        else:
            pass

        if not creds or not creds.valid:
            if not creds_path.exists():
                raise FileNotFoundError("Missing credentials.json in this folder.")
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def _decode_base64url(data: str) -> bytes:
    return base64.urlsafe_b64decode(data.encode("utf-8"))


def extract_best_body_text(payload: dict) -> str:
    def walk_parts(part):
        mime = part.get("mimeType", "")
        body = part.get("body", {}) or {}
        data = body.get("data")

        if data and mime in ("text/html", "text/plain"):
            try:
                raw = _decode_base64url(data)
                text = raw.decode("utf-8", errors="replace")
            except Exception:
                return None

            if mime == "text/html":
                soup = BeautifulSoup(text, "html.parser")
                return soup.get_text("\n", strip=True)
            return text.strip()

        for sub in part.get("parts", []) or []:
            out = walk_parts(sub)
            if out:
                return out
        return None

    return walk_parts(payload) or ""


def extract_headers(payload: dict) -> dict:
    out = {}
    for h in payload.get("headers", []) or []:
        name = h.get("name", "")
        value = h.get("value", "")
        if name and value:
            out[name.lower()] = value
    return out


def parse_order_and_amounts(text: str):
    order_match = re.search(r"\b\d{3}-\d{7}-\d{7}\b", text)
    order_id = order_match.group(0) if order_match else None

    amounts: list[str] = []
    patterns = [
        r"(Order\s+Summary|Order\s+Total|Total\s+before\s+tax|Total\s+for\s+this\s+order|Grand\s+Total|Total)\s*[:\-]?\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{2})?)",
        r"(?:charged|charge|order of|payment)\s*[:\-]?\s*\$?\s*([0-9][0-9,]*(?:\.[0-9]{2})?)",
        r"(Order\s+Total|Total\s+for\s+this\s+order|Grand\s+Total|Total)\s*[:\-]?\s*\$?\s*([0-9]+(?:\.[0-9]{2})?)",
        r"\$\s*([0-9]+(?:\.[0-9]{2})?)\s*(?:USD)?\s*(?:Order\s+Total|Total)",
    ]
    for pattern in patterns:
        for m in re.finditer(pattern, text, flags=re.IGNORECASE):
            amount = m.group(m.lastindex).replace(",", "")
            if amount not in amounts:
                amounts.append(amount)

    # Also collect generic money mentions to capture split-charge amounts that may
    # not appear under standard "Order Total" labels.
    for m in re.finditer(r"\$([0-9][0-9,]*(?:\.[0-9]{2}))", text):
        amount = m.group(1).replace(",", "")
        if amount not in amounts:
            amounts.append(amount)

    return order_id, amounts[:20]


def guess_category(subject: str, body: str) -> str:
    hay = f"{subject}\n{body}".lower()
    if any(k in hay for k in ("grocery", "food", "kitchen")):
        return "Groceries"
    if any(k in hay for k in ("book", "kindle")):
        return "Books"
    if any(k in hay for k in ("pet", "dog", "cat")):
        return "Pet Supplies"
    if any(k in hay for k in ("vitamin", "supplement", "medicine")):
        return "Health"
    if any(k in hay for k in ("cable", "usb", "charger", "headphone", "monitor")):
        return "Electronics"
    return "Shopping"


def to_milliunits(total_str: str) -> int | None:
    if not total_str:
        return None
    try:
        amount = Decimal(total_str)
    except InvalidOperation:
        return None
    return int(amount * 1000)


def ynab_request(method: str, path: str, token: str, payload: dict | None = None):
    url = f"https://api.ynab.com/v1{path}"
    body = None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    max_attempts = 10
    for attempt in range(1, max_attempts + 1):
        req = request.Request(url, data=body, headers=headers, method=method)
        try:
            with request.urlopen(req) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            if e.code == 429 and attempt < max_attempts:
                retry_after = e.headers.get("Retry-After") if e.headers else None
                sleep_seconds = None
                if retry_after:
                    try:
                        sleep_seconds = float(retry_after)
                    except ValueError:
                        sleep_seconds = None
                if sleep_seconds is None:
                    sleep_seconds = min(90, 2 ** attempt)
                print(f"WARN YNAB rate limit (429). Retrying in {sleep_seconds:.1f}s (attempt {attempt}/{max_attempts})...")
                time.sleep(sleep_seconds)
                continue
            raise RuntimeError(f"YNAB API error {e.code}: {detail}") from e


def load_ynab_transactions(token: str, budget_id: str, since_date: str, account_id: str | None):
    q = parse.urlencode({"since_date": since_date})
    if account_id:
        path = f"/budgets/{budget_id}/accounts/{account_id}/transactions?{q}"
    else:
        path = f"/budgets/{budget_id}/transactions?{q}"
    data = ynab_request("GET", path, token)
    return data.get("data", {}).get("transactions", []) or []


def load_ynab_transaction(token: str, budget_id: str, tx_id: str) -> dict:
    data = ynab_request("GET", f"/budgets/{budget_id}/transactions/{tx_id}", token)
    return data.get("data", {}).get("transaction", {}) or {}


def load_ynab_category_map(token: str, budget_id: str) -> dict[str, str]:
    data = ynab_request("GET", f"/budgets/{budget_id}/categories", token)
    groups = data.get("data", {}).get("category_groups", []) or []
    out: dict[str, str] = {}
    for g in groups:
        for c in g.get("categories", []) or []:
            if c.get("deleted"):
                continue
            name = (c.get("name") or "").strip()
            cid = c.get("id")
            if name and cid:
                out[name.lower()] = cid
    return out


def update_ynab_transaction(
    token: str,
    budget_id: str,
    tx_id: str,
    memo: str,
    new_payee_name: str | None,
    split_subtransactions: list[dict] | None = None,
    new_category_id: str | None = None,
):
    path = f"/budgets/{budget_id}/transactions/{tx_id}"
    transaction = {"memo": memo}
    if new_payee_name:
        transaction["payee_name"] = new_payee_name
    if new_category_id and not split_subtransactions:
        transaction["category_id"] = new_category_id
    if split_subtransactions is not None:
        transaction["subtransactions"] = split_subtransactions
    payload = {"transaction": transaction}
    ynab_request("PUT", path, token, payload)


def flush_batched_ynab_updates(token: str, budget_id: str, updates: list[dict], batch_size: int = 25) -> int:
    if not updates:
        return 0

    applied = 0
    for i in range(0, len(updates), batch_size):
        chunk = updates[i:i + batch_size]
        try:
            ynab_request("PATCH", f"/budgets/{budget_id}/transactions", token, {"transactions": chunk})
            applied += len(chunk)
        except Exception as e:
            print(f"WARN Batch update failed for {len(chunk)} txns: {e}. Falling back to per-transaction updates.")
            for tx_payload in chunk:
                tx_id = tx_payload.get("id")
                try:
                    ynab_request("PATCH", f"/budgets/{budget_id}/transactions", token, {"transactions": [tx_payload]})
                    applied += 1
                except Exception as ie:
                    print(f"WARN Apply failed for tx {tx_id}: {ie}")
    return applied


def force_unsplit_transaction(
    token: str,
    budget_id: str,
    tx_detail: dict,
    memo: str,
    new_payee_name: str | None,
    new_category_id: str | None,
):
    tx_id = tx_detail.get("id")
    if not tx_id:
        return

    payload_tx = {
        "account_id": tx_detail.get("account_id"),
        "date": tx_detail.get("date"),
        "amount": tx_detail.get("amount"),
        "cleared": tx_detail.get("cleared"),
        "approved": tx_detail.get("approved"),
        "flag_color": tx_detail.get("flag_color"),
        "import_id": tx_detail.get("import_id"),
        "memo": memo,
        "subtransactions": [],
    }

    if new_payee_name:
        payload_tx["payee_name"] = new_payee_name
    elif tx_detail.get("payee_id"):
        payload_tx["payee_id"] = tx_detail.get("payee_id")
    elif tx_detail.get("payee_name"):
        payload_tx["payee_name"] = tx_detail.get("payee_name")

    if new_category_id:
        payload_tx["category_id"] = new_category_id
    elif tx_detail.get("category_id"):
        payload_tx["category_id"] = tx_detail.get("category_id")

    payload_tx = {k: v for k, v in payload_tx.items() if v is not None}
    ynab_request("PUT", f"/budgets/{budget_id}/transactions/{tx_id}", token, {"transaction": payload_tx})


def fetch_amazon_candidates(service, query: str, max_results: int):
    messages = []
    page_token = None
    while len(messages) < max_results:
        page_size = min(100, max_results - len(messages))
        call = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=page_size,
            pageToken=page_token,
        )
        result = call.execute()
        batch = result.get("messages", []) or []
        messages.extend(batch)
        page_token = result.get("nextPageToken")
        if not page_token or not batch:
            break

    rows = []

    for m in messages:
        msg = service.users().messages().get(userId="me", id=m["id"], format="full").execute()
        payload = msg.get("payload", {}) or {}
        headers = extract_headers(payload)
        subject = headers.get("subject", "")
        body = extract_best_body_text(payload)
        order_id, total_candidates = parse_order_and_amounts(body)
        amount_options_milli = [to_milliunits(x) for x in total_candidates if to_milliunits(x) is not None]
        amount_options_milli = [x for x in amount_options_milli if x is not None]
        amount_milli = amount_options_milli[0] if amount_options_milli else None
        internal_ms = msg.get("internalDate")
        if internal_ms:
            email_dt = datetime.fromtimestamp(int(internal_ms) / 1000, UTC)
            email_date = email_dt.date().isoformat()
        else:
            email_dt = None
            email_date = None

        if amount_milli is None or not email_date:
            continue

        rows.append(
            {
                "order_id": order_id,
                "amount_milli": amount_milli,
                "amount_options_milli": amount_options_milli[:8],
                "email_date": email_date,
                "email_dt": email_dt,
                "subject": subject,
                "body_excerpt": body[:5000],
                "category_guess": guess_category(subject, body),
            }
        )
    return rows


def search_candidate_for_transaction(
    service,
    tx: dict,
    day_window: int,
    per_query_scan_limit: int = 40,
    max_delta_days: int = 21,
    query_window_days: int = 21,
    max_amount_gap_milli: int = 200,
) -> dict | None:
    tx_amount = tx.get("amount")
    tx_date = tx.get("date")
    if not isinstance(tx_amount, int) or not tx_date:
        return None

    target_amount_milli = abs(tx_amount)
    target_amount_text = f"{target_amount_milli / 1000:.2f}"
    tx_dt = datetime.fromisoformat(tx_date).date()
    # Account for card posting lag vs email sent date (common +/-1 day drift).
    reference_dates = [tx_dt, tx_dt - timedelta(days=1), tx_dt + timedelta(days=1)]
    after_dt = (tx_dt - timedelta(days=query_window_days)).strftime("%Y/%m/%d")
    before_dt = (tx_dt + timedelta(days=query_window_days)).strftime("%Y/%m/%d")

    target_amount_short = target_amount_text.rstrip("0").rstrip(".")

    queries = [
        f'from:amazon "{target_amount_text}" after:{after_dt} before:{before_dt}',
        f'from:order-update@amazon.com "{target_amount_text}" after:{after_dt} before:{before_dt}',
        f'from:auto-confirm@amazon.com "{target_amount_text}" after:{after_dt} before:{before_dt}',
        f'from:shipment-tracking@amazon.com "{target_amount_text}" after:{after_dt} before:{before_dt}',
        f'subject:(order OR shipped OR delivered OR receipt OR arriving) "{target_amount_text}" after:{after_dt} before:{before_dt}',
        f'from:(amazon.com) "{target_amount_text}" after:{after_dt} before:{before_dt}',
        f'from:(amazon.com) "{target_amount_short}" after:{after_dt} before:{before_dt}',
        f'("amazon" OR "order-update@amazon.com") after:{after_dt} before:{before_dt}',
        f'"{target_amount_text}" after:{after_dt} before:{before_dt}',
    ]

    best = None
    best_delta = None
    best_amount_gap = None
    best_quality = None
    seen_message_ids: set[str] = set()

    for q in queries:
        page_token = None
        seen = 0
        while seen < per_query_scan_limit:
            result = service.users().messages().list(
                userId="me",
                q=q,
                maxResults=30,
                pageToken=page_token,
            ).execute()
            messages = result.get("messages", []) or []
            if not messages:
                break
            for m in messages:
                msg_id = m.get("id")
                if not msg_id or msg_id in seen_message_ids:
                    continue
                seen_message_ids.add(msg_id)
                seen += 1
                msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()
                payload = msg.get("payload", {}) or {}
                headers = extract_headers(payload)
                subject = headers.get("subject", "")
                body = extract_best_body_text(payload)
                snippet = msg.get("snippet", "") or ""
                order_id, total_candidates = parse_order_and_amounts(body)
                amount_options_milli = [to_milliunits(x) for x in total_candidates if to_milliunits(x) is not None]
                amount_options_milli = [x for x in amount_options_milli if x is not None]
                parsed_contains_exact = target_amount_milli in amount_options_milli
                has_amount_text = (
                    (f"${target_amount_text}" in body)
                    or (f"${target_amount_text}" in snippet)
                    or (f"${target_amount_short}" in body)
                    or (f"${target_amount_short}" in snippet)
                    or (target_amount_text in subject)
                    or (target_amount_text in body)
                    or (target_amount_text in snippet)
                    or (target_amount_short in subject)
                    or (target_amount_short in body)
                    or (target_amount_short in snippet)
                )
                if not amount_options_milli and has_amount_text:
                    # Fallback when amount is present in snippet/body but parser misses structured totals.
                    amount_options_milli = [target_amount_milli]
                if not amount_options_milli:
                    continue

                internal_ms = msg.get("internalDate")
                if not internal_ms:
                    continue
                email_dt = datetime.fromtimestamp(int(internal_ms) / 1000, UTC)
                email_date = email_dt.date()
                delta = min(abs((email_date - ref).days) for ref in reference_dates)
                if delta > max(day_window, max_delta_days):
                    continue

                # Prefer exact amount, otherwise allow near-match up to 20 cents.
                min_gap = min(abs(a - target_amount_milli) for a in amount_options_milli)
                if min_gap > max_amount_gap_milli:
                    continue

                # Quality ranking: parsed exact > parsed near > text-only fallback.
                if parsed_contains_exact:
                    quality = 0
                elif total_candidates:
                    quality = 1
                else:
                    quality = 2

                candidate = {
                    "order_id": order_id,
                    "amount_milli": target_amount_milli,
                    "amount_options_milli": amount_options_milli[:8],
                    "email_date": email_date.isoformat(),
                    "email_dt": email_dt,
                    "subject": subject,
                    "body_excerpt": body[:5000],
                    "category_guess": guess_category(subject, body),
                    "match_confidence": "low" if quality == 2 else "high",
                }

                if best is None:
                    best = candidate
                    best_delta = delta
                    best_amount_gap = min_gap
                    best_quality = quality
                else:
                    # Rank by quality first, then amount gap, then date distance.
                    if (
                        quality < best_quality
                        or (quality == best_quality and min_gap < best_amount_gap)
                        or (quality == best_quality and min_gap == best_amount_gap and delta < best_delta)
                    ):
                        best = candidate
                        best_delta = delta
                        best_amount_gap = min_gap
                        best_quality = quality

            page_token = result.get("nextPageToken")
            if not page_token:
                break

        if best:
            return best

    return None


def find_best_match(
    candidate: dict,
    ynab_transactions: list[dict],
    day_window: int,
    used_tx_ids: set[str],
    allow_relaxed: bool = True,
):
    c_date = datetime.fromisoformat(candidate["email_date"]).date()
    option_amounts = candidate.get("amount_options_milli") or [candidate.get("amount_milli")]
    target_amounts = {-abs(a) for a in option_amounts if isinstance(a, int)}
    best = None
    best_delta = None

    # Pass 1: strict window
    for tx in ynab_transactions:
        tx_id = tx.get("id")
        tx_amount = tx.get("amount")
        tx_date_raw = tx.get("date")
        if not tx_id or tx_id in used_tx_ids:
            continue
        if tx_amount not in target_amounts or not tx_date_raw:
            continue
        tx_date = datetime.fromisoformat(tx_date_raw).date()
        delta = abs((tx_date - c_date).days)
        if delta > day_window:
            continue
        if best is None or delta < best_delta:
            best = tx
            best_delta = delta
    if best:
        return best

    if not allow_relaxed:
        return None

    # Pass 2: relaxed window for delayed posting edge cases (deep mode only).
    relaxed_window = max(day_window, 7)
    for tx in ynab_transactions:
        tx_id = tx.get("id")
        tx_amount = tx.get("amount")
        tx_date_raw = tx.get("date")
        if not tx_id or tx_id in used_tx_ids:
            continue
        if tx_amount not in target_amounts or not tx_date_raw:
            continue
        tx_date = datetime.fromisoformat(tx_date_raw).date()
        delta = abs((tx_date - c_date).days)
        if delta > relaxed_window:
            continue
        if best is None or delta < best_delta:
            best = tx
            best_delta = delta
    return best


def ai_summarize_candidate(candidate: dict, api_key: str, model: str, charge_amount: str | None = None):
    url = "https://api.openai.com/v1/chat/completions"
    prompt = (
        "You classify Amazon purchases for personal budgeting. "
        "Return strict JSON with keys: category, items, memo. "
        "items must be an array of objects: {name, amount, category}. amount is a number in USD when visible, else null. "
        "category should be a short label like Groceries, Skin Care, Household, Zoe, etc. "
        "Item names must be very concise (2-4 words). Omit non-essential brands and extra descriptors. "
        "Keep a brand only if it is identity-critical (for example PlaqueOff). "
        "Prefer post-tax charged item amounts when visible. "
        "If multiple products are present, assign categories PER ITEM and do not collapse all items into one category. "
        "If multiple items are returned, include amount for every item whenever possible. "
        "If charge_amount is provided, prioritize only the subset of items likely included in that specific charge."
    )
    user_content = {
        "subject": candidate.get("subject", ""),
        "order_id": candidate.get("order_id"),
        "email_date": candidate.get("email_date"),
        "charge_amount": charge_amount,
        "email_text_excerpt": candidate.get("body_excerpt", ""),
    }
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(user_content)},
        ],
        "response_format": {"type": "json_object"},
    }

    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method="POST",
    )
    try:
        with request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API error {e.code}: {detail}") from e

    content = data["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    category = normalize_category((parsed.get("category") or "Shopping").strip()[:48])
    memo = normalize_summary((parsed.get("memo") or "Amazon purchase").strip()[:180])
    items = normalize_items(parsed.get("items"))
    return category, memo, items


def normalize_category(category: str) -> str:
    clean = re.sub(r"\s+", " ", category).strip()
    if not clean:
        return "Shopping"
    return clean.title()


def map_to_user_category(raw_category: str) -> str:
    c = normalize_category(raw_category).lower()
    mapping = {
        "groceries": "Groceries",
        "snacks": "Groceries",
        "eating out": "Eating Out",
        "shopping": "Shopping",
        "transportation": "Transportation",
        "zoe": "Zoe",
        "med school costs": "Med School Costs",
        "entertainment": "Entertainment",
        "hair": "Hair",
        "health": "Health/Skin Care",
        "skin care": "Health/Skin Care",
        "skincare": "Health/Skin Care",
        "health/skin care": "Health/Skin Care",
        "botox": "Botox",
        "supplements": "Supplements",
    }
    return mapping.get(c, "Shopping")


def normalize_summary(summary: str) -> str:
    clean = re.sub(r"\s+", " ", summary.replace("\n", " ")).strip()
    if not clean:
        return "Amazon purchase"
    # Add clearer separators when AI uses "and" for multiple items.
    if "," not in clean and ";" not in clean and "|" not in clean:
        clean = re.sub(r"\s+\band\b\s+", ", ", clean, flags=re.IGNORECASE)
    return clean


def normalize_items(raw_items) -> list[dict]:
    if not isinstance(raw_items, list):
        return []
    out = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        name = re.sub(r"\s+", " ", str(item.get("name") or "")).strip()
        if not name:
            continue
        name = simplify_item_name(name)
        category = normalize_category(str(item.get("category") or "").strip() or "Shopping")
        category = infer_item_category(name, category)
        amount = item.get("amount")
        amount_str = None
        if isinstance(amount, (int, float)):
            amount_str = f"${float(amount):.2f}"
        elif isinstance(amount, str):
            amt = amount.strip().replace("$", "")
            try:
                amount_str = f"${float(amt):.2f}"
            except ValueError:
                amount_str = None
        out.append({"name": name[:90], "amount": amount_str, "category": category})
    return out[:12]


def simplify_item_name(name: str) -> str:
    n = re.sub(r"\s+", " ", name).strip()
    if not n:
        return "Item"

    # Remove common brand/vendor prefixes that are usually noise in memos.
    n = re.sub(r"(?i)^(amazon basics?|amazon fresh|nutricost|the ordinary|now foods)\s+", "", n).strip()

    replacements = [
        (r"(?i)\breal techniques?\b", ""),
        (r"(?i)\beasy touch\b", ""),
        (r"(?i)\bproden plaqueoff powder for pets?\b", "PlaqueOff powder"),
        (r"(?i)\bproden plaqueoff\b", "PlaqueOff"),
        (r"(?i)\bmiracle complexion sponge\b", "makeup sponge"),
        (r"(?i)\binsulin syringes?\b", "insulin syringes"),
        (r"(?i)\b(?:nutricost\s+)?prebiotic fiber(?:\s+unflavored)?\b", "prebiotic fiber"),
        (r"(?i)\bprotein chewy bars?\b", "protein bars"),
        (r"(?i)\bprotein bars?\b.*$", "protein bars"),
    ]
    for pattern, repl in replacements:
        n = re.sub(pattern, repl, n).strip()

    # Remove parenthetical descriptors and unit/size tokens.
    n = re.sub(r"\([^)]*\)", "", n).strip()
    n = re.sub(r"\([^)]*$", "", n).strip()
    n = re.sub(r"(?i)\b\d+(?:\.\d+)?\s*(pounds?|lbs?|ounces?|oz|fl\s*oz|count|ct|pack|packs)\b", "", n).strip()
    n = re.sub(r"(?i)\b(pack of|count|ct|oz|fl oz|lb|lbs)\b.*$", "", n).strip()
    n = re.sub(r"(?i)[,\s]*&\s*$", "", n).strip()
    n = re.sub(
        r"(?i)\b(variety|assorted|organic|natural|premium|advanced|professional|daily|formula|unflavored|unscented)\b",
        "",
        n,
    ).strip()
    n = re.sub(r"(?i)\b(white|black|blue|red|green)\b", "", n).strip()
    n = re.sub(r"(?i)\b(for|with|and|by)\b", " ", n).strip()
    n = re.sub(r"\s{2,}", " ", n).strip(" -,:;")

    # If nothing useful remains after cleanup, fall back to a generic label.
    if not n:
        return "Item"

    # Keep memo items concise.
    words = n.split()
    if len(words) > 4:
        n = " ".join(words[:4])
    return n.title()


def infer_item_category(item_name: str, fallback_category: str) -> str:
    n = item_name.lower()
    if any(k in n for k in ("protein bar", "protein bars", "chewy bar", "chewy bars", "snack bar", "snack bars")):
        return map_to_user_category("groceries")
    if any(k in n for k in ("dog", "cat", "pet", "plaqueoff", "litter", "dog chew", "cat chew", "flea", "tick")):
        return map_to_user_category("zoe")
    if any(k in n for k in ("uber", "lyft", "gas", "parking", "transit", "metro")):
        return map_to_user_category("transportation")
    if any(k in n for k in ("restaurant", "doordash", "ubereats", "grubhub", "takeout")):
        return map_to_user_category("eating out")
    if any(k in n for k in ("botox", "dysport", "xeomin")):
        return map_to_user_category("botox")
    if any(k in n for k in ("biotin", "omega", "multivitamin", "probiotic", "prebiotic", "creatine")):
        return map_to_user_category("supplements")
    if any(k in n for k in ("serum", "cleanser", "moisturizer", "spf", "sunscreen", "retinol", "beauty", "skincare", "skin care")):
        return map_to_user_category("health/skin care")
    if any(k in n for k in ("syringe", "vitamin", "supplement", "medicine", "insulin", "medical")):
        return map_to_user_category("health/skin care")
    if any(k in n for k in ("snack", "protein bar", "bar", "food", "grocery", "coffee", "tea", "kitchen")):
        return map_to_user_category("groceries")
    if any(k in n for k in ("detergent", "cleaner", "dishwasher", "disposer", "trash bag", "paper towel", "toilet paper")):
        return map_to_user_category("shopping")
    return map_to_user_category(fallback_category)


def _amount_to_float(amount_text: str | None) -> float | None:
    if not amount_text:
        return None
    try:
        return float(amount_text.replace("$", "").strip())
    except ValueError:
        return None


def pick_items_for_charge(items: list[dict], tx_amount_milli: int, tolerance_milli: int = 250) -> list[dict]:
    # Keep only the item subset that best matches this charge amount, for split Amazon shipments.
    if not items or not isinstance(tx_amount_milli, int):
        return items

    target = abs(tx_amount_milli)
    priced: list[tuple[int, int]] = []
    for idx, item in enumerate(items):
        amount = _amount_to_float(item.get("amount"))
        if amount is None:
            continue
        priced.append((idx, int(round(amount * 1000))))

    if not priced:
        return items

    best_mask = 0
    best_gap = None
    best_count = None
    n = len(priced)
    for mask in range(1, 1 << n):
        subtotal = 0
        count = 0
        for bit in range(n):
            if mask & (1 << bit):
                subtotal += priced[bit][1]
                count += 1
        gap = abs(subtotal - target)
        # Prefer smaller gap; for near-ties, prefer fewer items.
        if (
            best_gap is None
            or gap < best_gap
            or (abs(gap - best_gap) <= 200 and (best_count is None or count < best_count))
        ):
            best_gap = gap
            best_mask = mask
            best_count = count
            if best_gap == 0:
                break

    # Guardrail: if a single dominant item is close enough to the charge, prefer it
    # over multi-item subsets to avoid cross-charge contamination.
    dominant_idx = None
    dominant_gap = None
    for idx, amount in priced:
        gap = abs(amount - target)
        # Strong preference for a single-item charge when one item explains most of the total.
        if amount >= int(target * 0.70) and gap <= max(2000, int(target * 0.25)):
            if dominant_gap is None or gap < dominant_gap:
                dominant_gap = gap
                dominant_idx = idx
    if dominant_idx is not None:
        return [items[dominant_idx]]

    # Allow wider tolerance for tax/shipping drift.
    effective_tolerance = max(tolerance_milli, int(target * 0.12))
    if best_gap is None or best_gap > effective_tolerance:
        return items

    chosen_indices = {priced[bit][0] for bit in range(n) if best_mask & (1 << bit)}
    selected = [item for idx, item in enumerate(items) if idx in chosen_indices]
    return selected or items


def compress_items_for_charge(items: list[dict], tx_amount_milli: int | None) -> list[dict]:
    # Merge duplicate item rows and scale totals to the exact charge amount.
    if not items:
        return items

    grouped: dict[tuple[str, str], dict] = {}
    unknown: list[dict] = []
    for item in items:
        name = (item.get("name") or "Item").strip()
        category = map_to_user_category(item.get("category") or "Shopping")
        amt = _amount_to_float(item.get("amount"))
        if amt is None:
            unknown.append({"name": name, "category": category, "amount": None})
            continue
        key = (name.lower(), category.lower())
        row = grouped.setdefault(key, {"name": name, "category": category, "amount_milli": 0})
        row["amount_milli"] += int(round(amt * 1000))

    target_abs = abs(tx_amount_milli) if isinstance(tx_amount_milli, int) and tx_amount_milli != 0 else None

    if not grouped:
        # If all amounts are missing, still assign amounts so memo output includes prices.
        if target_abs and unknown:
            per = target_abs // len(unknown)
            remainder = target_abs - (per * len(unknown))
            out_unknown = []
            for i, u in enumerate(unknown):
                amt = per + (remainder if i == 0 else 0)
                out_unknown.append({"name": u["name"], "category": u["category"], "amount": f"${amt / 1000:.2f}"})
            return out_unknown
        return unknown or items

    values = list(grouped.values())
    if target_abs and len(values) == 1:
        values[0]["amount_milli"] = target_abs
    elif target_abs:
        current = sum(v["amount_milli"] for v in values)
        if current > 0:
            scaled = [int(round(v["amount_milli"] * target_abs / current)) for v in values]
            delta = target_abs - sum(scaled)
            if scaled:
                idx = max(range(len(scaled)), key=lambda i: scaled[i])
                scaled[idx] += delta
            for i, v in enumerate(values):
                v["amount_milli"] = scaled[i]

    # If some items had unknown amounts, distribute remaining amount across them.
    if target_abs and unknown:
        known_total = sum(v["amount_milli"] for v in values)
        remaining = max(0, target_abs - known_total)
        if remaining > 0:
            per = remaining // len(unknown)
            remainder = remaining - (per * len(unknown))
            for i, u in enumerate(unknown):
                amt = per + (remainder if i == 0 else 0)
                values.append({"name": u["name"], "category": u["category"], "amount_milli": amt})

    out = []
    for v in values:
        out.append(
            {
                "name": v["name"],
                "category": v["category"],
                "amount": f"${v['amount_milli'] / 1000:.2f}",
            }
        )
    out.extend(unknown)
    return out


def choose_primary_category_id(items: list[dict], category_map: dict[str, str]) -> str | None:
    if not items:
        return None
    scores: dict[str, int] = {}
    for item in items:
        cat = map_to_user_category(item.get("category") or "Shopping")
        cid = category_map.get(cat.lower())
        if not cid:
            continue
        amt = _amount_to_float(item.get("amount"))
        weight = int(round(amt * 1000)) if amt is not None else 1
        scores[cid] = scores.get(cid, 0) + max(weight, 1)
    if not scores:
        return None
    return max(scores, key=scores.get)


def force_single_split_for_all_strategy(
    desired_splits: list[dict],
    split_strategy: str,
    tx_amount_milli: int | None,
    items: list[dict],
    category_map: dict[str, str],
) -> list[dict]:
    strategy = (split_strategy or "conservative").strip().lower()
    if strategy != "all" or desired_splits:
        return desired_splits
    if not isinstance(tx_amount_milli, int) or tx_amount_milli == 0:
        return desired_splits

    category_id = choose_primary_category_id(items, category_map) or category_map.get("shopping")
    if not category_id:
        return desired_splits

    first_name = (items[0].get("name") if items else None) or "Amazon Item"
    memo = format_item_for_memo({"name": first_name, "amount": f"${abs(tx_amount_milli) / 1000:.2f}"})
    return [{"amount": tx_amount_milli, "category_id": category_id, "memo": memo[:200]}]


def filter_transactions_by_payee(
    transactions: list[dict],
    target_payee_exact: str | None,
    contains_term: str,
    exclude_prime: bool,
    mode: str,
) -> tuple[list[dict], str]:
    mode_clean = (mode or "contains").strip().lower()
    if mode_clean == "exact" and target_payee_exact:
        filtered = [t for t in transactions if (t.get("payee_name") or "") == target_payee_exact]
        return filtered, f"exact:{target_payee_exact}"

    term = (contains_term or "amazon").strip().lower()
    filtered = []
    for t in transactions:
        payee = (t.get("payee_name") or "").strip().lower()
        if term and term not in payee:
            continue
        if exclude_prime and "prime" in payee:
            continue
        filtered.append(t)
    return filtered, f'contains:"{term}"{" excluding prime" if exclude_prime else ""}'


def is_transfer_like_transaction(tx: dict) -> bool:
    payee = (tx.get("payee_name") or "").strip().lower()
    if "transfer" in payee:
        return True
    if "payment" in payee:
        return True
    transfer_account = tx.get("transfer_account_id")
    if transfer_account:
        return True
    return False


def build_split_subtransactions(
    items: list[dict],
    tx_amount_milli: int,
    category_map: dict[str, str],
    allow_single_category: bool = False,
) -> list[dict]:
    # Build YNAB split rows from categorized item amounts.
    # Keep one memo item per split row (even within same category) to avoid packed memo strings.
    if not items or not isinstance(tx_amount_milli, int) or tx_amount_milli == 0:
        return []

    item_rows: list[dict] = []
    category_ids: set[str] = set()
    for item in items:
        amount = _amount_to_float(item.get("amount"))
        if amount is None:
            continue
        cat_name = map_to_user_category(item.get("category") or "Shopping")
        cid = category_map.get(cat_name.lower())
        if not cid:
            continue
        category_ids.add(cid)
        item_rows.append(
            {
                "category_id": cid,
                "amount_milli": int(round(amount * 1000)),
                "name": item.get("name") or "Item",
                "amount": item.get("amount"),
            }
        )

    # Usually split only when multiple categories are represented.
    # In force mode, allow itemized splits even within one category.
    if (not allow_single_category) and len(category_ids) < 2:
        return []

    target_abs = abs(tx_amount_milli)
    sum_abs = sum(v["amount_milli"] for v in item_rows)
    if sum_abs <= 0:
        return []

    # Accept moderate tax/shipping mismatch and allocate it proportionally.
    # This avoids dropping splits when Amazon itemized amounts don't perfectly match card charge totals.
    allowed_gap = max(200, int(target_abs * 0.25))  # at least $0.20 or up to 25%
    if abs(sum_abs - target_abs) > allowed_gap:
        return []

    sign = -1 if tx_amount_milli < 0 else 1

    # Proportional allocation so rows include tax/fees in ratio to item subtotal.
    scaled: list[int] = []
    for row in item_rows:
        scaled.append(int(round(row["amount_milli"] * target_abs / sum_abs)))

    # Reconcile rounding remainder to keep exact transaction total.
    scaled_total = sum(scaled)
    remainder = target_abs - scaled_total
    if remainder != 0:
        largest_idx = max(range(len(scaled)), key=lambda i: scaled[i])
        scaled[largest_idx] += remainder

    splits = []
    for idx, row in enumerate(item_rows):
        memo = format_item_for_memo({"name": row["name"], "amount": f"${scaled[idx] / 1000:.2f}"})
        splits.append(
            {
                "amount": sign * scaled[idx],
                "category_id": row["category_id"],
                "memo": memo[:200],
            }
        )

    # Final hard reconciliation: split sum must equal the transaction amount exactly.
    current_total = sum(s["amount"] for s in splits)
    delta = tx_amount_milli - current_total
    if splits and delta != 0:
        splits[0]["amount"] += delta
    return splits


def should_use_splits_for_transaction(
    items: list[dict],
    tx_amount_milli: int | None,
    category_map: dict[str, str],
    split_strategy: str,
) -> bool:
    strategy = (split_strategy or "conservative").strip().lower()
    if strategy in {"off", "none", "false", "0"}:
        return False
    if strategy == "all":
        return isinstance(tx_amount_milli, int) and tx_amount_milli != 0

    # Conservative mode: split only when itemization is small and high-confidence.
    if not isinstance(tx_amount_milli, int) or tx_amount_milli == 0:
        return False
    if len(items) < 2 or len(items) > 4:
        return False

    parsed_amounts: list[int] = []
    category_ids: set[str] = set()
    for item in items:
        amount = _amount_to_float(item.get("amount"))
        if amount is None:
            return False
        parsed_amounts.append(int(round(abs(amount) * 1000)))
        cat_name = map_to_user_category(item.get("category") or "Shopping")
        cid = category_map.get(cat_name.lower())
        if not cid:
            return False
        category_ids.add(cid)

    # Splits are most useful for cross-category purchases.
    if len(category_ids) < 2:
        return False

    target_abs = abs(tx_amount_milli)
    sum_abs = sum(parsed_amounts)
    if sum_abs <= 0:
        return False

    # Stricter than split builder guardrails.
    # Require item subtotal to be close to final charge before enabling splits.
    allowed_gap = max(150, int(target_abs * 0.08))  # >= $0.15 or <= 8%
    return abs(sum_abs - target_abs) <= allowed_gap


def normalize_existing_subtransactions(tx: dict) -> list[dict]:
    out = []
    for s in tx.get("subtransactions", []) or []:
        out.append(
            {
                "amount": s.get("amount"),
                "category_id": s.get("category_id"),
                "memo": (s.get("memo") or "").strip(),
            }
        )
    return sorted(out, key=lambda x: (x.get("category_id") or "", x.get("amount") or 0, x.get("memo") or ""))


def split_changed(tx: dict, desired_splits: list[dict]) -> bool:
    current = normalize_existing_subtransactions(tx)
    if not desired_splits:
        # If transaction is currently split but desired is non-split, this is a change.
        return len(current) > 0
    target = sorted(
        [{"amount": s["amount"], "category_id": s["category_id"], "memo": (s.get("memo") or "").strip()} for s in desired_splits],
        key=lambda x: (x.get("category_id") or "", x.get("amount") or 0, x.get("memo") or ""),
    )
    return current != target


def compose_memo(candidate: dict, summary: str | None, items: list[dict], markdown: bool) -> str:
    clean_items = []
    seen = set()
    for item in items:
        key = ((item.get("name") or "").strip().lower(), (item.get("amount") or "").strip())
        if key in seen:
            continue
        seen.add(key)
        clean_items.append(item)

    # For multi-item memos, keep only entries with explicit amounts so each line includes price.
    if len(clean_items) > 1:
        priced_items = [i for i in clean_items if (i.get("amount") or "").strip()]
        if priced_items:
            clean_items = priced_items

    # Build one memo line per item for reliable YNAB rendering.
    parts = []
    if clean_items:
        parts = [format_item_for_memo(i) for i in clean_items]
    elif summary:
        parts.append(summary)
    else:
        parts.append("Purchase")
    parts = [re.sub(r"\s+", " ", p.replace("\r", " ").replace("\n", " ")).strip() for p in parts if p and p.strip()]
    if not parts:
        return "Purchase"

    if len(parts) > 1:
        # Plain multiline memo, no bullets.
        return "\n".join(parts)

    return parts[0]


def format_item_for_memo(item: dict) -> str:
    name = (item.get("name") or "Item").strip()
    amount = (item.get("amount") or "").strip()
    if amount:
        return f"{name} ({amount})"
    return name


def parse_args():
    parser = argparse.ArgumentParser(description="Sync Amazon email info into YNAB transaction memos.")
    parser.add_argument("--apply", action="store_true", help="Apply updates. Without this, runs in dry-run mode.")
    parser.add_argument(
        "--months-back",
        type=int,
        default=int(env_first("MONTHS_BACK", "months_back", default="1")),
        help="Simple date range control in months (default: 1).",
    )
    parser.add_argument(
        "--coverage",
        choices=["gaps", "all"],
        default=env_first("COVERAGE_MODE", "coverage_mode", default="gaps"),
        help="gaps = target likely incomplete records; all = reprocess all matching Amazon records.",
    )
    parser.add_argument(
        "--depth",
        choices=["normal", "deep"],
        default=env_first("DEPTH_MODE", "depth_mode", default="normal"),
        help="normal = faster pass; deep = comprehensive matching/fallback.",
    )
    parser.add_argument("--max-results", type=int, default=200, help="Max Gmail messages to inspect.")
    parser.add_argument("--day-window", type=int, default=5, help="Allowed day difference for amount/date matching.")
    parser.add_argument(
        "--days-back",
        type=int,
        default=None,
        help="How many days of Gmail/YNAB history to process. If omitted, uses months-back * 30 (or DAYS_BACK from env if set).",
    )
    parser.add_argument(
        "--log-file",
        default=env_first("RUN_LOG_FILE", "run_log_file"),
        help="Optional log file path. If omitted, creates logs/ynab_sync_<timestamp>.log",
    )
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Disable dry-run quick optimizations (slower but more exhaustive).",
    )
    parser.add_argument(
        "--fallback-mode",
        choices=["none", "light", "full"],
        default=env_first("FALLBACK_MODE", "fallback_mode", default="light"),
        help="Fallback matching depth for unmatched transactions.",
    )
    parser.add_argument("--query", default=env_first("GMAIL_QUERY", "gmail_query", default=DEFAULT_QUERY), help="Gmail search query.")
    return parser.parse_args()


def apply_days_back_to_query(query: str, days_back: int) -> str:
    q = (query or "").strip()
    if not q:
        return f"newer_than:{days_back}d"
    # Enforce effective look-back window by replacing existing date filters.
    q = re.sub(r"\bnewer_than:[^\s)]+", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\bafter:[^\s)]+", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\bbefore:[^\s)]+", "", q, flags=re.IGNORECASE)
    q = re.sub(r"\s+", " ", q).strip()
    return f"{q} newer_than:{days_back}d".strip()


def main():
    load_dotenv()
    args = parse_args()

    log_path = Path(args.log_file) if args.log_file else default_log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_fh = log_path.open("a", encoding="utf-8")
    sys.stdout = Tee(sys.stdout, log_fh)
    sys.stderr = Tee(sys.stderr, log_fh)
    print(f"Log file: {log_path.resolve()}")

    ynab_token = env_first("YNAB_TOKEN", "ynab_api_key")
    budget_id = env_first("YNAB_BUDGET_ID", "ynab_budget_id")
    account_id = env_first("YNAB_ACCOUNT_ID", "ynab_account_id")
    target_payee = env_first("YNAB_PAYEE_NAME_TO_BE_PROCESSED", "ynab_payee_name_to_be_processed")
    payee_filter_mode = env_first("YNAB_PAYEE_FILTER_MODE", "ynab_payee_filter_mode", default="contains")
    payee_contains = env_first("YNAB_PAYEE_CONTAINS", "ynab_payee_contains", default="amazon")
    exclude_prime = env_bool("YNAB_EXCLUDE_PRIME", "ynab_exclude_prime", default=True)
    completed_payee = env_first("YNAB_PAYEE_NAME_PROCESSING_COMPLETED", "ynab_payee_name_processing_completed")
    use_markdown = env_bool("YNAB_USE_MARKDOWN", "ynab_use_markdown", default=False)
    use_splits = env_bool("YNAB_USE_SPLITS", "ynab_use_splits", default=True)
    split_strategy = env_first("YNAB_SPLIT_STRATEGY", "ynab_split_strategy", default="conservative")
    use_ai = env_bool("USE_AI_SUMMARIZATION", "use_ai_summarization", default=False)
    openai_key = env_first("OPENAI_API_KEY", "openai_api_key")
    openai_model = env_first("OPENAI_MODEL", "openai_model", default="gpt-4o-mini")
    ai_cache_path = Path(env_first("AI_CACHE_FILE", "ai_cache_file", default="logs/ai_cache.json"))
    ai_cache = load_json(ai_cache_path)
    ai_cache_dirty = False

    missing = [
        key for key, value in {
            "YNAB_TOKEN/ynab_api_key": ynab_token,
            "YNAB_BUDGET_ID/ynab_budget_id": budget_id,
        }.items() if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    if use_ai and not openai_key:
        raise RuntimeError("use_ai_summarization=true but OPENAI_API_KEY/openai_api_key is missing.")

    # Simple-mode controls (months/depth/coverage) map to internal knobs.
    if args.months_back < 1:
        raise RuntimeError("--months-back must be >= 1")
    if args.days_back is None:
        env_days_back = env_first("DAYS_BACK", "days_back")
        if env_days_back:
            try:
                args.days_back = int(env_days_back)
            except ValueError as e:
                raise RuntimeError("DAYS_BACK/days_back must be an integer if set.") from e
        else:
            args.days_back = args.months_back * 30
    if args.days_back < 1:
        raise RuntimeError("--days-back must be >= 1")
    if args.depth == "deep":
        args.full_scan = True
        if args.fallback_mode == "light":
            args.fallback_mode = "full"

    quick_mode = (not args.apply) and (not args.full_scan) and (args.coverage != "all")
    effective_max_results = args.max_results
    if quick_mode:
        effective_max_results = min(args.max_results, 60)

    fallback_mode = args.fallback_mode

    since_date = (datetime.now(UTC) - timedelta(days=args.days_back)).date().isoformat()
    effective_query = apply_days_back_to_query(args.query, args.days_back)
    gmail = get_gmail_service()
    candidates = fetch_amazon_candidates(gmail, effective_query, effective_max_results)
    ynab_transactions = load_ynab_transactions(ynab_token, budget_id, since_date, account_id)
    category_map = load_ynab_category_map(ynab_token, budget_id)

    ynab_transactions, payee_filter_label = filter_transactions_by_payee(
        ynab_transactions,
        target_payee,
        payee_contains,
        exclude_prime,
        payee_filter_mode,
    )
    # Never process transfer/payment transactions.
    ynab_transactions = [t for t in ynab_transactions if not is_transfer_like_transaction(t)]
    if args.coverage == "gaps":
        # Focus on likely gaps: uncategorized, empty memo, or "needs memo" style payee labels.
        def is_gap(tx: dict) -> bool:
            memo = (tx.get("memo") or "").strip()
            payee = (tx.get("payee_name") or "").lower()
            cat_id = tx.get("category_id")
            has_splits = bool(tx.get("subtransactions") or [])
            missing_category = (not cat_id) and (not has_splits)
            return (not memo) or missing_category or ("needs memo" in payee)

        ynab_transactions = [t for t in ynab_transactions if is_gap(t)]
        payee_filter_label = f"{payee_filter_label} + gaps-only"
    else:
        payee_filter_label = f"{payee_filter_label} + all-records"
    ynab_transactions.sort(key=lambda t: (t.get("date") or ""), reverse=True)
    if quick_mode:
        ynab_transactions = ynab_transactions[:80]

    candidates.sort(key=lambda x: x.get("email_dt") or datetime.min.replace(tzinfo=UTC))

    print(f"Gmail candidates: {len(candidates)}")
    print(f"YNAB transactions fetched: {len(ynab_transactions)}")
    print(f"Mode: {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"Coverage: {args.coverage}")
    print(f"Depth: {args.depth}")
    print(f"Quick mode: {'ON' if quick_mode else 'OFF'}")
    print(f"AI summarization: {'ON' if use_ai else 'OFF'}")
    print(f"YNAB splits: {'ON' if use_splits else 'OFF'}")
    print(f"Split strategy: {(split_strategy or 'conservative').lower()}")
    print(f"Fallback mode: {fallback_mode}")
    print(f"Filtering payee: {payee_filter_label}")
    print(f"Days back: {args.days_back}")
    print()

    proposed = 0
    applied = 0
    used_tx_ids = set()
    tx_detail_cache: dict[str, dict] = {}
    pending_updates: list[dict] = []

    for c in candidates:
        primary_day_window = args.day_window if args.depth == "deep" else min(args.day_window, 1)
        tx = find_best_match(
            c,
            ynab_transactions,
            primary_day_window,
            used_tx_ids,
            allow_relaxed=(args.depth == "deep"),
        )
        if not tx:
            continue

        category = c["category_guess"]
        summary = None
        items = []
        tx_amount_text = f"{abs(tx.get('amount', 0)) / 1000:.2f}" if isinstance(tx.get("amount"), int) else None
        if use_ai:
            try:
                cache_key_raw = json.dumps(
                    {
                        "cache_version": CACHE_VERSION,
                        "subject": c.get("subject"),
                        "order_id": c.get("order_id"),
                        "email_date": c.get("email_date"),
                        "charge_amount": tx_amount_text,
                        "excerpt": c.get("body_excerpt", "")[:2000],
                    },
                    sort_keys=True,
                )
                cache_key = hashlib.sha1(cache_key_raw.encode("utf-8")).hexdigest()
                cached = ai_cache.get(cache_key)
                if cached:
                    category = cached.get("category", "Shopping")
                    summary = cached.get("summary")
                    items = cached.get("items") or []
                else:
                    category, summary, items = ai_summarize_candidate(c, openai_key, openai_model, tx_amount_text)
                    ai_cache[cache_key] = {"category": category, "summary": summary, "items": items}
                    ai_cache_dirty = True
            except Exception as e:
                summary = None
                print(f"WARN AI fallback for tx {tx.get('id')}: {e}")
        else:
            category = normalize_category(category)
            if c.get("subject"):
                items = [{"name": normalize_summary(c["subject"]), "amount": None, "category": category}]
        if isinstance(tx.get("amount"), int):
            items = pick_items_for_charge(items, tx["amount"])
        items = compress_items_for_charge(items, tx.get("amount"))

        allow_splits_here = use_splits and should_use_splits_for_transaction(
            items, tx.get("amount"), category_map, split_strategy
        )
        desired_splits = (
            build_split_subtransactions(
                items,
                tx.get("amount"),
                category_map,
                allow_single_category=((split_strategy or "").strip().lower() == "all"),
            )
            if allow_splits_here
            else []
        )
        desired_splits = force_single_split_for_all_strategy(
            desired_splits,
            split_strategy,
            tx.get("amount"),
            items,
            category_map,
        )
        desired_category_id = choose_primary_category_id(items, category_map) if not desired_splits else None
        memo = SPLIT_PARENT_MEMO if desired_splits else compose_memo(c, summary, items, use_markdown)
        existing = tx.get("memo") or ""
        old_payee = tx.get("payee_name") or ""
        tx_id = tx.get("id")
        tx_for_compare = tx
        if use_splits and tx_id:
            if tx_id not in tx_detail_cache:
                tx_detail_cache[tx_id] = load_ynab_transaction(ynab_token, budget_id, tx_id)
            tx_for_compare = tx_detail_cache[tx_id] or tx
        old_category_id = tx_for_compare.get("category_id", tx.get("category_id"))

        wants_payee_change = bool(completed_payee and old_payee != completed_payee)
        wants_memo_change = existing != memo
        wants_split_change = split_changed(tx_for_compare, desired_splits)
        wants_category_change = bool(desired_category_id and desired_category_id != old_category_id)

        if not wants_memo_change and not wants_payee_change and not wants_split_change and not wants_category_change:
            used_tx_ids.add(tx["id"])
            continue

        proposed += 1
        payee_change_text = (
            f"{old_payee!r}->{completed_payee!r}" if wants_payee_change else f"{old_payee!r} (unchanged)"
        )
        print(
            f"{tx.get('date')} | {tx.get('amount') / 1000:.2f} | tx:{tx.get('id')} | "
            f"payee:{payee_change_text}"
        )
        print(f"  memo: {existing!r} -> {memo!r}")
        if desired_category_id and not desired_splits:
            print(f"  category_id: {old_category_id!r} -> {desired_category_id!r}")
        if desired_splits:
            for s in desired_splits:
                print(f"  split: {s['amount'] / 1000:.2f} | cat:{s['category_id']} | memo:{s.get('memo','')!r}")

        if args.apply:
            existing_has_splits = bool(tx_for_compare.get("subtransactions") or [])
            split_payload = None
            if desired_splits:
                split_payload = desired_splits if wants_split_change else None
            elif use_splits and existing_has_splits and not desired_splits:
                # Force-clear stale splits whenever desired state is non-split.
                split_payload = []
            if split_payload == [] and existing_has_splits:
                try:
                    force_unsplit_transaction(
                        ynab_token,
                        budget_id,
                        tx_for_compare,
                        memo,
                        completed_payee if wants_payee_change else None,
                        desired_category_id if wants_category_change else None,
                    )
                    applied += 1
                except Exception as e:
                    print(f"WARN Apply failed for tx {tx.get('id')}: {e}")
            elif not desired_splits and wants_category_change:
                # For non-split category updates, use full PUT for reliability.
                try:
                    force_unsplit_transaction(
                        ynab_token,
                        budget_id,
                        tx_for_compare,
                        memo,
                        completed_payee if wants_payee_change else None,
                        desired_category_id,
                    )
                    applied += 1
                except Exception as e:
                    print(f"WARN Apply failed for tx {tx.get('id')}: {e}")
            else:
                update_tx = {"id": tx["id"], "memo": memo}
                if wants_payee_change:
                    update_tx["payee_name"] = completed_payee
                if split_payload is not None:
                    update_tx["subtransactions"] = split_payload
                if wants_category_change and not desired_splits:
                    update_tx["category_id"] = desired_category_id
                pending_updates.append(update_tx)

        used_tx_ids.add(tx["id"])

    print()
    print(f"Proposed updates: {proposed}")
    unmatched = [t for t in ynab_transactions if t.get("id") not in used_tx_ids]
    def is_gap_candidate(t: dict) -> bool:
        memo = (t.get("memo") or "").strip()
        payee = (t.get("payee_name") or "").lower()
        cat_id = t.get("category_id")
        has_splits = bool(t.get("subtransactions") or [])
        missing_category = (not cat_id) and (not has_splits)
        return (not memo) or missing_category or ("needs memo" in payee)
    if unmatched and fallback_mode != "none":
        print("Running targeted fallback matching for unmatched transactions...")
    fallback_targets = unmatched
    if args.coverage == "all":
        # In all-records mode, fallback should prioritize true data gaps and avoid
        # rewriting already-labeled transactions with weaker matches.
        gap_targets = [t for t in unmatched if is_gap_candidate(t)]
        if gap_targets:
            fallback_targets = gap_targets
    if fallback_mode == "light":
        light_limit = 6 if args.coverage == "gaps" else 25
        fallback_targets = fallback_targets[:light_limit]
    fallback_scan_limit = 40 if fallback_mode == "light" else 120
    fallback_max_delta_days = 21 if fallback_mode == "light" else 45
    fallback_query_window_days = 21 if fallback_mode == "light" else 45
    fallback_max_amount_gap = 200 if fallback_mode == "light" else 1200
    for tx in fallback_targets:
        if fallback_mode == "none":
            break
        candidate = search_candidate_for_transaction(
            gmail,
            tx,
            args.day_window,
            per_query_scan_limit=fallback_scan_limit,
            max_delta_days=fallback_max_delta_days,
            query_window_days=fallback_query_window_days,
            max_amount_gap_milli=fallback_max_amount_gap,
        )
        if not candidate:
            continue

        summary = None
        items = []
        tx_amount_text = f"{abs(tx.get('amount', 0)) / 1000:.2f}" if isinstance(tx.get("amount"), int) else None
        if use_ai:
            try:
                cache_key_raw = json.dumps(
                    {
                        "cache_version": CACHE_VERSION,
                        "subject": candidate.get("subject"),
                        "order_id": candidate.get("order_id"),
                        "email_date": candidate.get("email_date"),
                        "charge_amount": tx_amount_text,
                        "excerpt": candidate.get("body_excerpt", "")[:2000],
                    },
                    sort_keys=True,
                )
                cache_key = hashlib.sha1(cache_key_raw.encode("utf-8")).hexdigest()
                cached = ai_cache.get(cache_key)
                if cached:
                    summary = cached.get("summary")
                    items = cached.get("items") or []
                else:
                    _, summary, items = ai_summarize_candidate(candidate, openai_key, openai_model, tx_amount_text)
                    ai_cache[cache_key] = {"category": "Shopping", "summary": summary, "items": items}
                    ai_cache_dirty = True
            except Exception as e:
                summary = None
                print(f"WARN AI fallback for tx {tx.get('id')}: {e}")
        else:
            category = normalize_category(candidate["category_guess"])
            if candidate.get("subject"):
                items = [{"name": normalize_summary(candidate["subject"]), "amount": None, "category": category}]
        if isinstance(tx.get("amount"), int):
            items = pick_items_for_charge(items, tx["amount"])
        items = compress_items_for_charge(items, tx.get("amount"))

        allow_splits_here = use_splits and should_use_splits_for_transaction(
            items, tx.get("amount"), category_map, split_strategy
        )
        desired_splits = (
            build_split_subtransactions(
                items,
                tx.get("amount"),
                category_map,
                allow_single_category=((split_strategy or "").strip().lower() == "all"),
            )
            if allow_splits_here
            else []
        )
        desired_splits = force_single_split_for_all_strategy(
            desired_splits,
            split_strategy,
            tx.get("amount"),
            items,
            category_map,
        )
        desired_category_id = choose_primary_category_id(items, category_map) if not desired_splits else None
        memo = SPLIT_PARENT_MEMO if desired_splits else compose_memo(candidate, summary, items, use_markdown)
        existing = tx.get("memo") or ""
        old_payee = tx.get("payee_name") or ""
        tx_id = tx.get("id")
        if (candidate.get("match_confidence") == "low") and existing.strip():
            # Skip risky fallback overwrites on already-labeled transactions.
            continue
        tx_for_compare = tx
        if use_splits and tx_id:
            if tx_id not in tx_detail_cache:
                tx_detail_cache[tx_id] = load_ynab_transaction(ynab_token, budget_id, tx_id)
            tx_for_compare = tx_detail_cache[tx_id] or tx
        old_category_id = tx_for_compare.get("category_id", tx.get("category_id"))
        wants_payee_change = bool(completed_payee and old_payee != completed_payee)
        wants_memo_change = existing != memo
        wants_split_change = split_changed(tx_for_compare, desired_splits)
        wants_category_change = bool(desired_category_id and desired_category_id != old_category_id)
        if not wants_memo_change and not wants_payee_change and not wants_split_change and not wants_category_change:
            used_tx_ids.add(tx["id"])
            continue

        proposed += 1
        payee_change_text = (
            f"{old_payee!r}->{completed_payee!r}" if wants_payee_change else f"{old_payee!r} (unchanged)"
        )
        print(
            f"{tx.get('date')} | {tx.get('amount') / 1000:.2f} | tx:{tx.get('id')} | "
            f"payee:{payee_change_text} [fallback]"
        )
        print(f"  memo: {existing!r} -> {memo!r}")
        if desired_category_id and not desired_splits:
            print(f"  category_id: {old_category_id!r} -> {desired_category_id!r}")
        if desired_splits:
            for s in desired_splits:
                print(f"  split: {s['amount'] / 1000:.2f} | cat:{s['category_id']} | memo:{s.get('memo','')!r}")

        if args.apply:
            existing_has_splits = bool(tx_for_compare.get("subtransactions") or [])
            split_payload = None
            if desired_splits:
                split_payload = desired_splits if wants_split_change else None
            elif use_splits and existing_has_splits and not desired_splits:
                # Force-clear stale splits whenever desired state is non-split.
                split_payload = []
            if split_payload == [] and existing_has_splits:
                try:
                    force_unsplit_transaction(
                        ynab_token,
                        budget_id,
                        tx_for_compare,
                        memo,
                        completed_payee if wants_payee_change else None,
                        desired_category_id if wants_category_change else None,
                    )
                    applied += 1
                except Exception as e:
                    print(f"WARN Apply failed for tx {tx.get('id')}: {e}")
            elif not desired_splits and wants_category_change:
                try:
                    force_unsplit_transaction(
                        ynab_token,
                        budget_id,
                        tx_for_compare,
                        memo,
                        completed_payee if wants_payee_change else None,
                        desired_category_id,
                    )
                    applied += 1
                except Exception as e:
                    print(f"WARN Apply failed for tx {tx.get('id')}: {e}")
            else:
                update_tx = {"id": tx["id"], "memo": memo}
                if wants_payee_change:
                    update_tx["payee_name"] = completed_payee
                if split_payload is not None:
                    update_tx["subtransactions"] = split_payload
                if wants_category_change and not desired_splits:
                    update_tx["category_id"] = desired_category_id
                pending_updates.append(update_tx)

        used_tx_ids.add(tx["id"])

    unmatched = [t for t in ynab_transactions if t.get("id") not in used_tx_ids]
    if unmatched:
        if args.coverage == "all":
            gap_unmatched = [t for t in unmatched if is_gap_candidate(t)]
            print(
                f"Unmatched YNAB transactions (all-records mode): {len(unmatched)} total | "
                f"{len(gap_unmatched)} gap-candidates"
            )
            to_show = gap_unmatched or unmatched
        else:
            print(f"Unmatched YNAB transactions: {len(unmatched)}")
            to_show = unmatched
        for t in to_show[:10]:
            print(
                f"  - {t.get('date')} | {t.get('amount') / 1000:.2f} | "
                f"{(t.get('payee_name') or '(no payee)')} | tx:{t.get('id')}"
            )
    if args.apply:
        applied += flush_batched_ynab_updates(ynab_token, budget_id, pending_updates, batch_size=25)
        print(f"Applied updates: {applied}")
    if ai_cache_dirty:
        save_json(ai_cache_path, ai_cache)


if __name__ == "__main__":
    main()
