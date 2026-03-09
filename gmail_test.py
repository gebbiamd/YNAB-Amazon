import os
import re
import base64
from pathlib import Path

from bs4 import BeautifulSoup

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


# Read-only Gmail access
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# Change these if you want
DEFAULT_QUERY = 'in:"YNAB Amazon" (from:amazon OR from:order-update@amazon.com OR subject:("order" OR "shipped" OR "delivered" OR "receipt")) newer_than:90d'
MAX_RESULTS = 20


def get_gmail_service():
    """
    Expects:
      - credentials.json in the current folder (OAuth client from Google Cloud)
      - token.json will be created after first auth
    """
    creds = None
    token_path = Path("token.json")
    creds_path = Path("credentials.json")

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not creds_path.exists():
                raise FileNotFoundError(
                    "Missing credentials.json in this folder. "
                    "Download OAuth client JSON from Google Cloud and name it credentials.json"
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
            creds = flow.run_local_server(port=0)

        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def _decode_base64url(data: str) -> bytes:
    return base64.urlsafe_b64decode(data.encode("utf-8"))


def extract_best_body_text(payload: dict) -> str:
    """
    Gmail messages can be nested multiparts.
    We try to grab:
      - text/html first
      - otherwise text/plain
    """
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
    headers = payload.get("headers", []) or []
    out = {}
    for h in headers:
        name = h.get("name", "")
        value = h.get("value", "")
        if name and value:
            out[name.lower()] = value
    return out


def parse_order_and_total(text: str):
    """
    Heuristic parsing. Amazon email formats vary, so we do best-effort extraction.
    Returns (order_id, total_str) where either can be None.
    """
    # Order ID patterns like: 112-1234567-1234567
    order_match = re.search(r"\b\d{3}-\d{7}-\d{7}\b", text)
    order_id = order_match.group(0) if order_match else None

    # Total patterns (tries a few common phrases)
    total = None
    total_patterns = [
        r"(Order\s+Total|Total\s+for\s+this\s+order|Grand\s+Total|Total)\s*[:\-]?\s*\$?\s*([0-9]+(?:\.[0-9]{2})?)",
        r"\$\s*([0-9]+(?:\.[0-9]{2})?)\s*(?:USD)?\s*(?:Order\s+Total|Total)",
    ]
    for pat in total_patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            # pick last capture group that looks like money
            total = m.group(m.lastindex)
            break

    return order_id, total


def main():
    service = get_gmail_service()

    query = os.getenv("GMAIL_QUERY", DEFAULT_QUERY)

    print("Query:", query)
    res = service.users().messages().list(
        userId="me",
        q=query,
        maxResults=MAX_RESULTS,
    ).execute()

    msgs = res.get("messages", []) or []
    print(f"Found {len(msgs)} messages\n")

    if not msgs:
        return

    for i, m in enumerate(msgs, start=1):
        msg = service.users().messages().get(
            userId="me",
            id=m["id"],
            format="full",
        ).execute()

        payload = msg.get("payload", {}) or {}
        headers = extract_headers(payload)
        subject = headers.get("subject", "(no subject)")
        date = headers.get("date", "(no date)")

        body_text = extract_best_body_text(payload)
        order_id, total = parse_order_and_total(body_text)

        print(f"--- {i} ---")
        print("Subject:", subject)
        print("Date   :", date)
        print("Order  :", order_id)
        print("Total  :", total)
        print()

if __name__ == "__main__":
    main()
