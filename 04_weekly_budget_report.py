import argparse
import base64
import html
import io
import json
import os
import re
from calendar import monthrange
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from math import isclose
from pathlib import Path
from statistics import mean, pstdev
from urllib import error, request

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


GMAIL_SEND_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
YNAB_BASE_URL = "https://api.ynab.com/v1"
BILL_KEYWORDS = {
    "rent", "mortgage", "insurance", "phone", "internet", "electric", "water", "utility",
    "subscription", "netflix", "spotify", "hulu", "gym", "loan", "payment", "interest", "membership",
    "allegro", "hoa", "laser hair removal", "wood haven bridg", "woodhaven bridg",
}
SUBSCRIPTION_KEYWORDS = {
    "spotify", "netflix", "hulu", "disney", "apple", "apple.com", "itunes", "app store", "icloud",
    "youtube", "prime", "anki", "membership", "patreon", "onlyfans", "gym", "fitness",
}
WASTEFUL_SUB_KEYWORDS = {"onlyfans", "patreon"}
SAFE_SUB_KEYWORDS = {"anytime fitness", "gym", "fitness"}


def load_dotenv(path: str = ".env") -> None:
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


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")


def ynab_request(token: str, method: str, path: str, payload: dict | None = None) -> dict:
    url = f"{YNAB_BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")

    req = request.Request(url, method=method, headers=headers, data=body)
    try:
        with request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"YNAB API error {e.code}: {detail}") from e


def openai_recommendations(api_key: str, model: str, metrics: dict) -> dict:
    subs_for_ai = metrics.get("subscriptions", [])[:12]
    prompt = {
        "role": "user",
        "content": (
            "You are a personal budget analyst. "
            "Given these YNAB metrics, return JSON with keys: "
            "executive_summary (string, 2-4 sentences), wins (array of 3 short bullets), "
            "risks (array of 3 short bullets), recommendations (array of 5 concrete actions), "
            "subscription_assessment (array of objects with payee_name, waste_level, confidence, reason). "
            "fixed_bill_assessment (array with payee_name, stability_level, confidence, reason), "
            "goal_assessment (array with category_name, urgency, confidence, reason), "
            "outlier_assessment (array with date, payee_name, confidence, reason), "
            "category_assessment (array with category_name, confidence, reason). "
            "waste_level must be exactly one of: keep, review, waste. "
            "stability_level must be one of: stable, watch, optimize. "
            "urgency must be one of: low, medium, high. "
            "For subscription_assessment, do not use amount-only logic. "
            "Judge likely value/waste using necessity, category fit, recurring pattern, and opportunity cost. "
            "If previous_subscription_assessment is provided and evidence is unchanged, keep prior label unless confidence is >=0.80 for a change. "
            "Confidence must be 0.0-1.0. Reason should be concise and specific. "
            "Write in concise Gen Z-friendly tone (clear, modern, not cringe, no slang overload). "
            "Use light markdown emphasis where helpful (e.g., **Category Name**, *pace issue*, __must-do__). "
            "Prioritize underfunded goals, unusual transactions, and spending trend shifts. "
            f"Metrics: {json.dumps(metrics)}. "
            f"Subscriptions to score: {json.dumps(subs_for_ai)}"
        ),
    }

    payload = {
        "model": model,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": "You produce practical, data-grounded budgeting advice."},
            prompt,
        ],
    }

    req = request.Request(
        "https://api.openai.com/v1/chat/completions",
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload).encode("utf-8"),
    )

    try:
        with request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API error {e.code}: {detail}") from e

    content = data["choices"][0]["message"]["content"]
    parsed = json.loads(content)

    return {
        "executive_summary": parsed.get("executive_summary", ""),
        "wins": parsed.get("wins", [])[:3],
        "risks": parsed.get("risks", [])[:3],
        "recommendations": parsed.get("recommendations", [])[:5],
        "subscription_assessment": parsed.get("subscription_assessment", [])[:12],
        "fixed_bill_assessment": parsed.get("fixed_bill_assessment", [])[:12],
        "goal_assessment": parsed.get("goal_assessment", [])[:15],
        "outlier_assessment": parsed.get("outlier_assessment", [])[:12],
        "category_assessment": parsed.get("category_assessment", [])[:12],
    }


def get_gmail_send_service(credentials_path: Path, token_path: Path):
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), GMAIL_SEND_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not credentials_path.exists():
                raise FileNotFoundError(
                    f"Missing OAuth client file at {credentials_path}. "
                    "Create credentials.json from Google Cloud OAuth Client."
                )
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), GMAIL_SEND_SCOPES)
            creds = flow.run_local_server(port=0)
        token_path.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def send_html_email(service, to_email: str, subject: str, html_body: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    service.users().messages().send(userId="me", body={"raw": raw}).execute()


@dataclass
class CategorySnapshot:
    name: str
    activity: float
    budgeted: float
    available: float | None = None

    @property
    def variance(self) -> float:
        if self.available is not None:
            return round(self.available, 2)
        return round(self.budgeted - self.activity, 2)

    @property
    def month_delta(self) -> float:
        return round(self.budgeted - self.activity, 2)


@dataclass
class RecurringSnapshot:
    payee_name: str
    kind: str
    months_seen: int
    avg_amount: float
    latest_amount: float
    canonical_key: str
    last_seen_date: str


@dataclass
class MonthSnapshot:
    label: str
    spend: float
    budgeted: float


@dataclass
class ActionRecommendation:
    priority: str
    emoji: str
    title: str
    detail: str


@dataclass
class UnderfundedSnapshot:
    category_name: str
    goal_type: str
    target_amount: float
    funded_amount: float
    underfunded_amount: float


@dataclass
class OutlierSnapshot:
    date: str
    payee_name: str
    category_name: str
    amount: float
    baseline_mean: float
    reason: str
    memo: str = ""



def milli_to_float(amount_milli: int | float | None) -> float:
    if amount_milli is None:
        return 0.0
    return round(amount_milli / 1000.0, 2)


def canonical_recurring_name(name: str) -> str:
    n = (name or "").strip().lower()
    if not n:
        return "Unknown Recurring"
    if "wood haven bridg" in n or "woodhaven bridg" in n or "hoa" in n:
        return "HOA Fee"
    if "allegro" in n or "laser hair removal" in n:
        return "Laser Hair Removal (Allegro Credit)"
    if "u-verse" in n or "uverse" in n or "at&t" in n or n == "internet":
        return "Internet Service"
    if "we energies" in n or n == "electric":
        return "Electric Utility"
    if "abc anytime fitness" in n or "anytime fitness" in n:
        return "Anytime Fitness"
    if "apple.com" in n or "itunes" in n or "app store" in n:
        return "Apple App Store"
    return name.strip()


def classify_recurring_kind(name: str, dominant_category: str = "") -> tuple[bool, bool]:
    n = (name or "").lower()
    c = (dominant_category or "").lower()
    bill_like = any(k in n for k in BILL_KEYWORDS) or any(k in c for k in BILL_KEYWORDS)
    sub_like = any(k in n for k in SUBSCRIPTION_KEYWORDS) or any(k in c for k in SUBSCRIPTION_KEYWORDS)
    return bill_like, sub_like


def latest_nonzero_amount(monthly: dict[str, float], month_labels: list[str]) -> float:
    for m in reversed(month_labels):
        v = monthly.get(m, 0.0)
        if v > 0:
            return round(v, 2)
    return 0.0


def is_wasteful_subscription(name: str, avg_amount: float) -> bool:
    n = (name or "").lower()
    if any(k in n for k in SAFE_SUB_KEYWORDS):
        return False
    if any(k in n for k in WASTEFUL_SUB_KEYWORDS):
        return True
    return avg_amount >= 45.0


def icon_for_label(name: str, kind: str = "") -> str:
    n = (name or "").lower()
    if "mortgage" in n or "hoa" in n or "rent" in n:
        return "🏠"
    if "electric" in n or "utility" in n:
        return "⚡"
    if "internet" in n or "u-verse" in n or "uverse" in n:
        return "🌐"
    if "spotify" in n or "netflix" in n or "patreon" in n or "onlyfans" in n:
        return "🎬"
    if "fitness" in n or "gym" in n:
        return "💪"
    if "apple" in n or "app store" in n or "itunes" in n:
        return "📱"
    if "insurance" in n:
        return "🛡️"
    if kind == "fixed_bill":
        return "🧾"
    if kind == "subscription":
        return "🔁"
    return "🗂️"


def normalize_payee_key(name: str) -> str:
    return canonical_recurring_name(name).strip().lower()


def build_subscription_assessment_map(ai: dict, subscriptions: list[RecurringSnapshot]) -> dict[str, dict[str, str | float]]:
    by_key: dict[str, dict[str, str | float]] = {}
    raw = ai.get("subscription_assessment", [])
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            payee = str(item.get("payee_name", "")).strip()
            if not payee:
                continue
            level = str(item.get("waste_level", "")).strip().lower()
            if level not in {"keep", "review", "waste"}:
                continue
            confidence_raw = item.get("confidence", 0.5)
            try:
                confidence = float(confidence_raw)
            except Exception:
                confidence = 0.5
            confidence = max(0.0, min(1.0, confidence))
            reason = str(item.get("reason", "")).strip()
            by_key[normalize_payee_key(payee)] = {
                "waste_level": level,
                "confidence": confidence,
                "reason": reason,
            }

    # Fill any gaps with deterministic fallback so report is always complete.
    for s in subscriptions:
        key = normalize_payee_key(s.payee_name)
        if key in by_key:
            continue
        level = "waste" if is_wasteful_subscription(s.payee_name, s.avg_amount) else ("review" if s.avg_amount >= 35 else "keep")
        reason = (
            "Likely discretionary subscription; verify value and usage."
            if level == "waste"
            else "Could be optimized; check if this still delivers value."
            if level == "review"
            else "Likely aligned with regular use."
        )
        by_key[key] = {"waste_level": level, "confidence": 0.45, "reason": reason}
    return by_key


def stabilize_subscription_assessment(
    current: list[dict],
    previous: list[dict],
    min_conf_for_change: float = 0.8,
) -> list[dict]:
    prev_map: dict[str, dict] = {}
    for item in previous or []:
        if not isinstance(item, dict):
            continue
        payee = str(item.get("payee_name", "")).strip()
        level = str(item.get("waste_level", "")).strip().lower()
        if not payee or level not in {"keep", "review", "waste"}:
            continue
        prev_map[normalize_payee_key(payee)] = item

    stabilized: list[dict] = []
    for item in current or []:
        if not isinstance(item, dict):
            continue
        payee = str(item.get("payee_name", "")).strip()
        level = str(item.get("waste_level", "")).strip().lower()
        if not payee or level not in {"keep", "review", "waste"}:
            stabilized.append(item)
            continue
        key = normalize_payee_key(payee)
        prev = prev_map.get(key)
        if not prev:
            stabilized.append(item)
            continue
        prev_level = str(prev.get("waste_level", "")).strip().lower()
        try:
            conf = float(item.get("confidence", 0.5))
        except Exception:
            conf = 0.5
        if prev_level in {"keep", "review", "waste"} and level != prev_level and conf < min_conf_for_change:
            carry = dict(item)
            carry["waste_level"] = prev_level
            carry["reason"] = str(item.get("reason", "")).strip() + " (label stabilized from prior run)"
            stabilized.append(carry)
        else:
            stabilized.append(item)
    return stabilized


def build_fixed_bill_assessment_map(ai: dict, fixed_bills: list[RecurringSnapshot]) -> dict[str, dict[str, str | float]]:
    out: dict[str, dict[str, str | float]] = {}
    raw = ai.get("fixed_bill_assessment", [])
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            payee = str(item.get("payee_name", "")).strip()
            level = str(item.get("stability_level", "")).strip().lower()
            if not payee or level not in {"stable", "watch", "optimize"}:
                continue
            reason = str(item.get("reason", "")).strip()
            try:
                confidence = float(item.get("confidence", 0.5))
            except Exception:
                confidence = 0.5
            out[normalize_payee_key(payee)] = {
                "stability_level": level,
                "reason": reason,
                "confidence": max(0.0, min(1.0, confidence)),
            }
    for r in fixed_bills:
        key = normalize_payee_key(r.payee_name)
        if key in out:
            continue
        out[key] = {
            "stability_level": "stable",
            "reason": "Recurring baseline appears consistent.",
            "confidence": 0.45,
        }
    return out


def build_goal_assessment_map(ai: dict, underfunded: list[UnderfundedSnapshot]) -> dict[str, dict[str, str | float]]:
    out: dict[str, dict[str, str | float]] = {}
    raw = ai.get("goal_assessment", [])
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            category = str(item.get("category_name", "")).strip()
            urgency = str(item.get("urgency", "")).strip().lower()
            if not category or urgency not in {"low", "medium", "high"}:
                continue
            reason = str(item.get("reason", "")).strip()
            try:
                confidence = float(item.get("confidence", 0.5))
            except Exception:
                confidence = 0.5
            out[category.lower()] = {"urgency": urgency, "reason": reason, "confidence": max(0.0, min(1.0, confidence))}
    for u in underfunded:
        key = u.category_name.lower()
        if key in out:
            continue
        urgency = "high" if u.underfunded_amount >= 300 else ("medium" if u.underfunded_amount >= 100 else "low")
        out[key] = {
            "urgency": urgency,
            "reason": "Funding gap should be reviewed this week.",
            "confidence": 0.4,
        }
    return out


def build_outlier_assessment_map(ai: dict, outliers: list[OutlierSnapshot]) -> dict[str, dict[str, str | float]]:
    out: dict[str, dict[str, str | float]] = {}
    raw = ai.get("outlier_assessment", [])
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            date_s = str(item.get("date", "")).strip()
            payee = str(item.get("payee_name", "")).strip()
            if not date_s or not payee:
                continue
            reason = str(item.get("reason", "")).strip()
            try:
                confidence = float(item.get("confidence", 0.5))
            except Exception:
                confidence = 0.5
            out[f"{date_s}|{payee.lower()}"] = {"reason": reason, "confidence": max(0.0, min(1.0, confidence))}
    for o in outliers:
        key = f"{o.date}|{o.payee_name.lower()}"
        if key in out:
            continue
        out[key] = {"reason": o.reason, "confidence": 0.45}
    return out


def build_category_assessment_map(ai: dict, categories: list[CategorySnapshot]) -> dict[str, dict[str, str | float]]:
    out: dict[str, dict[str, str | float]] = {}
    raw = ai.get("category_assessment", [])
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            category = str(item.get("category_name", "")).strip()
            if not category:
                continue
            reason = str(item.get("reason", "")).strip()
            try:
                confidence = float(item.get("confidence", 0.5))
            except Exception:
                confidence = 0.5
            out[category.lower()] = {"reason": reason, "confidence": max(0.0, min(1.0, confidence))}
    for c in categories:
        key = c.name.lower()
        if key in out:
            continue
        if c.variance < 0:
            reason = "Spending exceeded budget; tighten weekly cap."
        else:
            reason = "Currently under budget; maintain pace."
        out[key] = {"reason": reason, "confidence": 0.4}
    return out


def extract_memo_highlights(entries: list[dict], month_labels: list[str], limit: int = 18) -> list[dict]:
    if not entries or not month_labels:
        return []
    current = month_labels[-1]
    recent_months = set(month_labels[-2:]) if len(month_labels) >= 2 else {current}
    rows = [e for e in entries if e.get("month") == current and str(e.get("memo", "")).strip()]
    if not rows:
        rows = [e for e in entries if e.get("month") in recent_months and str(e.get("memo", "")).strip()]
    rows.sort(
        key=lambda e: (
            0 if "amazon" in str(e.get("payee", "")).lower() else 1,
            -float(e.get("amount", 0.0)),
        )
    )
    out = []
    for e in rows[:limit]:
        memo = str(e.get("memo", "")).strip()
        if len(memo) > 180:
            memo = memo[:177] + "..."
        out.append(
            {
                "date": e.get("date", ""),
                "payee": e.get("payee", ""),
                "category": e.get("category", ""),
                "amount": round(float(e.get("amount", 0.0)), 2),
                "memo": memo,
            }
        )
    return out


def chart_to_base64(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buffer.getvalue()).decode("utf-8")


def build_trend_chart(month_labels: list[str], month_spend: list[float]) -> str:
    if not month_labels or (month_spend and all(isclose(x, 0.0, abs_tol=0.001) for x in month_spend)):
        fig, ax = plt.subplots(figsize=(10, 3.2))
        ax.text(0.5, 0.5, "No spending activity detected in selected months", ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.patch.set_facecolor("white")
        return chart_to_base64(fig)

    fig, ax = plt.subplots(figsize=(10, 4.8))
    avg_line = mean(month_spend) if month_spend else 0.0
    ax.plot(month_labels, month_spend, marker="o", markersize=7, linewidth=3.0, color="#0f766e")
    ax.fill_between(month_labels, month_spend, color="#14b8a6", alpha=0.22)
    ax.axhline(avg_line, color="#0f766e", linestyle="--", linewidth=1.4, alpha=0.6)
    ax.text(month_labels[-1], month_spend[-1], f"  ${month_spend[-1]:,.0f}", va="center", fontsize=10, color="#0b4b47")
    ax.set_title("Monthly Spending Trend", fontsize=14, fontweight="bold")
    ax.set_ylabel("Spend ($)")
    ax.grid(alpha=0.2, linestyle="--")
    ax.set_facecolor("#f0fdfa")
    fig.patch.set_facecolor("#f8fffd")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return chart_to_base64(fig)


def build_top_categories_chart(categories: list[CategorySnapshot]) -> str:
    top = sorted(categories, key=lambda c: c.activity, reverse=True)[:8]
    if not top:
        fig, ax = plt.subplots(figsize=(10, 3.2))
        ax.text(0.5, 0.5, "No category spend found for analysis month", ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.patch.set_facecolor("white")
        return chart_to_base64(fig)

    names = [c.name[:24] for c in top][::-1]
    values = [c.activity for c in top][::-1]
    colors = ["#ef4444" if c.variance < 0 else "#0ea5e9" for c in top][::-1]

    fig, ax = plt.subplots(figsize=(10, 5.4))
    ax.hlines(y=names, xmin=0, xmax=values, color="#cbd5e1", linewidth=3, alpha=0.9)
    ax.scatter(values, names, s=130, c=colors, alpha=0.95, edgecolors="white", linewidth=1.2, zorder=3)
    for i, v in enumerate(values):
        ax.text(v, i, f"  ${v:,.0f}", va="center", ha="left", fontsize=9, color="#334155")
    ax.set_title("Top Category Spend (Current Month)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Spend ($)")
    ax.grid(axis="x", alpha=0.2, linestyle=":")
    ax.set_facecolor("#f0f9ff")
    fig.patch.set_facecolor("#f8fbff")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return chart_to_base64(fig)


def build_budget_vs_spend_chart(months: list[MonthSnapshot]) -> str:
    labels = [m.label for m in months]
    spend = [m.spend for m in months]
    budgeted = [m.budgeted for m in months]
    if not labels or (
        all(isclose(x, 0.0, abs_tol=0.001) for x in spend)
        and all(isclose(x, 0.0, abs_tol=0.001) for x in budgeted)
    ):
        fig, ax = plt.subplots(figsize=(10, 3.2))
        ax.text(0.5, 0.5, "No budget/spend values detected for selected months", ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.patch.set_facecolor("white")
        return chart_to_base64(fig)

    fig, ax = plt.subplots(figsize=(10, 4.9))
    x = list(range(len(labels)))
    w = 0.35
    ax.bar([i - w / 2 for i in x], budgeted, width=w, color="#93c5fd", alpha=0.9, label="Budgeted")
    ax.bar([i + w / 2 for i in x], spend, width=w, color="#fca5a5", alpha=0.95, label="Spent")
    ax.plot(x, spend, linewidth=1.8, color="#ef4444", alpha=0.8)
    for i in x:
        if spend[i] > budgeted[i]:
            ax.text(i, spend[i], " over", color="#b91c1c", fontsize=9, va="bottom", ha="center")
    ax.set_xticks(x, labels)
    ax.set_title("Budget vs Spend", fontsize=14, fontweight="bold")
    ax.set_ylabel("Amount ($)")
    ax.grid(axis="y", alpha=0.2, linestyle=":")
    ax.legend(frameon=False, loc="upper left")
    ax.set_facecolor("#f8fafc")
    fig.patch.set_facecolor("#f1f5f9")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return chart_to_base64(fig)


def build_recurring_chart(recurring: list[RecurringSnapshot]) -> str:
    top = sorted(recurring, key=lambda r: r.avg_amount, reverse=True)[:8]
    if not top:
        fig, ax = plt.subplots(figsize=(10, 3.2))
        ax.text(0.5, 0.5, "No recurring payees detected yet", ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.patch.set_facecolor("white")
        return chart_to_base64(fig)

    names = [r.payee_name[:24] for r in top][::-1]
    values = [r.avg_amount for r in top][::-1]
    fig, ax = plt.subplots(figsize=(10, 5.2))
    ax.barh(names, values, color="#86efac", alpha=0.9)
    ax.scatter(values, names, s=100, color="#16a34a", zorder=3, edgecolors="white", linewidth=1.0)
    for i, v in enumerate(values):
        ax.text(v, i, f" ${v:,.0f}", va="center", ha="left", fontsize=9, color="#166534")
    ax.set_title("Recurring Spend Candidates (Avg Monthly)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Average spend per month ($)")
    ax.grid(axis="x", alpha=0.2, linestyle=":")
    ax.set_facecolor("#f7fee7")
    fig.patch.set_facecolor("#f0fdf4")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return chart_to_base64(fig)


def build_goal_heatmap(underfunded: list[UnderfundedSnapshot]) -> str:
    top = underfunded[:12]
    if not top:
        fig, ax = plt.subplots(figsize=(10, 3.2))
        ax.text(0.5, 0.5, "No underfunded goal categories detected", ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.patch.set_facecolor("white")
        return chart_to_base64(fig)

    names = [u.category_name[:24] for u in top][::-1]
    funded_ratio = [
        (u.funded_amount / u.target_amount * 100.0) if u.target_amount > 0 else 100.0
        for u in top
    ][::-1]
    colors = []
    for pct in funded_ratio:
        if pct >= 90:
            colors.append("#16a34a")
        elif pct >= 65:
            colors.append("#f59e0b")
        else:
            colors.append("#dc2626")

    fig, ax = plt.subplots(figsize=(10, 5.4))
    ax.barh(names, funded_ratio, color=colors, alpha=0.92)
    for i, v in enumerate(funded_ratio):
        ax.text(v, i, f" {v:.0f}%", va="center", ha="left", fontsize=9, color="#0f172a")
    ax.set_xlim(0, 100)
    ax.set_title("Goal Funding Heatmap (% Funded)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Funded percent of current target")
    ax.axvline(100, color="#334155", linewidth=1.0, linestyle="--", alpha=0.35)
    ax.grid(axis="x", alpha=0.2, linestyle=":")
    ax.set_facecolor("#ecfeff")
    fig.patch.set_facecolor("#f0f9ff")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    return chart_to_base64(fig)


def build_waste_radar_chart(subscriptions: list[RecurringSnapshot], ai: dict) -> str:
    if not subscriptions:
        fig, ax = plt.subplots(figsize=(10, 3.2))
        ax.text(0.5, 0.5, "No subscriptions to score yet", ha="center", va="center", fontsize=12)
        ax.axis("off")
        fig.patch.set_facecolor("white")
        return chart_to_base64(fig)

    assess = build_subscription_assessment_map(ai, subscriptions)
    statuses = ("keep", "review", "waste")
    labels = {"keep": "Keep", "review": "Review", "waste": "Waste"}
    colors = {"keep": "#22c55e", "review": "#f59e0b", "waste": "#ef4444"}
    counts = {k: 0 for k in statuses}
    monthly = {k: 0.0 for k in statuses}

    for s in subscriptions:
        key = normalize_payee_key(s.payee_name)
        level = str(assess.get(key, {}).get("waste_level", "review")).lower()
        if level not in statuses:
            level = "review"
        counts[level] += 1
        monthly[level] += float(s.avg_amount)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 3.9), gridspec_kw={"width_ratios": [1.1, 1.6]})
    fig.patch.set_facecolor("#fffbeb")

    pie_vals = [counts[k] for k in statuses]
    if sum(pie_vals) == 0:
        pie_vals = [1, 0, 0]
    ax1.pie(
        pie_vals,
        labels=[labels[k] for k in statuses],
        colors=[colors[k] for k in statuses],
        startangle=90,
        counterclock=False,
        wedgeprops={"width": 0.42, "edgecolor": "white"},
        textprops={"fontsize": 9},
    )
    ax1.set_title("AI Verdict Mix", fontsize=12, fontweight="bold")
    ax1.set_aspect("equal")

    bars = [monthly[k] for k in statuses]
    y = list(range(len(statuses)))
    ax2.barh(y, bars, color=[colors[k] for k in statuses], alpha=0.9)
    ax2.set_yticks(y, [labels[k] for k in statuses])
    ax2.invert_yaxis()
    ax2.set_title("Monthly Cost by Verdict", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Avg monthly ($)")
    ax2.grid(axis="x", alpha=0.2, linestyle="--")
    ax2.set_facecolor("#fff7ed")
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    for i, v in enumerate(bars):
        ax2.text(v, i, f" ${v:,.2f}", va="center", ha="left", fontsize=9, color="#334155")

    fig.suptitle("AI Waste Radar", fontsize=13, fontweight="bold", y=1.02)
    fig.tight_layout()
    return chart_to_base64(fig)


def render_inline_formatting(text: str) -> str:
    safe = html.escape(text or "")
    # Markdown-like emphasis -> HTML
    safe = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", safe)
    safe = re.sub(r"__(.+?)__", r"<strong>\1</strong>", safe)
    safe = re.sub(r"\*(.+?)\*", r"<em>\1</em>", safe)
    safe = re.sub(r"_(.+?)_", r"<em>\1</em>", safe)
    safe = re.sub(r"~~(.+?)~~", r"<u>\1</u>", safe)
    return safe


def slugify_text(text: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower())
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or "item"


def build_report_html(
    budget_name: str,
    months: list[MonthSnapshot],
    categories: list[CategorySnapshot],
    fixed_bills: list[RecurringSnapshot],
    subscriptions: list[RecurringSnapshot],
    emerging_recurring: list[RecurringSnapshot],
    underfunded: list[UnderfundedSnapshot],
    outliers: list[OutlierSnapshot],
    ai: dict,
    trend_chart_b64: str,
    category_chart_b64: str,
    budget_vs_spend_b64: str,
    recurring_b64: str,
    goal_heatmap_b64: str,
    waste_radar_b64: str,
    dashboard_latest_url: str | None = None,
) -> str:
    subscription_assessment = build_subscription_assessment_map(ai, subscriptions)
    fixed_bill_assessment = build_fixed_bill_assessment_map(ai, fixed_bills)
    goal_assessment = build_goal_assessment_map(ai, underfunded)
    outlier_assessment = build_outlier_assessment_map(ai, outliers)
    category_assessment = build_category_assessment_map(ai, categories)
    money_at_risk = 0.0
    for r in subscriptions:
        level = str(subscription_assessment.get(normalize_payee_key(r.payee_name), {}).get("waste_level", "review")).lower()
        if level in {"review", "waste"}:
            money_at_risk += r.avg_amount
    priority_items: list[tuple[float, str]] = []
    for u in underfunded[:12]:
        info = goal_assessment.get(u.category_name.lower(), {})
        urgency = str(info.get("urgency", "medium")).lower()
        conf = float(info.get("confidence", 0.5))
        urg_mult = {"high": 1.25, "medium": 1.0, "low": 0.7}.get(urgency, 1.0)
        score = u.underfunded_amount * urg_mult * (0.7 + conf * 0.6)
        reason = str(info.get("reason", "Funding gap should be reviewed this week."))
        goal_id = f"goal-{slugify_text(u.category_name)}"
        priority_items.append(
            (
                score,
                f"<li><span class='q-emoji'>🎯</span><strong><a href='#{goal_id}' class='q-link'>Fund {u.category_name}</a></strong> "
                f"<span class='q-meta'>${u.underfunded_amount:,.2f} gap · {urgency.title()} · {conf:.0%}</span><br>"
                f"<span class='ai-note'>{render_inline_formatting(reason)}</span></li>",
            )
        )
    for r in sorted(subscriptions, key=lambda x: x.avg_amount, reverse=True)[:12]:
        info = subscription_assessment.get(normalize_payee_key(r.payee_name), {})
        level = str(info.get("waste_level", "review")).lower()
        if level not in {"waste", "review"}:
            continue
        conf = float(info.get("confidence", 0.5))
        lvl_mult = 1.3 if level == "waste" else 0.9
        score = r.avg_amount * lvl_mult * (0.7 + conf * 0.6)
        reason = str(info.get("reason", "Review value and usage."))
        sub_id = f"sub-{slugify_text(r.payee_name)}"
        priority_items.append(
            (
                score,
                f"<li><span class='q-emoji'>{'🗑️' if level == 'waste' else '🤔'}</span><strong><a href='#{sub_id}' class='q-link'>{r.payee_name}</a></strong> "
                f"<span class='q-meta'>${r.avg_amount:,.2f}/mo · {level.title()} · {conf:.0%}</span><br>"
                f"<span class='ai-note'>{render_inline_formatting(reason)}</span></li>",
            )
        )
    for o in outliers[:12]:
        info = outlier_assessment.get(f"{o.date}|{o.payee_name.lower()}", {})
        conf = float(info.get("confidence", 0.5))
        score = o.amount * (0.6 + conf * 0.7)
        reason = str(info.get("reason", o.reason))
        outlier_id = f"outlier-{slugify_text(o.date + '-' + o.payee_name)}"
        priority_items.append(
            (
                score,
                f"<li><span class='q-emoji'>🚨</span><strong><a href='#{outlier_id}' class='q-link'>{o.payee_name}</a></strong> "
                f"<span class='q-meta'>${o.amount:,.2f} on {o.date} · {conf:.0%}</span><br>"
                f"<span class='ai-note'>{render_inline_formatting(reason)}</span></li>",
            )
        )
    priority_items.sort(key=lambda x: x[0], reverse=True)
    ai_priority_rows = "".join(item[1] for item in priority_items[:7])
    if not ai_priority_rows:
        ai_priority_rows = "<li><span class='q-emoji'>🧭</span><strong>No urgent AI flags this run.</strong></li>"

    top_rows_list: list[str] = []
    for c in sorted(categories, key=lambda x: x.activity, reverse=True)[:12]:
        if c.name.strip().lower() == "inflow: ready to assign":
            continue
        cat_id = f"cat-{slugify_text(c.name)}"
        available = c.variance
        spent = max(0.0, c.activity)
        budget = max(0.0, c.budgeted)
        month_delta = c.month_delta
        over_amount = max(0.0, -month_delta)
        if month_delta < 0:
            avail_class = "pill-bad"
            avail_label = f"Over budget ${over_amount:,.2f}"
        elif month_delta <= max(25.0, c.budgeted * 0.1):
            avail_class = "pill-warn"
            avail_label = f"Tight ${month_delta:,.2f}"
        else:
            avail_class = "pill-good"
            avail_label = f"Avail ${month_delta:,.2f}"
        util = (spent / budget) if budget > 0 else 0.0
        util_pct = max(0.0, min(100.0, util * 100.0))
        util_class = "thermo-good"
        if util > 1.0:
            util_class = "thermo-bad"
        elif util > 0.85:
            util_class = "thermo-warn"

        icon = icon_for_label(c.name)
        prefix = (icon + " ") if icon else ""
        top_rows_list.append(
            f"<tr id='{cat_id}'>"
            f"<td>{prefix}{c.name}</td>"
            f"<td><span class='pill-money'>${c.activity:,.2f}</span></td>"
            f"<td><span class='pill-money'>${c.budgeted:,.2f}</span></td>"
            f"<td><span class='pill {avail_class}'>{avail_label}</span></td>"
            f"<td><div class='thermo'><div class='thermo-fill {util_class}' style='width:{util_pct:.1f}%'></div></div>"
            f"<div class='thermo-meta'>${spent:,.2f} / ${budget:,.2f} "
            f"{((f'({util*100:.0f}% used)') if budget > 0 else '(no budget set)')}"
            f"{(' · OVER by $' + format(over_amount, ',.2f')) if over_amount > 0 else ''}</div></td>"
            f"<td><span class='ai-note'>{render_inline_formatting(str(category_assessment.get(c.name.lower(), {}).get('reason', '')))}</span> "
            f"<span class='conf'>({float(category_assessment.get(c.name.lower(), {}).get('confidence', 0.5)):.0%})</span></td>"
            f"</tr>"
        )
    top_rows = "\n".join(top_rows_list)

    notes = "".join(f"<li>{render_inline_formatting(r)}</li>" for r in ai.get("recommendations", []))
    fixed_rows = "\n".join(
        (
            f"<tr class='row-fixed' id='bill-{slugify_text(r.payee_name)}'><td>{((icon_for_label(r.payee_name, 'fixed_bill') + ' ') if icon_for_label(r.payee_name, 'fixed_bill') else '')}{r.payee_name}</td><td>{r.months_seen}</td>"
            f"<td><span class='pill-money'>${r.avg_amount:,.2f}</span></td><td><span class='pill-money'>${r.latest_amount:,.2f}</span></td>"
            f"<td><span class='pill pill-slate'>{str(fixed_bill_assessment.get(normalize_payee_key(r.payee_name), {}).get('stability_level', 'stable')).title()}</span> "
            f"<span class='conf'>({float(fixed_bill_assessment.get(normalize_payee_key(r.payee_name), {}).get('confidence', 0.5)):.0%})</span><br>"
            f"<span class='ai-note'>{render_inline_formatting(str(fixed_bill_assessment.get(normalize_payee_key(r.payee_name), {}).get('reason', '')))}</span></td></tr>"
        )
        for r in sorted(fixed_bills, key=lambda x: x.avg_amount, reverse=True)[:12]
    )
    subscription_rows_list: list[str] = []
    for r in sorted(subscriptions, key=lambda x: x.avg_amount, reverse=True)[:12]:
        key = normalize_payee_key(r.payee_name)
        ai_sub = subscription_assessment.get(key, {})
        level = str(ai_sub.get("waste_level", "review"))
        reason = str(ai_sub.get("reason", "")).strip() or "No note."
        confidence = float(ai_sub.get("confidence", 0.5))
        if level == "waste":
            badge = "🗑️ Waste"
            row_class = "waste-row"
        elif level == "keep":
            badge = "✅ Keep"
            row_class = "row-sub-keep"
        else:
            badge = "🤔 Review"
            row_class = "row-sub-review"
        badge_class = "pill-good" if level == "keep" else ("pill-bad" if level == "waste" else "pill-warn")
        subscription_rows_list.append(
            f"<tr class='{row_class}' id='sub-{slugify_text(r.payee_name)}'>"
            f"<td>{((icon_for_label(r.payee_name, 'subscription') + ' ') if icon_for_label(r.payee_name, 'subscription') else '')}{r.payee_name}</td><td>{r.months_seen}</td>"
            f"<td><span class='pill-money'>${r.avg_amount:,.2f}</span></td><td><span class='pill-money'>${r.latest_amount:,.2f}</span></td>"
            f"<td><span class='pill {badge_class}'>{badge}</span> <span class='conf'>({confidence:.0%})</span><br><span class='sub-reason'>{render_inline_formatting(reason)}</span></td></tr>"
        )
    subscription_rows = "\n".join(subscription_rows_list)
    emerging_rows = "\n".join(
        (
            f"<tr class='row-emerge'><td>{((icon_for_label(r.payee_name, r.kind) + ' ') if icon_for_label(r.payee_name, r.kind) else '')}{r.payee_name}</td><td>{'Fixed Bill' if r.kind == 'fixed_bill' else 'Subscription'}</td>"
            f"<td>{r.months_seen}</td><td>{r.last_seen_date or '-'}</td><td>${r.avg_amount:,.2f}</td><td>${r.latest_amount:,.2f}</td></tr>"
        )
        for r in sorted(emerging_recurring, key=lambda x: x.avg_amount, reverse=True)[:12]
    )
    underfunded_rows = "\n".join(
        (
            f"<tr class='row-goal' id='goal-{slugify_text(u.category_name)}'><td>{u.category_name}</td><td><span class='pill-money'>${u.target_amount:,.2f}</span></td>"
            f"<td><span class='pill-money'>${u.funded_amount:,.2f}</span></td><td><span class='pill pill-bad'>${u.underfunded_amount:,.2f}</span></td>"
            f"<td><span class='pill {'pill-bad' if str(goal_assessment.get(u.category_name.lower(), {}).get('urgency', 'medium')).lower() == 'high' else ('pill-warn' if str(goal_assessment.get(u.category_name.lower(), {}).get('urgency', 'medium')).lower() == 'medium' else 'pill-good')}'>{str(goal_assessment.get(u.category_name.lower(), {}).get('urgency', 'medium')).title()}</span> "
            f"<span class='conf'>({float(goal_assessment.get(u.category_name.lower(), {}).get('confidence', 0.5)):.0%})</span><br>"
            f"<span class='ai-note'>{render_inline_formatting(str(goal_assessment.get(u.category_name.lower(), {}).get('reason', '')))}</span></td></tr>"
        )
        for u in underfunded[:12]
    )
    outlier_rows = "\n".join(
        (
            f"<tr class='row-alert' id='outlier-{slugify_text(o.date + '-' + o.payee_name)}'><td>{o.date}</td><td>{o.payee_name}</td><td>{o.category_name}</td>"
            f"<td><span class='pill-money'>${o.amount:,.2f}</span></td><td><span class='pill-money'>${o.baseline_mean:,.2f}</span></td><td>{o.reason}</td>"
            f"<td><span class='ai-note'>{render_inline_formatting(str(outlier_assessment.get(f'{o.date}|{o.payee_name.lower()}', {}).get('reason', '')))}</span> "
            f"<span class='conf'>({float(outlier_assessment.get(f'{o.date}|{o.payee_name.lower()}', {}).get('confidence', 0.5)):.0%})</span>"
            + (f"<br><span class='memo'>Memo: {html.escape(o.memo)}</span>" if o.memo else "")
            + "</td></tr>"
        )
        for o in outliers[:12]
    )
    action_cards = "".join(
        (
            f"<div class='action {a.get('priority', 'medium')}'><h4>{a.get('emoji', '🧭')} <strong>{a.get('title', 'Action')}</strong></h4>"
            f"<p>{render_inline_formatting(a.get('detail', ''))}</p></div>"
        )
        for a in ai.get("action_plan", [])
    )
    if not outlier_rows:
        outlier_rows = "<tr class='row-alert'><td colspan='7'>No unusual transactions flagged this month.</td></tr>"

    now = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    month_labels = [m.label for m in months]
    month_spend = [m.spend for m in months]
    total_current = month_spend[-1] if month_spend else 0
    avg_spend = mean(month_spend) if month_spend else 0
    projected = float(ai.get("projected_month_spend", 0))
    underfunded_total = float(ai.get("underfunded_total", 0))
    dashboard_cta = (
        f'<p><a href="{dashboard_latest_url}" style="color:#0f766e;font-weight:700;">Open live dashboard</a></p>'
        if dashboard_latest_url
        else ""
    )

    reference_target = float(ai.get("current_month_budgeted", 0)) if float(ai.get("current_month_budgeted", 0)) > 0 else avg_spend
    reference_label = "Budget Target" if float(ai.get("current_month_budgeted", 0)) > 0 else "Avg Monthly Spend"
    pace_ratio = (projected / reference_target) if reference_target > 0 else 0.0
    pace_status = "On Track"
    pace_class = "good"
    if pace_ratio > 1.10:
        pace_status = "Running Hot"
        pace_class = "bad"
    elif pace_ratio > 0.95:
        pace_status = "Near Limit"
        pace_class = "warn"
    track_max = max(reference_target * 1.25, projected, total_current, 1.0)
    current_pct = min(100.0, (total_current / track_max) * 100.0)
    projected_pct = min(100.0, (projected / track_max) * 100.0)
    target_pct = min(100.0, (reference_target / track_max) * 100.0)
    return f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="color-scheme" content="light only" />
  <meta name="supported-color-schemes" content="light" />
  <style>
    :root {{
      --bg: #f3f8f7;
      --card: #ffffff;
      --ink: #0f172a;
      --muted: #475569;
      --accent: #0f766e;
      --good: #15803d;
      --bad: #b91c1c;
      --border: #dbe4e6;
    }}
    body {{ margin:0; padding:24px; background:linear-gradient(180deg, #eef7f5, #f8fbfa); color:var(--ink); font-family: "Avenir Next", "Segoe UI", sans-serif; -webkit-text-size-adjust:100%; }}
    .container {{ max-width: 980px; margin:0 auto; }}
    .card {{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:18px; margin-bottom:16px; box-shadow: 0 4px 14px rgba(15,118,110,0.06); }}
    h1 {{ margin:0 0 6px; color:var(--accent); }}
    h2 {{ margin:0 0 10px; font-size:20px; }}
    p {{ color:var(--muted); line-height:1.5; }}
    .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:12px; }}
    .kpi {{ background:#f8fffd; border:1px solid var(--border); border-radius:10px; padding:12px; }}
    .kpi .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; }}
    .kpi .value {{ font-size:24px; font-weight:700; margin-top:4px; }}
    .img {{ width:100%; border-radius:10px; border:1px solid var(--border); }}
    table {{ width:100%; border-collapse:collapse; font-size:14px; }}
    th, td {{ padding:10px; border-bottom:1px solid var(--border); text-align:left; }}
    th {{ background:#f0f6f5; color:#0b4b47; }}
    .good {{ color:var(--good); font-weight:700; }}
    .bad {{ color:var(--bad); font-weight:700; }}
    ul {{ margin:8px 0 0 18px; color:#1f2937; }}
    .actions {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:10px; }}
    .action {{ border-radius:10px; border:1px solid var(--border); padding:12px; background:#fff; }}
    .action h4 {{ margin:0 0 6px; }}
    .action p {{ margin:0; color:#1f2937; }}
    .action.high {{ background:#fff2f2; border-color:#fecaca; }}
    .action.medium {{ background:#fff7ed; border-color:#fed7aa; }}
    .action.low {{ background:#ecfdf5; border-color:#bbf7d0; }}
    .pace-card {{ background:linear-gradient(135deg,#f0fdf4,#eff6ff); border:1px solid #dbeafe; }}
    .card-trend {{ background:linear-gradient(180deg,#f0fdfa,#f8fffd); border-color:#99f6e4; }}
    .card-cats {{ background:linear-gradient(180deg,#f0f9ff,#f8fbff); border-color:#bfdbfe; }}
    .card-budget {{ background:linear-gradient(180deg,#f8fafc,#f1f5f9); border-color:#cbd5e1; }}
    .card-fixed {{ background:linear-gradient(180deg,#f7fee7,#f0fdf4); border-color:#bbf7d0; }}
    .card-subs {{ background:linear-gradient(180deg,#fffbeb,#fff7ed); border-color:#fed7aa; }}
    .card-emerging {{ background:linear-gradient(180deg,#eef2ff,#f5f3ff); border-color:#c4b5fd; }}
    .card-goals {{ background:linear-gradient(180deg,#ecfeff,#f0f9ff); border-color:#a5f3fc; }}
    .card-categories {{ background:linear-gradient(180deg,#f8fafc,#f8fafc); border-color:#cbd5e1; }}
    .pace-top {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    .pill {{ display:inline-block; padding:5px 10px; border-radius:999px; font-size:12px; font-weight:700; }}
    .pill.good {{ background:#dcfce7; color:#166534; }}
    .pill.warn {{ background:#fef3c7; color:#92400e; }}
    .pill.bad {{ background:#fee2e2; color:#991b1b; }}
    .pill-good {{ background:#dcfce7; color:#166534; }}
    .pill-warn {{ background:#fef3c7; color:#92400e; }}
    .pill-bad {{ background:#fee2e2; color:#991b1b; }}
    .pill-slate {{ background:#e2e8f0; color:#0f172a; }}
    .pill-money {{ background:#f8fafc; color:#0f172a; border:1px solid #cbd5e1; border-radius:999px; padding:3px 8px; font-weight:700; font-size:12px; }}
    .pace-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:8px; margin-top:10px; }}
    .pace-box {{ background:#fff; border:1px solid #dbe4e6; border-radius:10px; padding:10px; }}
    .pace-box .label {{ color:#475569; font-size:11px; text-transform:uppercase; }}
    .pace-box .value {{ font-weight:800; font-size:20px; margin-top:2px; }}
    .track-wrap {{ margin-top:12px; }}
    .track {{ position:relative; height:14px; border-radius:999px; background:#e2e8f0; overflow:hidden; }}
    .fill-now {{ position:absolute; left:0; top:0; bottom:0; background:#0f766e; }}
    .mark-proj, .mark-target {{ position:absolute; top:-4px; width:2px; height:22px; }}
    .mark-proj {{ background:#ea580c; }}
    .mark-target {{ background:#2563eb; }}
    .legend {{ margin-top:6px; color:#334155; font-size:12px; }}
    .warn {{ color:#92400e; font-weight:700; }}
    .waste-row td {{ background:#fef9c3; }}
    .row-sub-keep td {{ background:#ecfdf5; }}
    .row-sub-review td {{ background:#fffbeb; }}
    .row-fixed td {{ background:#f7fee7; }}
    .row-emerge td {{ background:#f5f3ff; }}
    .row-goal td {{ background:#ecfeff; }}
    .row-alert td {{ background:#fff7ed; }}
    .sub-reason {{ color:#334155; font-size:12px; }}
    .ai-note {{ color:#334155; font-size:12px; }}
    .conf {{ color:#64748b; font-size:12px; }}
    .memo {{ color:#475569; font-size:12px; font-style:italic; }}
    .queue-card {{ background:linear-gradient(180deg,#fff7ed,#fffbeb); border-color:#fdba74; }}
    .queue-list {{ margin:0; padding-left:0; list-style:none; }}
    .queue-list li {{ padding:10px; border:1px solid #fed7aa; border-radius:10px; background:#fff; margin-bottom:8px; }}
    .q-emoji {{ font-size:16px; margin-right:6px; }}
    .q-meta {{ color:#7c2d12; font-size:12px; font-weight:600; margin-left:6px; }}
    .q-link {{ color:#9a3412; text-decoration:none; }}
    .q-link:hover {{ text-decoration:underline; }}
    .thermo {{ width:180px; max-width:100%; height:10px; background:#e2e8f0; border-radius:999px; overflow:hidden; border:1px solid #cbd5e1; }}
    .thermo-fill {{ height:100%; }}
    .thermo-good {{ background:linear-gradient(90deg,#22c55e,#16a34a); }}
    .thermo-warn {{ background:linear-gradient(90deg,#f59e0b,#d97706); }}
    .thermo-bad {{ background:linear-gradient(90deg,#ef4444,#dc2626); }}
    .thermo-meta {{ font-size:11px; color:#475569; margin-top:3px; }}
    .alert-card {{ background:linear-gradient(180deg,#fff1f2,#fff7ed); border:1px solid #fecaca; }}
    @media (max-width: 720px) {{
      body {{ padding:12px; }}
      .card {{ padding:12px; border-radius:10px; }}
      th, td {{ padding:8px; font-size:13px; }}
      table {{ display:block; overflow-x:auto; white-space:nowrap; }}
      .kpi .value {{ font-size:20px; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <h1>{budget_name} Budget Report</h1>
      <p>Generated {now}</p>
      {dashboard_cta}
      <div class="kpis">
        <div class="kpi"><div class="label">Current Month Spend</div><div class="value">${total_current:,.2f}</div></div>
        <div class="kpi"><div class="label">Average Spend ({len(month_labels)} mo)</div><div class="value">${avg_spend:,.2f}</div></div>
        <div class="kpi"><div class="label">Months Included</div><div class="value">{len(month_labels)}</div></div>
        <div class="kpi"><div class="label">Projected This Month</div><div class="value">${projected:,.2f}</div></div>
        <div class="kpi"><div class="label">Still Underfunded</div><div class="value">${underfunded_total:,.2f}</div></div>
      </div>
    </div>

    <div class="card pace-card">
      <h2>⚡ Pace Snapshot</h2>
      <div class="pace-top">
        <span class="pill {pace_class}">{pace_status}</span>
        <span class="muted">Projected vs {reference_label}</span>
      </div>
      <div class="pace-grid">
        <div class="pace-box"><div class="label">Spent So Far</div><div class="value">${total_current:,.2f}</div></div>
        <div class="pace-box"><div class="label">Projected Month-End</div><div class="value">${projected:,.2f}</div></div>
        <div class="pace-box"><div class="label">{reference_label}</div><div class="value">${reference_target:,.2f}</div></div>
      </div>
      <div class="track-wrap">
        <div class="track">
          <div class="fill-now" style="width:{current_pct:.1f}%"></div>
          <div class="mark-proj" style="left:{projected_pct:.1f}%"></div>
          <div class="mark-target" style="left:{target_pct:.1f}%"></div>
        </div>
        <div class="legend">Green fill = current spend, Orange marker = projected, Blue marker = target reference.</div>
      </div>
    </div>

    <div class="card">
      <h2>Clear Action Plan</h2>
      <div class="actions">{action_cards}</div>
      <h3>AI Quick Notes</h3>
      <ul>{notes}</ul>
    </div>

    <div class="card queue-card">
      <h2>AI Priority Queue</h2>
      <p>Highest-impact actions to handle first, ranked by projected impact and model confidence.</p>
      <ul class="queue-list">{ai_priority_rows}</ul>
    </div>

    <div class="card card-trend">
      <h2>Spending Trend</h2>
      <img class="img" alt="Spending trend chart" src="data:image/png;base64,{trend_chart_b64}" />
    </div>

    <div class="card card-cats">
      <h2>Top Categories</h2>
      <img class="img" alt="Top categories chart" src="data:image/png;base64,{category_chart_b64}" />
    </div>

    <div class="card card-budget">
      <h2>Budget vs Spend</h2>
      <img class="img" alt="Budget vs spend chart" src="data:image/png;base64,{budget_vs_spend_b64}" />
    </div>

    <div class="card card-fixed">
      <h2>Fixed Bills</h2>
      <img class="img" alt="Recurring costs chart" src="data:image/png;base64,{recurring_b64}" />
      <table>
        <thead><tr><th>Payee</th><th>Months Seen</th><th>Avg Monthly</th><th>Latest</th><th>AI Bill Signal</th></tr></thead>
        <tbody>{fixed_rows}</tbody>
      </table>
    </div>

    <div class="card card-subs">
      <h2>Subscriptions</h2>
      <p><strong>Money At Risk This Month:</strong> <span class="warn">${money_at_risk:,.2f}</span> (AI-labeled <strong>Review</strong> + <strong>Waste</strong>)</p>
      <img class="img" alt="AI waste radar chart" src="data:image/png;base64,{waste_radar_b64}" />
      <table>
        <thead><tr><th>Payee</th><th>Months Seen</th><th>Avg Monthly</th><th>Latest</th><th>AI Value Verdict</th></tr></thead>
        <tbody>{subscription_rows}</tbody>
      </table>
    </div>

    <div class="card card-emerging">
      <h2>Emerging Recurring Bills (Early Signal)</h2>
      <table>
        <thead><tr><th>Name</th><th>Type</th><th>Months Seen</th><th>Last Seen</th><th>Avg Occurrence</th><th>Latest</th></tr></thead>
        <tbody>{emerging_rows}</tbody>
      </table>
    </div>

    <div class="card card-goals">
      <h2>Underfunded Goal Categories</h2>
      <img class="img" alt="Goal funding heatmap" src="data:image/png;base64,{goal_heatmap_b64}" />
      <table>
        <thead><tr><th>Category</th><th>Target</th><th>Funded</th><th>Still Needed</th><th>AI Goal Triage</th></tr></thead>
        <tbody>{underfunded_rows}</tbody>
      </table>
    </div>

    <div class="card alert-card">
      <h2>Unusual Transactions (Current Month)</h2>
      <table>
        <thead><tr><th>Date</th><th>Payee</th><th>Category</th><th>Amount</th><th>Typical</th><th>Why flagged</th><th>AI Read</th></tr></thead>
        <tbody>{outlier_rows}</tbody>
      </table>
    </div>

    <div class="card card-categories">
      <h2>Category Table</h2>
      <table>
        <thead><tr><th>Category</th><th>Spent</th><th>Budgeted</th><th>Available</th><th>Budget Thermometer</th><th>AI Category Read</th></tr></thead>
        <tbody>{top_rows}</tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""


def build_email_friendly_html(
    budget_name: str,
    metrics: dict,
    ai: dict,
    dashboard_base_url: str,
    report_id: str,
) -> str:
    current_spend = float(metrics.get("current_month_spend", 0))
    projected = float(metrics.get("projected_month_spend", 0))
    current_budget = float(metrics.get("current_month_budgeted", 0))
    average_spend = float(metrics.get("average_spend", 0))
    month_labels = metrics.get("months", [])
    latest_month = month_labels[-1] if month_labels else "Current"
    top_categories = metrics.get("top_categories", [])[:8]
    actions = (ai.get("action_plan", []) or [])[:5]
    if len(actions) < 3:
        actions = (ai.get("action_plan", []) or [])[:3]

    dashboard_latest = f"{dashboard_base_url}/latest"
    report_url = f"{dashboard_base_url}/report/{report_id}"
    reference = current_budget if current_budget > 0 else average_spend
    reference_label = "Budget Target" if current_budget > 0 else "Avg Monthly Spend"
    ratio = (projected / reference) if reference > 0 else 0.0
    pace_label = "On Track"
    pace_bg = "#dcfce7"
    pace_fg = "#166534"
    if ratio > 1.10:
        pace_label = "Running Hot"
        pace_bg = "#fee2e2"
        pace_fg = "#991b1b"
    elif ratio > 0.95:
        pace_label = "Near Limit"
        pace_bg = "#fef3c7"
        pace_fg = "#92400e"

    track_max = max(reference * 1.25, projected, current_spend, 1.0)
    now_pct = max(0.0, min(100.0, current_spend / track_max * 100.0))
    proj_pct = max(0.0, min(100.0, projected / track_max * 100.0))
    target_pct = max(0.0, min(100.0, reference / track_max * 100.0))

    action_tiles = ""
    for a in actions:
        pr = str(a.get("priority", "medium")).lower()
        bg = "#fff7ed"
        bd = "#fed7aa"
        if pr == "high":
            bg = "#fff2f2"
            bd = "#fecaca"
        elif pr == "low":
            bg = "#ecfdf5"
            bd = "#bbf7d0"
        action_tiles += (
            "<tr><td style='padding:0 0 10px 0;'>"
            f"<table role='presentation' width='100%' cellspacing='0' cellpadding='0' style='background:{bg};border:1px solid {bd};border-radius:10px;'>"
            "<tr><td style='padding:10px 12px;'>"
            f"<div style='font-weight:800;color:#0f172a;'>{html.escape(str(a.get('emoji', '🧭')))} {html.escape(str(a.get('title', 'Action')))}</div>"
            f"<div style='margin-top:5px;color:#334155;line-height:1.35;'>{render_inline_formatting(str(a.get('detail', '')))}</div>"
            "</td></tr></table></td></tr>"
        )

    category_rows = ""
    for c in top_categories:
        name = str(c.get("name", ""))
        spent = float(c.get("spent", 0))
        budgeted = float(c.get("budgeted", 0))
        month_delta = float(c.get("month_delta", budgeted - spent))
        available = month_delta

        util = (spent / budgeted) if budgeted > 0 else 0.0
        util_pct = max(0.0, min(100.0, util * 100.0))
        bar_color = "#16a34a"
        if util > 1.0:
            bar_color = "#dc2626"
        elif util > 0.85:
            bar_color = "#d97706"
        over_amount = max(0.0, spent - budgeted)
        avail_color = "#166534" if month_delta >= 0 else "#b91c1c"
        if name.strip().lower() == "inflow: ready to assign":
            continue
        category_rows += (
            "<tr>"
            f"<td style='padding:8px 10px;border-bottom:1px solid #e2e8f0;color:#0f172a;'>{html.escape(name)}</td>"
            f"<td style='padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right;'>${spent:,.2f}</td>"
            f"<td style='padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right;'>${budgeted:,.2f}</td>"
            f"<td style='padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right;color:{avail_color};font-weight:700;'>${month_delta:,.2f}</td>"
            "<td style='padding:8px 10px;border-bottom:1px solid #e2e8f0;'>"
            "<table role='presentation' width='100%' cellspacing='0' cellpadding='0' style='height:8px;background:#e2e8f0;border-radius:99px;overflow:hidden;'>"
            f"<tr><td style='width:{util_pct:.1f}%;background:{bar_color};'></td><td></td></tr></table>"
            f"<div style='font-size:11px;color:#64748b;margin-top:2px;'>${spent:,.0f} / ${budgeted:,.0f} "
            f"{((f'({util*100:.0f}% used)') if budgeted > 0 else '(no budget set)')}"
            f"{(' · OVER by $' + format(over_amount, ',.0f')) if over_amount > 0 else ''}</div>"
            "</td></tr>"
        )

    return f"""<!doctype html>
<html>
  <head>
    <meta name="color-scheme" content="light dark">
    <meta name="supported-color-schemes" content="light dark">
    <style>
      @media (prefers-color-scheme: dark) {{
        .mail-bg {{ background:#0f172a !important; }}
        .mail-card {{ background:#111827 !important; border-color:#334155 !important; }}
        .mail-text {{ color:#e2e8f0 !important; }}
        .mail-muted {{ color:#94a3b8 !important; }}
        .mail-table td {{ border-color:#334155 !important; color:#e2e8f0 !important; }}
        .mail-head td {{ background:#1f2937 !important; color:#e2e8f0 !important; }}
      }}
    </style>
  </head>
  <body style="margin:0;padding:0;background:#f1f5f9;color:#0f172a;font-family:Arial,Helvetica,sans-serif;" class="mail-bg mail-text">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f1f5f9;padding:16px 0;">
      <tr>
        <td align="center">
          <table role="presentation" width="860" cellspacing="0" cellpadding="0" style="width:860px;max-width:95%;background:#ffffff;border:1px solid #dbe4e6;border-radius:12px;overflow:hidden;" class="mail-card">
            <tr>
              <td style="padding:18px 18px 10px 18px;background:#ecfeff;border-bottom:1px solid #dbe4e6;" class="mail-head">
                <div style="font-size:24px;font-weight:800;color:#0f766e;">{html.escape(budget_name)} Budget Report</div>
                <div style="margin-top:4px;color:#475569;" class="mail-muted">{html.escape(latest_month)} snapshot</div>
                <div style="margin-top:8px;"><span style="display:inline-block;padding:5px 10px;border-radius:999px;background:{pace_bg};color:{pace_fg};font-weight:700;font-size:12px;">{pace_label}</span></div>
              </td>
            </tr>
            <tr>
              <td style="padding:14px 18px;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                  <tr>
                    <td style="padding:0 6px 0 0;" width="50%">
                      <a href="{dashboard_latest}" style="display:block;background:#0f766e;color:#ffffff;text-decoration:none;padding:10px 14px;border-radius:8px;font-weight:700;text-align:center;">Open Live Dashboard</a>
                    </td>
                    <td style="padding:0 0 0 6px;" width="50%">
                      <a href="{report_url}" style="display:block;background:#2563eb;color:#ffffff;text-decoration:none;padding:10px 14px;border-radius:8px;font-weight:700;text-align:center;">Open This Report</a>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:0 18px 12px 18px;">
                <div style="font-size:16px;font-weight:800;margin:2px 0 8px 0;">Pace Snapshot</div>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #e2e8f0;border-radius:10px;background:#f8fffd;" class="mail-table">
                  <tr><td style="padding:10px;">
                    <div style="color:#334155;font-size:12px;margin-bottom:6px;">Spent ${current_spend:,.2f} | Projected ${projected:,.2f} | {reference_label} ${reference:,.2f}</div>
                    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="height:14px;background:#e2e8f0;border-radius:99px;overflow:hidden;">
                      <tr>
                        <td style="width:{now_pct:.1f}%;background:#0f766e;"></td>
                        <td></td>
                      </tr>
                    </table>
                    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin-top:5px;">
                      <tr>
                        <td style="font-size:11px;color:#475569;">Now: {now_pct:.0f}%</td>
                        <td style="font-size:11px;color:#ea580c;text-align:center;">Projected: {proj_pct:.0f}%</td>
                        <td style="font-size:11px;color:#2563eb;text-align:right;">Target: {target_pct:.0f}%</td>
                      </tr>
                    </table>
                  </td></tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:0 18px 12px 18px;">
                <div style="font-size:16px;font-weight:800;margin:2px 0 8px 0;">Action Plan</div>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                  {action_tiles or "<tr><td style='padding:10px;'>No action items this run.</td></tr>"}
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:0 18px 18px 18px;">
                <div style="font-size:16px;font-weight:800;margin:2px 0 8px 0;">Category Table</div>
                <div style="font-size:12px;color:#64748b;margin:0 0 6px 0;">Thermometer shows month-to-date <strong>spent vs budgeted</strong>.</div>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border:1px solid #e2e8f0;border-radius:10px;overflow:hidden;" class="mail-table">
                  <tr style="background:#f8fafc;" class="mail-head">
                    <td style="padding:8px 10px;font-weight:700;">Category</td>
                    <td style="padding:8px 10px;text-align:right;font-weight:700;">Spent</td>
                    <td style="padding:8px 10px;text-align:right;font-weight:700;">Budgeted</td>
                    <td style="padding:8px 10px;text-align:right;font-weight:700;">Available</td>
                    <td style="padding:8px 10px;font-weight:700;">Thermometer</td>
                  </tr>
                  {category_rows or "<tr><td colspan='5' style='padding:10px;'>No category data this run.</td></tr>"}
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""


def load_budget_data(ynab_token: str, budget_id: str, months_back: int, min_date: date):
    budget = ynab_request(ynab_token, "GET", f"/budgets/{budget_id}")["data"]["budget"]
    month_data = ynab_request(ynab_token, "GET", f"/budgets/{budget_id}/months")["data"]["months"]

    today = date.today()
    current_month_start = date(today.year, today.month, 1)
    month_data = [
        m
        for m in month_data
        if date.fromisoformat(m["month"]) <= current_month_start
        and date.fromisoformat(m["month"]) >= date(min_date.year, min_date.month, 1)
    ]
    recent = month_data[-months_back:]
    snapshots: list[MonthSnapshot] = []

    for month in recent:
        total_spend = 0.0
        total_budgeted = 0.0
        for c in month.get("categories", []):
            if c.get("hidden") or c.get("deleted"):
                continue
            activity = milli_to_float(c.get("activity", 0))
            budgeted = milli_to_float(c.get("budgeted", 0))
            if activity < 0:
                total_spend += abs(activity)
            if budgeted > 0:
                total_budgeted += budgeted
        snapshots.append(
            MonthSnapshot(
                label=month["month"][:7],
                spend=round(total_spend, 2),
                budgeted=round(total_budgeted, 2),
            )
        )

    if not recent:
        raise RuntimeError("No past/current YNAB month data found for this budget.")

    current = recent[-1]
    categories = []
    for c in current.get("categories", []):
        if c.get("hidden") or c.get("deleted"):
            continue
        activity_raw = milli_to_float(c.get("activity", 0))
        activity = abs(activity_raw) if activity_raw < 0 else 0.0
        budgeted = milli_to_float(c.get("budgeted", 0))
        available = milli_to_float(c.get("balance", 0))
        if activity == 0 and budgeted == 0 and isclose(available, 0.0, abs_tol=0.001):
            continue
        categories.append(CategorySnapshot(name=c["name"], activity=activity, budgeted=budgeted, available=available))

    return budget["name"], snapshots, categories


def load_recurring_candidates(
    ynab_token: str,
    budget_id: str,
    month_labels: list[str],
    since_date: str,
) -> tuple[list[RecurringSnapshot], list[RecurringSnapshot]]:
    if not month_labels:
        return []

    tx_data = ynab_request(
        ynab_token,
        "GET",
        f"/budgets/{budget_id}/transactions?since_date={since_date}",
    )["data"]["transactions"]
    outflow_negative = infer_outflow_negative(tx_data)

    by_payee_amount: dict[str, dict[str, float]] = defaultdict(dict)
    by_payee_count: dict[str, dict[str, int]] = defaultdict(dict)
    by_payee_categories: dict[str, dict[str, float]] = defaultdict(dict)
    by_payee_last_date: dict[str, str] = {}
    by_category_amount: dict[str, dict[str, float]] = defaultdict(dict)
    by_category_count: dict[str, dict[str, int]] = defaultdict(dict)
    by_category_last_date: dict[str, str] = {}
    by_category_canonical_payees: dict[str, set[str]] = defaultdict(set)
    for entry in iter_clean_outflows(tx_data, set(month_labels), outflow_negative):
        payee = entry["payee"]
        month = entry["month"]
        tx_date = entry.get("date", "")
        by_payee_amount[payee][month] = by_payee_amount[payee].get(month, 0.0) + entry["amount"]
        by_payee_count[payee][month] = by_payee_count[payee].get(month, 0) + 1
        if tx_date and tx_date > by_payee_last_date.get(payee, ""):
            by_payee_last_date[payee] = tx_date
        cat = entry["category"]
        by_payee_categories[payee][cat] = by_payee_categories[payee].get(cat, 0.0) + entry["amount"]
        by_category_amount[cat][month] = by_category_amount[cat].get(month, 0.0) + entry["amount"]
        by_category_count[cat][month] = by_category_count[cat].get(month, 0) + 1
        if tx_date and tx_date > by_category_last_date.get(cat, ""):
            by_category_last_date[cat] = tx_date
        by_category_canonical_payees[cat].add(canonical_recurring_name(payee).lower())

    recurring: list[RecurringSnapshot] = []
    emerging: list[RecurringSnapshot] = []
    seen_names: set[str] = set()
    month_count = len(month_labels)
    excluded_keywords = [
        "amazon", "walmart", "target", "costco", "speedway", "shell", "bp", "exxon",
        "mcdonald", "raising cane", "raising canes", "starbucks", "restaurant", "cafe", "doordash", "uber eats",
        "chick-fil-a", "chick fil a",
    ]
    for payee, monthly in by_payee_amount.items():
        months_seen = len(monthly)
        payee_l = payee.lower()
        if any(k in payee_l for k in excluded_keywords):
            continue
        values = list(monthly.values())
        avg_amount_raw = sum(values) / len(values)
        std_raw = pstdev(values) if len(values) > 1 else 0.0
        cv = (std_raw / avg_amount_raw) if avg_amount_raw else 999.0
        monthly_tx_counts = list(by_payee_count.get(payee, {}).values())
        avg_tx_per_month = mean(monthly_tx_counts) if monthly_tx_counts else 0.0
        dominant_category = ""
        if by_payee_categories.get(payee):
            dominant_category = max(by_payee_categories[payee].items(), key=lambda kv: kv[1])[0].lower()
        bill_like, sub_like = classify_recurring_kind(payee_l, dominant_category)
        heuristic_fixed_bill = (
            (not sub_like)
            and months_seen >= 2
            and avg_tx_per_month <= 1.5
            and avg_amount_raw >= 80.0
            and cv <= 0.25
        )
        # Recurring bill heuristic: stable amount, appears in most months, and not high-frequency spending.
        if (sub_like and cv > 1.20) or ((not sub_like) and cv > 0.30):
            continue
        if (not sub_like) and avg_tx_per_month > 2.0:
            continue
        min_months = 2 if sub_like else (2 if bill_like else max(3, month_count - 1))
        if not (bill_like or sub_like or heuristic_fixed_bill):
            continue
        avg_amount = round(sum(monthly.values()) / max(1, months_seen), 2)
        if avg_amount < 5.0:
            continue
        latest_amount = latest_nonzero_amount(monthly, month_labels)
        kind = "subscription" if sub_like else "fixed_bill"
        canonical = canonical_recurring_name(payee)
        target_list = recurring if months_seen >= min_months else emerging
        target_list.append(
            RecurringSnapshot(
                payee_name=canonical,
                kind=kind,
                months_seen=months_seen,
                avg_amount=avg_amount,
                latest_amount=latest_amount,
                canonical_key=canonical.lower(),
                last_seen_date=by_payee_last_date.get(payee, ""),
            )
        )

    for category, monthly in by_category_amount.items():
        canonical = canonical_recurring_name(category)
        cat_l = canonical.lower()
        if cat_l in seen_names:
            continue
        # Mortgage categories often mix HOA and transfer housekeeping; avoid duplicate/incorrect fixed-bill rows.
        if "mortgage" in cat_l:
            continue
        # If one canonical payee dominates this category and has already been captured,
        # don't create a duplicate category-level recurring entry.
        canonical_payees = by_category_canonical_payees.get(category, set())
        if len(canonical_payees) == 1:
            only_payee = next(iter(canonical_payees))
            if only_payee in seen_names and only_payee != cat_l:
                continue
        values = list(monthly.values())
        months_seen = len(values)
        if months_seen < 2:
            continue
        avg_amount_raw = sum(values) / len(values)
        std_raw = pstdev(values) if len(values) > 1 else 0.0
        cv = (std_raw / avg_amount_raw) if avg_amount_raw else 999.0
        monthly_tx_counts = list(by_category_count.get(category, {}).values())
        avg_tx_per_month = mean(monthly_tx_counts) if monthly_tx_counts else 0.0

        bill_like, sub_like = classify_recurring_kind(cat_l, cat_l)
        category_fixed_bill = (
            bill_like
            or ("hoa" in cat_l)
            or ("mortgage" in cat_l)
            or ("electric" in cat_l)
            or ("internet" in cat_l)
            or ("insurance" in cat_l)
            or ("credit" in cat_l)
            or ("loan" in cat_l)
        )
        if sub_like and cv > 1.2:
            continue
        if (not sub_like) and cv > 0.5:
            continue
        if (not sub_like) and avg_tx_per_month > 3.0:
            continue
        if not (category_fixed_bill or sub_like):
            continue
        min_months = 2 if sub_like else 2
        avg_amount = round(sum(monthly.values()) / max(1, months_seen), 2)
        if avg_amount < 8.0:
            continue
        latest_amount = latest_nonzero_amount(monthly, month_labels)
        kind = "subscription" if sub_like else "fixed_bill"
        target_list = recurring if months_seen >= min_months else emerging
        target_list.append(
            RecurringSnapshot(
                payee_name=canonical,
                kind=kind,
                months_seen=months_seen,
                avg_amount=avg_amount,
                latest_amount=latest_amount,
                canonical_key=canonical.lower(),
                last_seen_date=by_category_last_date.get(category, ""),
            )
        )

    # De-duplicate by canonical key.
    dedup_main: dict[str, RecurringSnapshot] = {}
    for r in recurring:
        key = r.canonical_key
        if key not in dedup_main or r.avg_amount > dedup_main[key].avg_amount:
            dedup_main[key] = r
    recurring = list(dedup_main.values())
    seen_names = {r.canonical_key for r in recurring}
    dedup_emerging: dict[str, RecurringSnapshot] = {}
    for r in emerging:
        if r.canonical_key in seen_names:
            continue
        key = r.canonical_key
        if key not in dedup_emerging or r.avg_amount > dedup_emerging[key].avg_amount:
            dedup_emerging[key] = r
    emerging = list(dedup_emerging.values())
    return recurring, emerging


def load_goal_insights(
    ynab_token: str,
    budget_id: str,
) -> tuple[float, list[UnderfundedSnapshot], dict[str, float], dict[str, float], float]:
    data = ynab_request(ynab_token, "GET", f"/budgets/{budget_id}/categories")
    groups = data.get("data", {}).get("category_groups", []) or []

    current_budget_total = 0.0
    underfunded_total = 0.0
    budget_by_category: dict[str, float] = {}
    available_by_category: dict[str, float] = {}
    underfunded: list[UnderfundedSnapshot] = []

    for group in groups:
        if group.get("hidden", False) or group.get("deleted", False):
            continue
        for c in group.get("categories", []) or []:
            if c.get("hidden", False) or c.get("deleted", False):
                continue
            name = (c.get("name") or "").strip()
            if not name:
                continue

            budgeted = max(0.0, milli_to_float(c.get("budgeted", 0)))
            budget_by_category[name] = round(budgeted, 2)
            available_by_category[name] = round(milli_to_float(c.get("balance", 0)), 2)
            current_budget_total += budgeted

            under = max(0.0, milli_to_float(c.get("goal_under_funded", 0)))
            if under > 0:
                target = budgeted + under
                underfunded_total += under
                underfunded.append(
                    UnderfundedSnapshot(
                        category_name=name,
                        goal_type=(c.get("goal_type") or "none"),
                        target_amount=round(target, 2),
                        funded_amount=round(budgeted, 2),
                        underfunded_amount=round(under, 2),
                    )
                )

    underfunded.sort(key=lambda x: x.underfunded_amount, reverse=True)
    return (
        round(current_budget_total, 2),
        underfunded[:15],
        budget_by_category,
        available_by_category,
        round(underfunded_total, 2),
    )


def load_clean_transaction_entries(
    ynab_token: str,
    budget_id: str,
    month_labels: list[str],
    since_date: str,
) -> list[dict]:
    if not month_labels:
        return []
    tx_data = ynab_request(
        ynab_token,
        "GET",
        f"/budgets/{budget_id}/transactions?since_date={since_date}",
    )["data"]["transactions"]
    outflow_negative = infer_outflow_negative(tx_data)
    return list(iter_clean_outflows(tx_data, set(month_labels), outflow_negative))


def build_current_month_categories(
    entries: list[dict],
    current_label: str,
    budget_by_category: dict[str, float],
    available_by_category: dict[str, float],
) -> list[CategorySnapshot]:
    activity_by_category: dict[str, float] = defaultdict(float)
    for e in entries:
        if e.get("month") != current_label:
            continue
        category = str(e.get("category", "")).strip() or "Uncategorized"
        activity_by_category[category] += float(e.get("amount", 0.0))

    names = set(activity_by_category.keys()) | set(budget_by_category.keys()) | set(available_by_category.keys())
    categories: list[CategorySnapshot] = []
    for name in names:
        activity = round(activity_by_category.get(name, 0.0), 2)
        budgeted = round(float(budget_by_category.get(name, 0.0)), 2)
        available = round(float(available_by_category.get(name, 0.0)), 2)
        if activity <= 0 and budgeted <= 0 and isclose(available, 0.0, abs_tol=0.001):
            continue
        categories.append(CategorySnapshot(name=name, activity=activity, budgeted=budgeted, available=available))

    categories.sort(key=lambda c: c.activity, reverse=True)
    return categories


def detect_outliers(entries: list[dict], month_labels: list[str]) -> list[OutlierSnapshot]:
    if not entries or not month_labels:
        return []

    current_month = month_labels[-1]
    recent_months = set(month_labels[-2:]) if len(month_labels) >= 2 else {current_month}
    baseline_by_key: dict[tuple[str, str], list[float]] = defaultdict(list)
    recent_entries: list[dict] = []
    for e in entries:
        key = (e["payee"], e["category"])
        if e["month"] in recent_months:
            recent_entries.append(e)
        else:
            baseline_by_key[key].append(e["amount"])

    all_amounts = sorted(e["amount"] for e in entries)
    if all_amounts:
        idx = max(0, int(len(all_amounts) * 0.9) - 1)
        high_threshold = max(120.0, all_amounts[idx])
    else:
        high_threshold = 120.0

    outliers: list[OutlierSnapshot] = []
    for e in recent_entries:
        key = (e["payee"], e["category"])
        baseline = baseline_by_key.get(key, [])
        reason = ""
        base_mean = 0.0
        if len(baseline) >= 2:
            base_mean = mean(baseline)
            base_std = pstdev(baseline) if len(baseline) > 1 else 0.0
            threshold = base_mean + max(2 * base_std, 25.0)
            if e["amount"] >= max(60.0, threshold):
                reason = "Above normal for this payee/category"
        if not reason and e["amount"] >= high_threshold:
            reason = "One of your highest recent transactions"
        if not reason:
            continue
        outliers.append(
            OutlierSnapshot(
                date=e.get("date", ""),
                payee_name=e["payee"],
                category_name=e["category"],
                amount=round(e["amount"], 2),
                baseline_mean=round(base_mean, 2),
                reason=reason,
                memo=str(e.get("memo", "")),
            )
        )

    outliers.sort(key=lambda x: x.amount, reverse=True)
    return outliers[:12]


def infer_outflow_negative(tx_data: list[dict]) -> bool:
    inflow_amounts = []
    for tx in tx_data:
        if (tx.get("category_name") or "").strip().lower() == "inflow: ready to assign":
            amount = milli_to_float(tx.get("amount", 0))
            if not isclose(amount, 0.0, abs_tol=0.001):
                inflow_amounts.append(amount)
    if not inflow_amounts:
        return True
    return mean(inflow_amounts) > 0


def should_skip_tx(payee: str, category: str, memo: str) -> bool:
    payee_l = payee.lower()
    cat_l = category.lower()
    memo_l = memo.lower()
    if cat_l == "inflow: ready to assign":
        return True
    if "starting balance" in payee_l or "starting balance" in memo_l:
        return True
    if "reconciliation balance adjustment" in payee_l or "reconciliation balance adjustment" in memo_l:
        return True
    if "balance adjustment" in payee_l or "balance adjustment" in memo_l:
        return True
    return False


def iter_clean_outflows(tx_data: list[dict], allowed_months: set[str], outflow_negative: bool):
    for tx in tx_data:
        if tx.get("deleted", False) or tx.get("transfer_account_id"):
            continue
        tx_month = tx.get("date", "")[:7]
        if tx_month not in allowed_months:
            continue

        payee = (tx.get("payee_name") or "").strip() or "Unknown Payee"
        memo = (tx.get("memo") or "").strip()
        subtx = tx.get("subtransactions", []) or []
        if subtx:
            for stx in subtx:
                category = (stx.get("category_name") or "Uncategorized").strip() or "Uncategorized"
                if should_skip_tx(payee, category, memo):
                    continue
                amount = milli_to_float(stx.get("amount", 0))
                is_outflow = amount < 0 if outflow_negative else amount > 0
                if not is_outflow:
                    continue
                if category.lower() in {"uncategorized", "split"} and abs(amount) > 10000:
                    continue
                yield {
                    "date": tx.get("date", ""),
                    "month": tx_month,
                    "payee": payee,
                    "category": category,
                    "amount": abs(amount),
                    "memo": memo,
                }
            continue

        category = (tx.get("category_name") or "Uncategorized").strip() or "Uncategorized"
        if should_skip_tx(payee, category, memo):
            continue
        amount = milli_to_float(tx.get("amount", 0))
        is_outflow = amount < 0 if outflow_negative else amount > 0
        if not is_outflow:
            continue
        if category.lower() in {"uncategorized", "split"} and abs(amount) > 10000:
            continue
        yield {
            "date": tx.get("date", ""),
            "month": tx_month,
            "payee": payee,
            "category": category,
            "amount": abs(amount),
            "memo": memo,
        }


def load_transaction_aggregates(
    ynab_token: str,
    budget_id: str,
    month_labels: list[str],
    since_date: str,
) -> tuple[dict[str, float], dict[str, float]]:
    if not month_labels:
        return {}, {}

    tx_data = ynab_request(
        ynab_token,
        "GET",
        f"/budgets/{budget_id}/transactions?since_date={since_date}",
    )["data"]["transactions"]
    outflow_negative = infer_outflow_negative(tx_data)

    month_totals = {m: 0.0 for m in month_labels}
    category_totals: dict[str, float] = {}
    for entry in iter_clean_outflows(tx_data, set(month_labels), outflow_negative):
        month_totals[entry["month"]] += entry["amount"]
        category = entry["category"]
        category_totals[category] = category_totals.get(category, 0.0) + entry["amount"]

    return (
        {k: round(v, 2) for k, v in month_totals.items()},
        {k: round(v, 2) for k, v in category_totals.items()},
    )


def load_transfer_based_bills(
    ynab_token: str,
    budget_id: str,
    month_labels: list[str],
    since_date: str,
) -> list[RecurringSnapshot]:
    if not month_labels:
        return []

    accounts_data = ynab_request(ynab_token, "GET", f"/budgets/{budget_id}/accounts")
    accounts = accounts_data.get("data", {}).get("accounts", []) or []
    account_name_by_id = {a.get("id"): (a.get("name") or "") for a in accounts}

    tx_data = ynab_request(
        ynab_token,
        "GET",
        f"/budgets/{budget_id}/transactions?since_date={since_date}",
    )["data"]["transactions"]
    monthly_by_name: dict[str, dict[str, float]] = defaultdict(dict)
    last_date_by_name: dict[str, str] = {}
    mortgage_candidates: list[dict[str, float | str]] = []
    seen_transfer_pairs: set[str] = set()
    for tx in tx_data:
        if tx.get("deleted", False):
            continue
        transfer_id = tx.get("transfer_account_id")
        if not transfer_id:
            continue
        tx_id = str(tx.get("id") or "")
        pair_id = str(tx.get("transfer_transaction_id") or "")
        if tx_id and pair_id:
            pair_key = "|".join(sorted([tx_id, pair_id]))
            if pair_key in seen_transfer_pairs:
                continue
            seen_transfer_pairs.add(pair_key)
        month = (tx.get("date") or "")[:7]
        if month not in month_labels:
            continue
        amt = abs(milli_to_float(tx.get("amount", 0)))
        if amt < 500:
            continue
        source_name = (tx.get("account_name") or "").lower()
        target_name = (account_name_by_id.get(transfer_id) or "").lower()
        payee_name = (tx.get("payee_name") or "").lower()
        memo = (tx.get("memo") or "").lower()
        context = " ".join([source_name, target_name, payee_name, memo])

        mortgage_like = any(
            kw in context
            for kw in ("mortgage", "home loan", "loan payment", "condo mortgage", "vip savings")
        )
        if not mortgage_like:
            continue
        mortgage_candidates.append(
            {
                "month": month,
                "date": tx.get("date", ""),
                "amount": amt,
            }
        )

    # Pick one mortgage payment candidate per month using the most stable amount cluster.
    if mortgage_candidates:
        best_center = 0.0
        best_coverage = 0
        best_score = 0.0
        for base in mortgage_candidates:
            center = float(base["amount"])
            tolerance = max(90.0, center * 0.15)
            covered = {
                str(c["month"])
                for c in mortgage_candidates
                if abs(float(c["amount"]) - center) <= tolerance
            }
            coverage = len(covered)
            # Tie-break by preferring lower recurring transfer amount to avoid large sweep transfers.
            score = coverage * 100000.0 - center
            if coverage > best_coverage or (coverage == best_coverage and score > best_score):
                best_center = center
                best_coverage = coverage
                best_score = score

        if best_coverage >= 2:
            tolerance = max(90.0, best_center * 0.15)
            by_month: dict[str, list[dict[str, float | str]]] = defaultdict(list)
            for c in mortgage_candidates:
                if abs(float(c["amount"]) - best_center) <= tolerance:
                    by_month[str(c["month"])].append(c)

            for month, candidates in by_month.items():
                chosen = min(
                    candidates,
                    key=lambda c: (
                        abs(float(c["amount"]) - best_center),
                        str(c.get("date", "")),
                    ),
                )
                monthly_by_name["Mortgage Payment"][month] = float(chosen["amount"])
                tx_date = str(chosen.get("date", ""))
                if tx_date and tx_date > last_date_by_name.get("Mortgage Payment", ""):
                    last_date_by_name["Mortgage Payment"] = tx_date

    out: list[RecurringSnapshot] = []
    for name, monthly in monthly_by_name.items():
        months_seen = len(monthly)
        if months_seen < 2:
            continue
        vals = list(monthly.values())
        avg_amount = round(sum(vals) / months_seen, 2)
        latest_amount = latest_nonzero_amount(monthly, month_labels)
        out.append(
            RecurringSnapshot(
                payee_name=name,
                kind="fixed_bill",
                months_seen=months_seen,
                avg_amount=avg_amount,
                latest_amount=latest_amount,
                canonical_key=name.lower(),
                last_seen_date=last_date_by_name.get(name, ""),
            )
        )
    return out


def merge_recurring(primary: list[RecurringSnapshot], secondary: list[RecurringSnapshot]) -> list[RecurringSnapshot]:
    merged: dict[str, RecurringSnapshot] = {r.canonical_key: r for r in primary}
    for r in secondary:
        existing = merged.get(r.canonical_key)
        if existing is None:
            merged[r.canonical_key] = r
            continue
        # Prefer higher-confidence signal with more months and/or higher amount.
        if (r.months_seen, r.avg_amount) > (existing.months_seen, existing.avg_amount):
            merged[r.canonical_key] = r
    return list(merged.values())


def build_metrics(
    months: list[MonthSnapshot],
    categories: list[CategorySnapshot],
    recurring: list[RecurringSnapshot],
    emerging_recurring: list[RecurringSnapshot],
    underfunded: list[UnderfundedSnapshot],
    outliers: list[OutlierSnapshot],
    underfunded_total: float,
) -> dict:
    month_labels = [m.label for m in months]
    month_spend = [m.spend for m in months]
    month_budgeted = [m.budgeted for m in months]
    top_spend = sorted(categories, key=lambda c: c.activity, reverse=True)[:8]
    overspent = [
        c
        for c in categories
        if (c.available is not None and c.available < -0.01)
        or (c.available is None and c.budgeted > 0 and c.variance < 0)
    ]
    fixed_bills = [r for r in recurring if r.kind == "fixed_bill"]
    subscriptions = [r for r in recurring if r.kind == "subscription"]

    current_label = month_labels[-1] if month_labels else ""
    current_spend = month_spend[-1] if month_spend else 0.0
    projected_month_spend = 0.0
    if current_label and current_spend > 0:
        year = int(current_label[:4])
        month_num = int(current_label[5:7])
        days_in_month = monthrange(year, month_num)[1]
        elapsed = max(1, date.today().day if (year == date.today().year and month_num == date.today().month) else days_in_month)
        projected_month_spend = round((current_spend / elapsed) * days_in_month, 2)

    return {
        "months": month_labels,
        "monthly_spend": month_spend,
        "monthly_budgeted": month_budgeted,
        "current_month_spend": current_spend,
        "average_spend": round(mean(month_spend), 2) if month_spend else 0,
        "current_month_budgeted": month_budgeted[-1] if month_budgeted else 0,
        "projected_month_spend": projected_month_spend,
        "underfunded_total": underfunded_total,
        "top_categories": [
            {
                "name": c.name,
                "spent": c.activity,
                "budgeted": c.budgeted,
                "variance": round(c.variance, 2),
                "available": round(c.available, 2) if c.available is not None else None,
                "month_delta": round(c.month_delta, 2),
            }
            for c in top_spend
        ],
        "overspent_count": len(overspent),
        "largest_overspend": (
            max((abs(c.variance) for c in overspent), default=0)
        ),
        "recurring_candidates": [
            {
                "payee_name": r.payee_name,
                "kind": r.kind,
                "months_seen": r.months_seen,
                "avg_amount": r.avg_amount,
                "latest_amount": r.latest_amount,
            }
            for r in sorted(recurring, key=lambda x: x.avg_amount, reverse=True)[:10]
        ],
        "fixed_bills": [
            {
                "payee_name": r.payee_name,
                "avg_amount": r.avg_amount,
                "months_seen": r.months_seen,
                "latest_amount": r.latest_amount,
                "last_seen_date": r.last_seen_date,
            }
            for r in sorted(fixed_bills, key=lambda x: x.avg_amount, reverse=True)[:10]
        ],
        "subscriptions": [
            {
                "payee_name": r.payee_name,
                "avg_amount": r.avg_amount,
                "months_seen": r.months_seen,
                "latest_amount": r.latest_amount,
                "last_seen_date": r.last_seen_date,
            }
            for r in sorted(subscriptions, key=lambda x: x.avg_amount, reverse=True)[:10]
        ],
        "emerging_recurring": [
            {
                "payee_name": r.payee_name,
                "kind": r.kind,
                "avg_amount": r.avg_amount,
                "months_seen": r.months_seen,
                "latest_amount": r.latest_amount,
                "last_seen_date": r.last_seen_date,
            }
            for r in sorted(emerging_recurring, key=lambda x: x.avg_amount, reverse=True)[:10]
        ],
        "underfunded_categories": [
            {
                "category_name": u.category_name,
                "target_amount": u.target_amount,
                "funded_amount": u.funded_amount,
                "underfunded_amount": u.underfunded_amount,
            }
            for u in underfunded
        ],
        "outlier_transactions": [
            {
                "date": o.date,
                "payee_name": o.payee_name,
                "category_name": o.category_name,
                "amount": o.amount,
                "baseline_mean": o.baseline_mean,
                "reason": o.reason,
                "memo": o.memo,
            }
            for o in outliers
        ],
    }


def derive_action_plan(metrics: dict, subscription_assessment: list[dict] | None = None) -> list[ActionRecommendation]:
    plan: list[ActionRecommendation] = []
    current_spend = float(metrics.get("current_month_spend", 0))
    current_budget = float(metrics.get("current_month_budgeted", 0))
    avg_spend = float(metrics.get("average_spend", 0))
    overspent_count = int(metrics.get("overspent_count", 0))
    recurring = metrics.get("recurring_candidates", [])
    subscriptions = metrics.get("subscriptions", [])
    projected = float(metrics.get("projected_month_spend", 0))
    underfunded = metrics.get("underfunded_categories", [])
    outliers = metrics.get("outlier_transactions", [])
    monthly_spend = [float(x) for x in metrics.get("monthly_spend", [])]
    budget_reliable = current_budget > 0 and (avg_spend <= 0 or current_budget <= max(12000.0, avg_spend * 2.5))

    if budget_reliable and current_spend > current_budget * 1.05:
        over = current_spend - current_budget
        plan.append(
            ActionRecommendation(
                priority="high",
                emoji="🚨",
                title="Monthly Spend Is Over Budget",
                detail=f"Reduce this month by about ${over:,.2f}. Freeze non-essential categories until spend drops below budget.",
            )
        )
    elif budget_reliable:
        remaining = max(0.0, current_budget - current_spend)
        plan.append(
            ActionRecommendation(
                priority="low",
                emoji="✅",
                title="Overall Spend Is Within Budget",
                detail=f"You still have about ${remaining:,.2f} of headroom this month. Keep discretionary spending controlled.",
            )
        )
    elif current_spend > 0:
        plan.append(
            ActionRecommendation(
                priority="medium",
                emoji="🧱",
                title="Budget Target Data Looks Unreliable",
                detail=(
                    "Spend data is solid, but monthly target values look incomplete or inflated. "
                    "Recommendations below prioritize trend and goal underfunding until budget targets stabilize."
                ),
            )
        )

    if budget_reliable and projected > current_budget * 1.1:
        plan.append(
            ActionRecommendation(
                priority="high",
                emoji="⏱️",
                title="Current Pace Projects an Overrun",
                detail=f"At this weekly pace, month-end spend projects around ${projected:,.2f} vs budget ${current_budget:,.2f}. Tighten variable categories now.",
            )
        )

    if overspent_count > 0:
        plan.append(
            ActionRecommendation(
                priority="high",
                emoji="🧯",
                title="Overspent Categories Need Rebalancing",
                detail=f"{overspent_count} categories are currently over budget. Move dollars from low-priority categories today.",
            )
        )

    if underfunded:
        biggest_gap = underfunded[0]
        plan.append(
            ActionRecommendation(
                priority="high",
                emoji="🎯",
                title=f"Underfunded Goal: {biggest_gap.get('category_name', 'Category')}",
                detail=(
                    f"Still short ${float(biggest_gap.get('underfunded_amount', 0)):,.2f}. "
                    "If this goal matters this month, assign dollars before additional discretionary spending."
                ),
            )
        )

    if len(monthly_spend) >= 3:
        baseline = mean(monthly_spend[:-1]) if monthly_spend[:-1] else 0.0
        if baseline > 0 and current_spend > baseline * 1.25:
            plan.append(
                ActionRecommendation(
                    priority="high",
                    emoji="📈",
                    title="Spending Trend Jumped Above Baseline",
                    detail=(
                        f"Current month is tracking above your prior baseline (${baseline:,.2f}). "
                        "Audit discretionary categories this week to stop drift early."
                    ),
                )
            )

    top_categories = metrics.get("top_categories", [])
    top_focus = None
    for c in top_categories:
        name_l = str(c.get("name", "")).strip().lower()
        if name_l in {"uncategorized", "split", "inflow: ready to assign"}:
            continue
        top_focus = c
        break
    if top_focus:
        top = top_focus
        plan.append(
            ActionRecommendation(
                priority="medium",
                emoji="📉",
                title=f"Watch Category: {top.get('name', 'Top Spend')}",
                detail=f"Current spend is ${float(top.get('spent', 0)):,.2f}. Set a weekly cap to prevent end-of-month overrun.",
            )
        )

    if recurring:
        largest = recurring[0]
        plan.append(
            ActionRecommendation(
                priority="medium",
                emoji="🔁",
                title="Review Fixed Bills and Subscriptions",
                detail=(
                    f"Top probable recurring charge is {largest.get('payee_name')} at "
                    f"~${float(largest.get('avg_amount', 0)):,.2f}/month. Confirm value and renegotiate/cancel if needed."
                ),
            )
        )

    assessed_map: dict[str, dict] = {}
    if subscription_assessment:
        for item in subscription_assessment:
            if not isinstance(item, dict):
                continue
            payee = str(item.get("payee_name", "")).strip()
            level = str(item.get("waste_level", "")).strip().lower()
            if not payee or level not in {"keep", "review", "waste"}:
                continue
            assessed_map[normalize_payee_key(payee)] = item

    waste_candidates = []
    for s in subscriptions:
        key = normalize_payee_key(str(s.get("payee_name", "")))
        ai_level = str(assessed_map.get(key, {}).get("waste_level", "")).lower()
        if ai_level == "waste" or (not ai_level and is_wasteful_subscription(str(s.get("payee_name", "")), float(s.get("avg_amount", 0)))):
            waste_candidates.append(s)
    if waste_candidates:
        waste = waste_candidates[0]
        ai_reason = str(assessed_map.get(normalize_payee_key(str(waste.get("payee_name", ""))), {}).get("reason", "")).strip()
        plan.append(
            ActionRecommendation(
                priority="medium",
                emoji="🗑️",
                title="Possible Subscription Waste",
                detail=(
                    f"__{waste.get('payee_name')}__ is averaging **${float(waste.get('avg_amount', 0)):,.2f}/month**. "
                    + (f"{ai_reason} " if ai_reason else "")
                    + "If value is low, pause/cancel and reallocate that money to your priority goals."
                ),
            )
        )

    if outliers:
        biggest = outliers[0]
        plan.append(
            ActionRecommendation(
                priority="medium",
                emoji="🧠",
                title="Notable Transaction Flagged",
                detail=(
                    f"{biggest.get('payee_name')} on {biggest.get('date')} was ${float(biggest.get('amount', 0)):,.2f} "
                    f"vs typical ${float(biggest.get('baseline_mean', 0)):,.2f}. Quick review could prevent pattern drift."
                ),
            )
        )

    if not plan:
        plan.append(
            ActionRecommendation(
                priority="low",
                emoji="🧭",
                title="Need More Transaction History",
                detail="Continue tracking for another month to unlock stronger trend recommendations.",
            )
        )

    return plan[:6]


def write_debug_log(log_path: Path, payload: dict) -> None:
    save_json(log_path, payload)


def filter_active_months(months: list[MonthSnapshot], include_zero_months: bool) -> list[MonthSnapshot]:
    if include_zero_months or not months:
        return months
    current_label = months[-1].label
    filtered = [
        m
        for m in months
        if (m.spend > 0) or (m.budgeted > 0) or (m.label == current_label)
    ]
    if len(filtered) >= 2:
        return filtered
    return months[-2:] if len(months) >= 2 else months


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate and optionally email a YNAB + OpenAI weekly budget report")
    parser.add_argument("--months", type=int, default=int(env_first("REPORT_MONTHS", default="6")), help="How many months of YNAB data to analyze")
    parser.add_argument("--output", default=env_first("REPORT_OUTPUT_HTML", default="reports/weekly_budget_report.html"), help="Output HTML report path")
    parser.add_argument("--email-to", default=env_first("REPORT_EMAIL_TO"), help="If provided, send report to this email")
    parser.add_argument("--subject", default=env_first("REPORT_EMAIL_SUBJECT", default="Weekly Budget Insights"), help="Email subject")
    parser.add_argument(
        "--min-date",
        default=env_first("REPORT_MIN_DATE", default="2025-12-15"),
        help="Do not analyze data earlier than this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--include-zero-months",
        action="store_true",
        default=env_bool("REPORT_INCLUDE_ZERO_MONTHS", default=False),
        help="Include months with zero spend and zero budget in trend math",
    )
    parser.add_argument("--skip-email", action="store_true", help="Generate report but do not send email")
    return parser.parse_args()


def archive_report(output_path: Path, html: str, budget_name: str, metrics: dict, dashboard_base_url: str) -> tuple[str, Path]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_id = stamp
    history_dir = output_path.parent / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    archive_path = history_dir / f"report_{report_id}.html"
    archive_path.write_text(html, encoding="utf-8")

    index_path = output_path.parent / "report_index.json"
    index = load_json(index_path)
    reports = index.get("reports", [])
    reports.append(
        {
            "id": report_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "budget_name": budget_name,
            "current_month_spend": metrics.get("current_month_spend", 0),
            "average_spend": metrics.get("average_spend", 0),
            "overspent_count": metrics.get("overspent_count", 0),
            "months": metrics.get("months", []),
            "path": str(archive_path),
            "dashboard_url": f"{dashboard_base_url}/report/{report_id}",
        }
    )
    index["reports"] = reports[-200:]
    save_json(index_path, index)
    return report_id, archive_path


def main() -> None:
    load_dotenv()
    args = parse_args()
    try:
        min_date = date.fromisoformat(args.min_date)
    except ValueError as e:
        raise RuntimeError("--min-date must be YYYY-MM-DD") from e

    ynab_token = env_first("YNAB_API_KEY", "ynab_api_key")
    budget_id = env_first("YNAB_BUDGET_ID", "ynab_budget_id")
    openai_key = env_first("OPENAI_API_KEY", "openai_api_key")
    openai_model = env_first("OPENAI_MODEL", "openai_model", default="gpt-4o-mini")
    dashboard_base_url = env_first("DASHBOARD_BASE_URL", default="http://127.0.0.1:5001").rstrip("/")
    debug_log_path = Path(env_first("REPORT_DEBUG_LOG", default="logs/weekly_report_debug.json"))

    missing = [
        k
        for k, v in {
            "YNAB_API_KEY/ynab_api_key": ynab_token,
            "YNAB_BUDGET_ID/ynab_budget_id": budget_id,
            "OPENAI_API_KEY/openai_api_key": openai_key,
        }.items()
        if not v
    ]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    budget_name, months, categories = load_budget_data(ynab_token, budget_id, args.months, min_date=min_date)
    all_month_labels = [m.label for m in months]
    month_labels = all_month_labels
    analysis_since = max(min_date.isoformat(), f"{month_labels[0]}-01") if month_labels else min_date.isoformat()
    tx_monthly, tx_categories = load_transaction_aggregates(ynab_token, budget_id, month_labels, since_date=analysis_since)
    tx_entries = load_clean_transaction_entries(ynab_token, budget_id, month_labels, since_date=analysis_since)
    current_budget_total, underfunded, budget_map, available_map, underfunded_total = load_goal_insights(ynab_token, budget_id)
    spend_baseline = mean([v for v in tx_monthly.values() if v > 0]) if any(v > 0 for v in tx_monthly.values()) else 0.0
    effective_budget_target = current_budget_total
    if spend_baseline > 0 and current_budget_total > max(15000.0, spend_baseline * 3.0):
        effective_budget_target = 0.0
    used_tx_fallback = False
    if months and all(isclose(m.spend, 0.0, abs_tol=0.001) for m in months) and sum(tx_monthly.values()) > 0:
        used_tx_fallback = True
        for m in months:
            m.spend = tx_monthly.get(m.label, 0.0)
        categories = [
            CategorySnapshot(
                name=name,
                activity=amt,
                budgeted=budget_map.get(name, 0.0),
                available=available_map.get(name),
            )
            for name, amt in sorted(tx_categories.items(), key=lambda x: x[1], reverse=True)
            if amt > 0
        ]
    months = filter_active_months(months, include_zero_months=args.include_zero_months)
    month_labels = [m.label for m in months]
    if month_labels:
        current_label = month_labels[-1]
        categories_current = build_current_month_categories(tx_entries, current_label, budget_map, available_map)
        if categories_current:
            categories = categories_current
    recurring, emerging_recurring = load_recurring_candidates(ynab_token, budget_id, month_labels, since_date=analysis_since)
    transfer_bills = load_transfer_based_bills(ynab_token, budget_id, month_labels, since_date=analysis_since)
    recurring = merge_recurring(recurring, transfer_bills)
    outliers = detect_outliers(tx_entries, month_labels)
    if months and all(isclose(m.budgeted, 0.0, abs_tol=0.001) for m in months) and effective_budget_target > 0:
        months[-1].budgeted = effective_budget_target
    metrics = build_metrics(months, categories, recurring, emerging_recurring, underfunded, outliers, underfunded_total)
    metrics["memo_highlights"] = extract_memo_highlights(tx_entries, month_labels)
    previous_debug = load_json(debug_log_path)
    metrics["previous_subscription_assessment"] = previous_debug.get("subscription_assessment", [])

    try:
        ai = openai_recommendations(openai_key, openai_model, metrics)
    except Exception as e:
        ai = {
            "executive_summary": (
                "OpenAI insights were unavailable for this run. "
                "The trend and category analytics below are still based on live YNAB data."
            ),
            "wins": [],
            "risks": [f"AI generation failed: {e}"],
            "recommendations": [
                "Retry report generation later to include AI narrative recommendations.",
            ],
            "subscription_assessment": [],
        }
    ai["subscription_assessment"] = stabilize_subscription_assessment(
        ai.get("subscription_assessment", []),
        metrics.get("previous_subscription_assessment", []),
    )
    action_plan = derive_action_plan(metrics, subscription_assessment=ai.get("subscription_assessment", []))
    ai["action_plan"] = [
        {
            "priority": a.priority,
            "emoji": a.emoji,
            "title": a.title,
            "detail": a.detail,
        }
        for a in action_plan
    ]
    ai["projected_month_spend"] = metrics.get("projected_month_spend", 0)
    ai["underfunded_total"] = metrics.get("underfunded_total", 0)
    ai["current_month_budgeted"] = metrics.get("current_month_budgeted", 0)
    month_spend = [m.spend for m in months]
    trend_b64 = build_trend_chart(month_labels, month_spend)
    cat_b64 = build_top_categories_chart(categories)
    budget_vs_spend_b64 = build_budget_vs_spend_chart(months)
    fixed_bills = [r for r in recurring if r.kind == "fixed_bill"]
    subscriptions = [r for r in recurring if r.kind == "subscription"]
    recurring_b64 = build_recurring_chart(fixed_bills)
    goal_heatmap_b64 = build_goal_heatmap(underfunded)
    waste_radar_b64 = build_waste_radar_chart(subscriptions, ai)

    html = build_report_html(
        budget_name=budget_name,
        months=months,
        categories=categories,
        fixed_bills=fixed_bills,
        subscriptions=subscriptions,
        emerging_recurring=emerging_recurring,
        underfunded=underfunded,
        outliers=outliers,
        ai=ai,
        trend_chart_b64=trend_b64,
        category_chart_b64=cat_b64,
        budget_vs_spend_b64=budget_vs_spend_b64,
        recurring_b64=recurring_b64,
        goal_heatmap_b64=goal_heatmap_b64,
        waste_radar_b64=waste_radar_b64,
        dashboard_latest_url=f"{dashboard_base_url}/latest",
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    print(f"Saved report to: {output_path}")
    report_id, archive_path = archive_report(output_path, html, budget_name, metrics, dashboard_base_url)
    write_debug_log(
        debug_log_path,
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "budget_name": budget_name,
            "months": [m.__dict__ for m in months],
            "categories_top_20": [c.__dict__ | {"variance": c.variance} for c in sorted(categories, key=lambda x: x.activity, reverse=True)[:20]],
            "recurring_candidates": [r.__dict__ for r in recurring],
            "emerging_recurring": [r.__dict__ for r in emerging_recurring],
            "transfer_based_bills": [r.__dict__ for r in transfer_bills],
            "fixed_bills": [r.__dict__ for r in fixed_bills],
            "subscriptions": [r.__dict__ for r in subscriptions],
            "underfunded_categories": [u.__dict__ for u in underfunded],
            "outlier_transactions": [o.__dict__ for o in outliers],
            "goal_summary": {
                "current_budget_total": current_budget_total,
                "effective_budget_target": effective_budget_target,
                "spend_baseline": round(spend_baseline, 2),
                "underfunded_total": underfunded_total,
            },
            "include_zero_months": args.include_zero_months,
            "all_month_labels": all_month_labels,
            "analysis_since_date": analysis_since,
            "metrics": metrics,
            "subscription_assessment": ai.get("subscription_assessment", []),
            "fixed_bill_assessment": ai.get("fixed_bill_assessment", []),
            "goal_assessment": ai.get("goal_assessment", []),
            "outlier_assessment": ai.get("outlier_assessment", []),
            "category_assessment": ai.get("category_assessment", []),
            "action_plan": ai.get("action_plan", []),
            "used_transaction_fallback": used_tx_fallback,
            "transaction_monthly_totals": tx_monthly,
            "report_id": report_id,
        },
    )
    print(f"Archived report: {archive_path}")
    print(f"Debug log: {debug_log_path}")
    print(f"Dashboard latest: {dashboard_base_url}/latest")
    print(f"Dashboard report: {dashboard_base_url}/report/{report_id}")

    if args.skip_email or not args.email_to:
        print("Email send skipped.")
        return

    credentials_path = Path(env_first("GMAIL_CREDENTIALS_FILE", default="credentials.json"))
    token_path = Path(env_first("GMAIL_SEND_TOKEN_FILE", default="token_send.json"))
    gmail = get_gmail_send_service(credentials_path=credentials_path, token_path=token_path)
    email_html = build_email_friendly_html(
        budget_name=budget_name,
        metrics=metrics,
        ai=ai,
        dashboard_base_url=dashboard_base_url,
        report_id=report_id,
    )
    send_html_email(gmail, to_email=args.email_to, subject=args.subject, html_body=email_html)
    print(f"Email sent to: {args.email_to}")


if __name__ == "__main__":
    main()
