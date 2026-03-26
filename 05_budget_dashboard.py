import json
import os
import shutil
import subprocess
from pathlib import Path

from flask import Flask, abort, make_response, redirect, render_template_string, request


BASE_DIR = Path(__file__).resolve().parent
REPORT_DIR = BASE_DIR / "reports"
INDEX_PATH = REPORT_DIR / "report_index.json"
HISTORY_DIR = REPORT_DIR / "history"

app = Flask(__name__)


def load_reports() -> list[dict]:
    if not INDEX_PATH.exists():
        return []
    try:
        data = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    reports = data.get("reports", [])
    reports.sort(key=lambda r: r.get("id", ""), reverse=True)
    return reports


@app.route("/", methods=["GET", "POST"])
def dashboard_home():
    reports = load_reports()
    latest = reports[0] if reports else None
    run_output = ""
    run_ok = None
    run_latest_id = ""
    requested_email = os.getenv("REPORT_EMAIL_TO", "")
    send_email_checked = True

    if request.method == "POST":
        send_email_checked = request.form.get("send_email") == "on"
        requested_email = (request.form.get("email_to") or "").strip()
        cmd = []
        if shutil.which("uv"):
            cmd = ["uv", "run", "04_weekly_budget_report.py"]
        else:
            cmd = ["python3", "04_weekly_budget_report.py"]
        if not send_email_checked:
            cmd.append("--skip-email")
        elif requested_email:
            cmd.extend(["--email-to", requested_email])

        try:
            proc = subprocess.run(
                cmd,
                cwd=str(BASE_DIR),
                capture_output=True,
                text=True,
                timeout=300,
            )
            run_ok = proc.returncode == 0
            run_output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
        except Exception as e:
            run_ok = False
            run_output = f"Failed to run report: {e}"
        reports = load_reports()
        latest = reports[0] if reports else None
        run_latest_id = str(latest.get("id", "")) if latest else ""

    html = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg:#eef6f4;
      --card:#ffffff;
      --ink:#0f172a;
      --muted:#475569;
      --accent:#0f766e;
      --accent2:#2563eb;
      --border:#dbe4e6;
    }
    body {
      margin:0;
      color:var(--ink);
      font-family:"Avenir Next","Segoe UI",sans-serif;
      background:
        radial-gradient(1200px 400px at 20% -10%, #d1fae5 0%, transparent 60%),
        radial-gradient(900px 300px at 90% 0%, #dbeafe 0%, transparent 50%),
        var(--bg);
    }
    .wrap { max-width:1100px; margin:0 auto; padding:24px; }
    .card {
      background:var(--card);
      border:1px solid var(--border);
      border-radius:16px;
      padding:18px;
      margin-bottom:14px;
      box-shadow:0 8px 22px rgba(15,118,110,.08);
    }
    .hero {
      background:linear-gradient(135deg,#ecfeff,#f0fdf4);
      border-color:#bae6fd;
    }
    h1 { margin:0; color:var(--accent); font-size:40px; line-height:1.05; letter-spacing:-.02em; }
    h2 { margin:0 0 12px; font-size:34px; line-height:1.05; letter-spacing:-.02em; }
    h3 { margin:0 0 8px; font-size:22px; line-height:1.1; }
    .muted { color:var(--muted); }
    .actions { display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }
    .btn {
      display:inline-block;
      border:0;
      border-radius:10px;
      padding:11px 14px;
      font-weight:800;
      cursor:pointer;
      text-decoration:none;
      color:#fff;
      background:var(--accent);
    }
    .btn.secondary { background:var(--accent2); }
    .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:10px; margin-top:14px; }
    .kpi {
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px 12px;
      background:#ffffff;
    }
    .kpi .label { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.06em; }
    .kpi .value { margin-top:4px; font-size:24px; font-weight:900; color:#0b4b47; }
    .run-card { background:linear-gradient(180deg,#f8fafc,#ffffff); }
    .run-grid { display:grid; grid-template-columns:1fr; gap:12px; align-items:start; }
    .run-primary { width:100%; font-size:18px; padding:14px 16px; border-radius:12px; }
    .run-controls { display:grid; grid-template-columns:220px 1fr; gap:12px; align-items:center; }
    .field label { display:block; font-size:12px; text-transform:uppercase; letter-spacing:.05em; color:#475569; margin-bottom:6px; }
    input[type="text"] { width:100%; padding:10px; border:1px solid var(--border); border-radius:10px; font-size:14px; }
    .toggle { display:inline-flex; align-items:center; gap:8px; font-weight:700; color:#0b4b47; margin-bottom:10px; }
    .run-note { font-size:12px; color:#64748b; margin-top:8px; }
    pre { background:#0b1320; color:#d1fae5; padding:12px; border-radius:10px; overflow:auto; max-height:320px; border:1px solid #1e293b; }
    .ok { color:#166534; font-weight:800; }
    .bad { color:#b91c1c; font-weight:800; }
    table { width:100%; border-collapse:collapse; }
    th, td { border-bottom:1px solid var(--border); padding:10px; text-align:left; }
    th { background:#f0f7f6; color:#0b4b47; font-size:13px; text-transform:uppercase; letter-spacing:.04em; }
    a { color:#0f766e; font-weight:800; text-decoration:none; }
    a:hover { text-decoration:underline; }
    .pill {
      display:inline-block;
      padding:3px 8px;
      border-radius:999px;
      font-size:11px;
      font-weight:800;
      background:#ecfeff;
      color:#0e7490;
      border:1px solid #bae6fd;
    }
    @media (max-width:760px) {
      h1 { font-size:30px; }
      h2 { font-size:26px; }
      .run-grid { grid-template-columns:1fr; }
      .run-controls { grid-template-columns:1fr; }
      .wrap { padding:14px; }
      .kpi .value { font-size:20px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card hero">
      <h1>YNAB Budget Dashboard</h1>
      <p class="muted">Run reports, email results, and open history from one place.</p>
      {% if latest %}
      <div class="actions">
        <a class="btn" href="/latest/">Open Latest Report</a>
        <a class="btn secondary" href="/report/{{ latest.id }}/">Open Report {{ latest.id }}</a>
      </div>
      <div class="kpis">
        <div class="kpi"><div class="label">Latest Run</div><div class="value" style="font-size:16px">{{ latest.created_at }}</div></div>
        <div class="kpi"><div class="label">Budget</div><div class="value" style="font-size:20px">{{ latest.budget_name }}</div></div>
        <div class="kpi"><div class="label">Current Spend</div><div class="value">${{ "{:,.2f}".format(latest.current_month_spend or 0) }}</div></div>
        <div class="kpi"><div class="label">Avg Spend</div><div class="value">${{ "{:,.2f}".format(latest.average_spend or 0) }}</div></div>
      </div>
      {% else %}
      <p>No reports yet. Run <code>04_weekly_budget_report.py</code> first.</p>
      {% endif %}
    </div>

    <div class="card run-card">
      <h2>Run Weekly Report</h2>
      <form method="post">
        <div class="run-grid">
          <button class="btn run-primary" type="submit">Run Weekly Report</button>
          <div class="run-controls">
            <label class="toggle"><input type="checkbox" name="send_email" {% if send_email_checked %}checked{% endif %}/> Send email after run</label>
            <div class="field">
              <label>Email Recipient (Optional)</label>
              <input type="text" name="email_to" value="{{ requested_email }}" placeholder="gebbiamd@gmail.com" />
            </div>
          </div>
        </div>
      </form>
      {% if run_ok is not none %}
        <p class="{{ 'ok' if run_ok else 'bad' }}">{{ 'Run completed' if run_ok else 'Run failed' }}</p>
        {% if run_ok and run_latest_id %}
          <p><a class="btn secondary" href="/report/{{ run_latest_id }}/">Open Newly Generated Report</a></p>
        {% endif %}
        <pre>{{ run_output }}</pre>
      {% endif %}
    </div>

    <div class="card">
      <h3>Report History <span class="pill">{{ reports|length }} runs</span></h3>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Budget</th>
            <th>Current Month Spend</th>
            <th>Average Spend</th>
            <th>Overspent Categories</th>
            <th>Link</th>
          </tr>
        </thead>
        <tbody>
          {% for r in reports %}
          <tr>
            <td>{{ r.created_at }}</td>
            <td>{{ r.budget_name }}</td>
            <td>${{ "{:,.2f}".format(r.current_month_spend or 0) }}</td>
            <td>${{ "{:,.2f}".format(r.average_spend or 0) }}</td>
            <td>{{ r.overspent_count or 0 }}</td>
            <td><a href="/report/{{ r.id }}/">Open</a></td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""
    rendered = render_template_string(
        html,
        reports=reports,
        latest=latest,
        run_ok=run_ok,
        run_output=run_output,
        requested_email=requested_email,
        send_email_checked=send_email_checked,
        run_latest_id=run_latest_id,
    )
    resp = make_response(rendered)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/latest")
@app.route("/latest/")
def dashboard_latest():
    reports = load_reports()
    if not reports:
        return redirect("/")
    return redirect(f"/report/{reports[0]['id']}/")


@app.route("/report/<report_id>")
@app.route("/report/<report_id>/")
def dashboard_report(report_id: str):
    normalized = report_id.replace("_", "")
    if not normalized.isdigit():
        abort(404)
    report_path = HISTORY_DIR / f"report_{report_id}.html"
    if not report_path.exists():
        abort(404)
    return report_path.read_text(encoding="utf-8")


if __name__ == "__main__":
    host = os.getenv("DASHBOARD_HOST", "127.0.0.1")
    port = int(os.getenv("DASHBOARD_PORT", "5001"))
    app.run(host=host, port=port, debug=False)
