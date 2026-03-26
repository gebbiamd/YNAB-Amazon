import argparse
import json
import shutil
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def load_reports(index_path: Path) -> list[dict]:
    if not index_path.exists():
        return []
    try:
        data = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    reports = data.get("reports", [])
    reports.sort(key=lambda r: r.get("id", ""), reverse=True)
    return reports


def render_home(reports: list[dict], base_url: str) -> str:
    latest = reports[0] if reports else None
    latest_report_href = f"{base_url}/report/{latest['id']}/" if latest else ""
    latest_href = f"{base_url}/latest/"
    rows = []
    for r in reports:
        rows.append(
            "<tr>"
            f"<td>{r.get('created_at', '')}</td>"
            f"<td>{r.get('budget_name', '')}</td>"
            f"<td>${float(r.get('current_month_spend') or 0):,.2f}</td>"
            f"<td>${float(r.get('average_spend') or 0):,.2f}</td>"
            f"<td>{int(r.get('overspent_count') or 0)}</td>"
            f"<td><a href=\"{base_url}/report/{r.get('id','')}/\">Open</a></td>"
            "</tr>"
        )

    latest_block = (
        f"""
      <div class="actions">
        <a class="btn" href="{latest_href}">Open Latest Report</a>
        <a class="btn secondary" href="{latest_report_href}">Open Report {latest.get('id','')}</a>
      </div>
      <div class="kpis">
        <div class="kpi"><div class="label">Latest Run</div><div class="value small">{latest.get('created_at','')}</div></div>
        <div class="kpi"><div class="label">Budget</div><div class="value medium">{latest.get('budget_name','')}</div></div>
        <div class="kpi"><div class="label">Current Spend</div><div class="value">${float(latest.get('current_month_spend') or 0):,.2f}</div></div>
        <div class="kpi"><div class="label">Avg Spend</div><div class="value">${float(latest.get('average_spend') or 0):,.2f}</div></div>
      </div>
        """
        if latest
        else "<p>No reports published yet.</p>"
    )

    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>YNAB Budget Reports</title>
  <style>
    :root {{
      --bg:#eef6f4;
      --card:#ffffff;
      --ink:#0f172a;
      --muted:#475569;
      --accent:#0f766e;
      --accent2:#2563eb;
      --border:#dbe4e6;
    }}
    body {{
      margin:0;
      color:var(--ink);
      font-family:"Avenir Next","Segoe UI",sans-serif;
      background:
        radial-gradient(1200px 400px at 20% -10%, #d1fae5 0%, transparent 60%),
        radial-gradient(900px 300px at 90% 0%, #dbeafe 0%, transparent 50%),
        var(--bg);
    }}
    .wrap {{ max-width:1100px; margin:0 auto; padding:24px; }}
    .card {{
      background:var(--card);
      border:1px solid var(--border);
      border-radius:16px;
      padding:18px;
      margin-bottom:14px;
      box-shadow:0 8px 22px rgba(15,118,110,.08);
    }}
    .hero {{
      background:linear-gradient(135deg,#ecfeff,#f0fdf4);
      border-color:#bae6fd;
    }}
    h1 {{ margin:0; color:var(--accent); font-size:40px; line-height:1.05; letter-spacing:-.02em; }}
    h3 {{ margin:0 0 8px; font-size:22px; line-height:1.1; }}
    .muted {{ color:var(--muted); }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    .btn {{
      display:inline-block;
      border:0;
      border-radius:10px;
      padding:11px 14px;
      font-weight:800;
      text-decoration:none;
      color:#fff;
      background:var(--accent);
    }}
    .btn.secondary {{ background:var(--accent2); }}
    .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:10px; margin-top:14px; }}
    .kpi {{
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px 12px;
      background:#ffffff;
    }}
    .kpi .label {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.06em; }}
    .kpi .value {{ margin-top:4px; font-size:24px; font-weight:900; color:#0b4b47; }}
    .kpi .value.small {{ font-size:16px; }}
    .kpi .value.medium {{ font-size:20px; }}
    .pill {{
      display:inline-block;
      padding:3px 8px;
      border-radius:999px;
      font-size:11px;
      font-weight:800;
      background:#ecfeff;
      color:#0e7490;
      border:1px solid #bae6fd;
    }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ border-bottom:1px solid var(--border); padding:10px; text-align:left; }}
    th {{ background:#f0f7f6; color:#0b4b47; font-size:13px; text-transform:uppercase; letter-spacing:.04em; }}
    a {{ color:#0f766e; font-weight:800; text-decoration:none; }}
    a:hover {{ text-decoration:underline; }}
    code {{ background:#e2e8f0; padding:2px 6px; border-radius:6px; }}
    @media (max-width:760px) {{
      h1 {{ font-size:30px; }}
      .wrap {{ padding:14px; }}
      .kpi .value {{ font-size:20px; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card hero">
      <h1>YNAB Budget Reports</h1>
      <p class="muted">Static weekly report archive published from GitHub Actions.</p>
      {latest_block}
    </div>
    <div class="card">
      <h3>Report History <span class="pill">{len(reports)} runs</span></h3>
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
          {''.join(rows) or '<tr><td colspan="6">No reports published yet.</td></tr>'}
        </tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""


def render_redirect(target: str, title: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <meta http-equiv="refresh" content="0; url={target}" />
  <link rel="canonical" href="{target}" />
  <title>{title}</title>
</head>
<body>
  <p>Redirecting to <a href="{target}">{target}</a>...</p>
</body>
</html>
"""


def build_site(report_dir: Path, site_dir: Path, base_url: str) -> None:
    if site_dir.exists():
        shutil.rmtree(site_dir)
    site_dir.mkdir(parents=True, exist_ok=True)

    reports = load_reports(report_dir / "report_index.json")
    history_dir = report_dir / "history"
    report_index_out = {"reports": []}

    for report in reports:
        report_id = str(report.get("id", ""))
        src = history_dir / f"report_{report_id}.html"
        if not src.exists():
            continue

        report_html = src.read_text(encoding="utf-8")
        report_dir_out = site_dir / "report" / report_id
        report_dir_out.mkdir(parents=True, exist_ok=True)
        (report_dir_out / "index.html").write_text(report_html, encoding="utf-8")
        (site_dir / "report" / f"{report_id}.html").write_text(report_html, encoding="utf-8")

        report_copy = dict(report)
        report_copy["dashboard_url"] = f"{base_url}/report/{report_id}/"
        report_copy["path"] = f"report/{report_id}/index.html"
        report_index_out["reports"].append(report_copy)

    latest_dir = site_dir / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    if report_index_out["reports"]:
        latest_id = report_index_out["reports"][0]["id"]
        latest_target = f"{base_url}/report/{latest_id}/"
        latest_html = render_redirect(latest_target, "Latest Budget Report")
        (latest_dir / "index.html").write_text(latest_html, encoding="utf-8")
        (site_dir / "latest.html").write_text(latest_html, encoding="utf-8")

    (site_dir / "index.html").write_text(render_home(report_index_out["reports"], base_url), encoding="utf-8")
    (site_dir / "report_index.json").write_text(json.dumps(report_index_out, ensure_ascii=True, indent=2), encoding="utf-8")
    (site_dir / ".nojekyll").write_text("", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a static YNAB budget site for GitHub Pages.")
    parser.add_argument("--report-dir", default="reports", help="Directory containing report_index.json and history/")
    parser.add_argument("--site-dir", default="site", help="Output directory for static site files")
    parser.add_argument("--base-url", default="", help="Base URL for published links, no trailing slash")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report_dir = (BASE_DIR / args.report_dir).resolve()
    site_dir = (BASE_DIR / args.site_dir).resolve()
    base_url = args.base_url.rstrip("/")
    build_site(report_dir, site_dir, base_url)
    print(f"Built static site: {site_dir}")


if __name__ == "__main__":
    main()
