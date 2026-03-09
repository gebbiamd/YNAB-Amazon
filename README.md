# YNAB Weekly Budget Insights (OpenAI + Email + Dashboard)

This project includes a weekly budget reporting workflow that:

1. Pulls month/category data from YNAB
2. Detects trends and variance metrics
3. Uses OpenAI to generate summaries and recommendations
4. Produces a UI-friendly HTML report (color-coded cards, tables, and multiple charts)
5. Archives each report and exposes them in a Flask dashboard
6. Optionally emails the report with links to the dashboard

## Scripts

- `04_weekly_budget_report.py` (generate report, archive, optional email)
- `05_budget_dashboard.py` (serve dashboard and historical reports)
- `run_weekly_budget_report.sh`
- `run_budget_dashboard.sh`

## Quick start

1. Install dependencies:

```bash
uv sync
```

2. Ensure `.env` has (already present in your setup):

- `ynab_api_key`
- `ynab_budget_id`
- `openai_api_key`

3. Start dashboard:

```bash
./run_budget_dashboard.sh
```

4. Generate report without email (manual pull):

```bash
uv run 04_weekly_budget_report.py --skip-email
```

5. Generate report and send email:

```bash
uv run 04_weekly_budget_report.py --email-to "you@example.com"
```

## Dashboard links

Default base URL: `http://127.0.0.1:5001`

- Home: `http://127.0.0.1:5001/`
- Latest report: `http://127.0.0.1:5001/latest`
- Specific report: `http://127.0.0.1:5001/report/<report_id>`

Each report run writes:

- Main output HTML (default `reports/weekly_budget_report.html`)
- Archived HTML (`reports/history/report_<timestamp>.html`)
- Index file (`reports/report_index.json`)

Report includes:

- Spending trend
- Top category spend
- Budget vs spend by month
- Fixed bills and subscriptions (split)
- Underfunded goal categories (save targets vs funded amount)
- Unusual transactions (current-month outlier detection vs your baseline)

## Environment variables

Required:

- `ynab_api_key` or `YNAB_API_KEY`
- `ynab_budget_id` or `YNAB_BUDGET_ID`
- `openai_api_key` or `OPENAI_API_KEY`

Optional report/email:

- `openai_model` or `OPENAI_MODEL` (default: `gpt-4o-mini`)
- `REPORT_MONTHS` (default: `6`)
- `REPORT_OUTPUT_HTML` (default: `reports/weekly_budget_report.html`)
- `REPORT_EMAIL_TO`
- `REPORT_EMAIL_SUBJECT` (default: `Weekly Budget Insights`)
- `GMAIL_CREDENTIALS_FILE` (default: `credentials.json`)
- `GMAIL_SEND_TOKEN_FILE` (default: `token_send.json`)
- `REPORT_DEBUG_LOG` (default: `logs/weekly_report_debug.json`)

Optional dashboard:

- `DASHBOARD_BASE_URL` (default: `http://127.0.0.1:5001`)
- `DASHBOARD_HOST` (default: `127.0.0.1`)
- `DASHBOARD_PORT` (default: `5001`)

## Gmail send setup

If you have only used Gmail read scopes before, run the report once with `--email-to` so OAuth grants `gmail.send` and creates `token_send.json`.

## Weekly scheduling (macOS launchd)

Use the template `com.ynab.weekly-report.plist.example` in this folder.

```bash
cp "/Users/stephengebbiaii/Projects/YNAB Amazon/com.ynab.weekly-report.plist.example" \
  ~/Library/LaunchAgents/com.ynab.weekly-report.plist

launchctl unload ~/Library/LaunchAgents/com.ynab.weekly-report.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.ynab.weekly-report.plist
launchctl list | rg ynab.weekly-report
```

## Practical note about links in email

If your dashboard runs only on your local machine (`127.0.0.1`), links work only on that machine.
If you want links that open from phone or other devices, host the dashboard on a reachable machine/domain and set `DASHBOARD_BASE_URL` accordingly.

## GitHub Actions Automation (Cloud Run)

This repo includes a workflow: `.github/workflows/ynab-sync.yml`

- Manual run: GitHub -> Actions -> `YNAB Amazon Sync` -> `Run workflow`
- Scheduled run: daily at `14:30 UTC` (edit cron in workflow if needed)

### Required GitHub Secrets

Set these in: `Repo -> Settings -> Secrets and variables -> Actions -> Secrets`

- `YNAB_API_KEY`
- `YNAB_BUDGET_ID`
- `GMAIL_CREDENTIALS_JSON` (full contents of `credentials.json`)
- `GMAIL_TOKEN_JSON` (full contents of `token.json`)

Optional secrets:

- `YNAB_ACCOUNT_ID`
- `YNAB_PAYEE_NAME_TO_BE_PROCESSED`
- `YNAB_PAYEE_NAME_PROCESSING_COMPLETED`
- `OPENAI_API_KEY`

### Optional GitHub Variables (for scheduled defaults)

Set these in: `Repo -> Settings -> Secrets and variables -> Actions -> Variables`

- `SYNC_APPLY` = `true` or `false`
- `SYNC_MONTHS_BACK` = `1` / `2` / `3` / `6`
- `SYNC_COVERAGE` = `gaps` / `all`
- `SYNC_DEPTH` = `normal` / `deep`
- `SYNC_DAYS_BACK` = optional integer
- `SYNC_SPLIT_STRATEGY` = `off` / `conservative` / `all`
- `GMAIL_QUERY` = optional Gmail query override

### Important OAuth note

Cloud runs rely on `GMAIL_TOKEN_JSON` refresh token validity. If Google revokes/expires it, regenerate token locally and update the GitHub secret.
