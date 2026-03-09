#!/usr/bin/env python3
import html
import subprocess
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs

HOST = "127.0.0.1"
PORT = 8765
SCRIPT = "03_sync_amazon_to_ynab.py"


def render_page(output: str = "", command: str = "") -> str:
    output_block = ""
    if output:
        output_block = f"""
        <h3>Output</h3>
        <pre>{html.escape(output)}</pre>
        """

    cmd_block = ""
    if command:
        cmd_block = f"<p><strong>Command:</strong> <code>{html.escape(command)}</code></p>"

    return f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>YNAB Amazon Sync</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; margin: 24px; color: #111; }}
    .card {{ max-width: 760px; border: 1px solid #ddd; border-radius: 10px; padding: 18px; }}
    fieldset {{ margin: 14px 0; border: 1px solid #ddd; border-radius: 8px; padding: 10px 12px; }}
    legend {{ padding: 0 6px; font-weight: 600; }}
    label {{ margin-right: 16px; display: inline-block; margin-bottom: 8px; }}
    button {{ background: #111; color: #fff; border: 0; border-radius: 8px; padding: 10px 14px; cursor: pointer; }}
    pre {{ background: #111; color: #eee; border-radius: 8px; padding: 12px; overflow-x: auto; white-space: pre-wrap; }}
    .hint {{ color: #666; font-size: 14px; }}
  </style>
</head>
<body>
  <div class=\"card\">
    <h2>YNAB Amazon Sync</h2>
    <p class=\"hint\">Choose settings and run. Dry-run does not write to YNAB.</p>
    <form method=\"post\" action=\"/run\">
      <fieldset>
        <legend>Run Mode</legend>
        <label><input type=\"radio\" name=\"run_mode\" value=\"dry\" checked> Dry-run</label>
        <label><input type=\"radio\" name=\"run_mode\" value=\"apply\"> Apply (write to YNAB)</label>
      </fieldset>

      <fieldset>
        <legend>Months Back</legend>
        <label><input type=\"radio\" name=\"months_back\" value=\"1\" checked> 1</label>
        <label><input type=\"radio\" name=\"months_back\" value=\"3\"> 3</label>
        <label><input type=\"radio\" name=\"months_back\" value=\"6\"> 6</label>
      </fieldset>

      <fieldset>
        <legend>Coverage</legend>
        <label><input type=\"radio\" name=\"coverage\" value=\"gaps\" checked> gaps</label>
        <label><input type=\"radio\" name=\"coverage\" value=\"all\"> all</label>
      </fieldset>

      <fieldset>
        <legend>Depth</legend>
        <label><input type=\"radio\" name=\"depth\" value=\"normal\" checked> normal</label>
        <label><input type=\"radio\" name=\"depth\" value=\"deep\"> deep</label>
      </fieldset>

      <button type=\"submit\">Run Sync</button>
    </form>
    {cmd_block}
    {output_block}
  </div>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def _send_html(self, body: str):
        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        if self.path != "/":
            self.send_error(404)
            return
        self._send_html(render_page())

    def do_POST(self):
        if self.path != "/run":
            self.send_error(404)
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        form = parse_qs(raw)

        run_mode = (form.get("run_mode", ["dry"])[0] or "dry").strip()
        months_back = (form.get("months_back", ["1"])[0] or "1").strip()
        coverage = (form.get("coverage", ["gaps"])[0] or "gaps").strip()
        depth = (form.get("depth", ["normal"])[0] or "normal").strip()

        if months_back not in {"1", "3", "6"}:
            months_back = "1"
        if coverage not in {"gaps", "all"}:
            coverage = "gaps"
        if depth not in {"normal", "deep"}:
            depth = "normal"

        cmd = [
            "python",
            SCRIPT,
            "--months-back",
            months_back,
            "--coverage",
            coverage,
            "--depth",
            depth,
        ]
        if run_mode == "apply":
            cmd.append("--apply")

        command_str = " ".join(cmd)
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
            output += f"\n\nExit code: {proc.returncode}\n"
        except Exception as e:
            output = f"Failed to run command: {e}"

        self._send_html(render_page(output=output, command=command_str))


def main():
    server = HTTPServer((HOST, PORT), Handler)
    print(f"Open http://{HOST}:{PORT} in your browser")
    server.serve_forever()


if __name__ == "__main__":
    main()
