#!/usr/bin/env python3
import queue
import re
import subprocess
import threading
import tkinter as tk
import os
from pathlib import Path
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

SCRIPT = "03_sync_amazon_to_ynab.py"
BASE_DIR = Path(__file__).resolve().parent
PYTHON_BIN = BASE_DIR / ".venv" / "bin" / "python"


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("YNAB Amazon Sync Launcher")
        self.geometry("900x700")
        self.minsize(760, 560)

        self.run_mode = tk.StringVar(value="dry")
        self.months_back = tk.IntVar(value=1)
        self.months_back_label = tk.StringVar(value="1 month")
        self.lookback_mode = tk.StringVar(value="month")
        self.coverage = tk.StringVar(value="gaps")
        self.depth = tk.StringVar(value="normal")
        self.search_var = tk.StringVar(value="")

        self.output_q: queue.Queue[str] = queue.Queue()
        self.proc_running = False
        self.proc: subprocess.Popen | None = None
        self.spinner_idx = 0
        self.spinner_frames = ["|", "/", "-", "\\"]
        self.search_positions: list[str] = []
        self.search_idx = -1

        self._build_ui()
        self.after(100, self._drain_queue)
        self.after(150, self._animate_status)

    def _build_ui(self):
        root = ttk.Frame(self, padding=14)
        root.pack(fill="both", expand=True)

        title = ttk.Label(root, text="YNAB Amazon Sync", font=("SF Pro Text", 20, "bold"))
        title.pack(anchor="w")
        ttk.Label(root, text="Choose options and run.").pack(anchor="w", pady=(0, 10))

        options = ttk.Frame(root)
        options.pack(fill="x")
        options.columnconfigure(0, weight=1)
        options.columnconfigure(1, weight=1)

        self._radio_group(options, "Run Mode", self.run_mode, [
            ("Dry-run", "dry"),
            ("Apply (write to YNAB)", "apply"),
        ]).grid(row=0, column=0, sticky="nwe", padx=(0, 18), pady=(0, 10))

        self._lookback_group(options).grid(row=0, column=1, sticky="nwe", padx=(0, 0), pady=(0, 10))

        self._radio_group(options, "Coverage", self.coverage, [
            ("Gaps only (fill missing/needs memo)", "gaps"),
            ("All records (reprocess and may overwrite)", "all"),
        ]).grid(row=1, column=0, sticky="nwe", padx=(0, 18), pady=(0, 10))

        self._radio_group(options, "Depth", self.depth, [
            ("Normal (faster, limited fallback)", "normal"),
            ("Deep (slower, exhaustive fallback)", "deep"),
        ]).grid(row=1, column=1, sticky="nwe", padx=(0, 0), pady=(0, 10))

        action_row = ttk.Frame(root)
        action_row.pack(fill="x", pady=(2, 10))

        self.run_btn = ttk.Button(action_row, text="Run", command=self.run_sync)
        self.run_btn.pack(side="left")

        self.abort_btn = ttk.Button(action_row, text="Abort", command=self.abort_run, state="disabled")
        self.abort_btn.pack(side="left", padx=(8, 0))

        clear_btn = ttk.Button(action_row, text="Clear Output", command=self.clear_output)
        clear_btn.pack(side="left", padx=(8, 0))

        close_btn = ttk.Button(action_row, text="Close", command=self.destroy)
        close_btn.pack(side="right")

        self.cmd_label = ttk.Label(root, text="Command: (not run yet)", foreground="#444")
        self.cmd_label.pack(fill="x", pady=(0, 8))

        status_row = ttk.Frame(root)
        status_row.pack(fill="x", pady=(0, 8))
        self.status_label = ttk.Label(status_row, text="Status: idle")
        self.status_label.pack(side="left")
        self.progress = ttk.Progressbar(status_row, mode="indeterminate", length=260)
        self.progress.pack(side="left", padx=(12, 0))

        search_row = ttk.Frame(root)
        search_row.pack(fill="x", pady=(0, 8))
        ttk.Label(search_row, text="Find in output:").pack(side="left")
        self.search_entry = ttk.Entry(search_row, textvariable=self.search_var, width=32)
        self.search_entry.pack(side="left", padx=(8, 6))
        self.search_entry.bind("<Return>", lambda _e: self.find_next())
        self.search_entry.bind("<Shift-Return>", lambda _e: self.find_prev())
        ttk.Button(search_row, text="Prev", command=self.find_prev).pack(side="left")
        ttk.Button(search_row, text="Next", command=self.find_next).pack(side="left", padx=(6, 0))
        ttk.Button(search_row, text="Clear", command=self.clear_search).pack(side="left", padx=(6, 0))
        self.search_status = ttk.Label(search_row, text="", foreground="#666")
        self.search_status.pack(side="left", padx=(10, 0))
        self.search_var.trace_add("write", lambda *_args: self.update_search_highlights())

        self.output = ScrolledText(root, wrap="word", height=28)
        self.output.pack(fill="both", expand=True)
        self.output.configure(font=("Menlo", 10), background="#0f1115", foreground="#d7dae0", insertbackground="#d7dae0")
        self._configure_output_tags()
        self.output.insert("end", "Ready.\n", "info")
        self.output.configure(state="disabled")

        legend = ttk.Label(
            root,
            text="Colors: command (cyan) | section (blue) | success (green) | warning (orange) | error (red)",
            foreground="#555",
        )
        legend.pack(anchor="w", pady=(8, 0))

    def _configure_output_tags(self):
        self.output.tag_configure("info", foreground="#d7dae0")
        self.output.tag_configure("command", foreground="#6bdcff")
        self.output.tag_configure("section", foreground="#8ab4ff", font=("Menlo", 12, "bold"))
        self.output.tag_configure("success", foreground="#61d495")
        self.output.tag_configure("warn", foreground="#ffb86c")
        self.output.tag_configure("error", foreground="#ff6b6b")
        self.output.tag_configure("muted", foreground="#8f96a3")
        self.output.tag_configure("search_hit", background="#f6e58d", foreground="#111111")
        self.output.tag_configure("search_current", background="#ffd166", foreground="#000000")

    def _radio_group(self, parent, title, var, options):
        frame = ttk.LabelFrame(parent, text=title, padding=10)
        for i, (label, value) in enumerate(options):
            ttk.Radiobutton(frame, text=label, value=value, variable=var).grid(row=i, column=0, sticky="w", pady=2)
        return frame

    def _lookback_group(self, parent):
        frame = ttk.LabelFrame(parent, text="Look-back Window", padding=10)
        ttk.Radiobutton(
            frame,
            text="1 week (fast, fewer requests)",
            value="week",
            variable=self.lookback_mode,
            command=self._refresh_lookback_label,
        ).grid(row=0, column=0, sticky="w", pady=(0, 4))
        ttk.Radiobutton(
            frame,
            text="Months",
            value="month",
            variable=self.lookback_mode,
            command=self._refresh_lookback_label,
        ).grid(row=1, column=0, sticky="w")
        scale = ttk.Scale(
            frame,
            from_=1,
            to=12,
            orient="horizontal",
            command=self._on_months_scale,
            length=180,
        )
        scale.set(self.months_back.get())
        scale.grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Label(frame, textvariable=self.months_back_label).grid(row=3, column=0, sticky="w")
        self._refresh_lookback_label()
        return frame

    def _on_months_scale(self, value: str):
        month = int(round(float(value)))
        if month < 1:
            month = 1
        if month > 12:
            month = 12
        self.months_back.set(month)
        self._refresh_lookback_label()

    def _refresh_lookback_label(self):
        if self.lookback_mode.get() == "week":
            self.months_back_label.set("1 week (7 days)")
            return
        month = self.months_back.get()
        self.months_back_label.set(f"{month} month" if month == 1 else f"{month} months")

    def clear_output(self):
        self.output.configure(state="normal")
        self.output.delete("1.0", "end")
        self.output.insert("end", "Ready.\n", "info")
        self.output.configure(state="disabled")
        self.clear_search()

    def _tag_for_line(self, line: str) -> str:
        s = line.strip()
        if not s:
            return "muted"
        if s.startswith("$ "):
            return "command"
        if s.startswith("Traceback") or "ServerNotFoundError" in s or "launcher error" in s.lower():
            return "error"
        if "WARN" in s or "warning" in s.lower():
            return "warn"
        if s.startswith("Log file:") or s.startswith("Gmail candidates:") or s.startswith("YNAB transactions fetched:") or s.startswith("Mode:"):
            return "section"
        if s.startswith("Proposed updates:") or s.startswith("Applied updates:") or s.startswith("Unmatched YNAB transactions:"):
            return "section"
        if re.match(r"^\[exit code:\s*0\]$", s):
            return "success"
        if re.match(r"^\[exit code:\s*[1-9]\d*\]$", s):
            return "error"
        return "info"

    def append_output(self, text: str, tag: str | None = None):
        self.output.configure(state="normal")
        if tag:
            self.output.insert("end", text, tag)
        else:
            for line in text.splitlines(keepends=True):
                self.output.insert("end", line, self._tag_for_line(line))
        self.output.see("end")
        self.output.configure(state="disabled")
        if self.search_var.get().strip():
            self.update_search_highlights()

    def clear_search(self):
        self.search_var.set("")
        self.search_positions = []
        self.search_idx = -1
        self.output.tag_remove("search_hit", "1.0", "end")
        self.output.tag_remove("search_current", "1.0", "end")
        self.search_status.configure(text="")

    def update_search_highlights(self):
        term = self.search_var.get().strip()
        self.output.tag_remove("search_hit", "1.0", "end")
        self.output.tag_remove("search_current", "1.0", "end")
        self.search_positions = []
        self.search_idx = -1
        if not term:
            self.search_status.configure(text="")
            return

        start = "1.0"
        while True:
            pos = self.output.search(term, start, stopindex="end", nocase=1)
            if not pos:
                break
            end = f"{pos}+{len(term)}c"
            self.output.tag_add("search_hit", pos, end)
            self.search_positions.append(pos)
            start = end

        if self.search_positions:
            self.search_status.configure(text=f"{len(self.search_positions)} matches")
            self.search_idx = 0
            self._focus_search_index()
        else:
            self.search_status.configure(text="0 matches")

    def _focus_search_index(self):
        if not self.search_positions:
            return
        pos = self.search_positions[self.search_idx]
        term = self.search_var.get().strip()
        end = f"{pos}+{len(term)}c"
        self.output.tag_remove("search_current", "1.0", "end")
        self.output.tag_add("search_current", pos, end)
        self.output.see(pos)
        self.search_status.configure(text=f"{self.search_idx + 1}/{len(self.search_positions)}")

    def find_next(self):
        if not self.search_var.get().strip():
            self.search_entry.focus_set()
            return
        if not self.search_positions:
            self.update_search_highlights()
            return
        self.search_idx = (self.search_idx + 1) % len(self.search_positions)
        self._focus_search_index()

    def find_prev(self):
        if not self.search_var.get().strip():
            self.search_entry.focus_set()
            return
        if not self.search_positions:
            self.update_search_highlights()
            return
        self.search_idx = (self.search_idx - 1) % len(self.search_positions)
        self._focus_search_index()

    def build_cmd(self):
        python_cmd = str(PYTHON_BIN) if PYTHON_BIN.exists() else "python"
        cmd = [
            python_cmd,
            "-u",
            SCRIPT,
            "--coverage", self.coverage.get(),
            "--depth", self.depth.get(),
        ]
        if self.lookback_mode.get() == "week":
            cmd.extend(["--months-back", "1", "--days-back", "7"])
        else:
            cmd.extend(["--months-back", str(self.months_back.get())])
        if self.run_mode.get() == "apply":
            cmd.append("--apply")
        return cmd

    def run_sync(self):
        if self.proc_running:
            return
        cmd = self.build_cmd()
        cmd_text = " ".join(cmd)
        self.cmd_label.configure(text=f"Command: {cmd_text}")
        self.append_output("\n", "muted")
        self.append_output(f"$ {cmd_text}\n", "command")
        self.proc_running = True
        self.run_btn.configure(state="disabled")
        self.abort_btn.configure(state="normal")
        self.status_label.configure(text="Status: running")
        self.progress.start(10)

        thread = threading.Thread(target=self._run_proc, args=(cmd,), daemon=True)
        thread.start()

    def _run_proc(self, cmd):
        try:
            env = os.environ.copy()
            # Avoid macOS CoreFoundation fork-safety noise in GUI-launched child processes.
            env.setdefault("OBJC_DISABLE_INITIALIZE_FORK_SAFETY", "YES")
            self.proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(BASE_DIR),
                env=env,
            )
            assert self.proc.stdout is not None
            for line in self.proc.stdout:
                self.output_q.put(line)
            rc = self.proc.wait()
            self.output_q.put(f"\n[exit code: {rc}]\n")
        except Exception as e:
            self.output_q.put(f"\n[launcher error] {e}\n")
        finally:
            self.proc = None
            self.output_q.put("__DONE__")

    def abort_run(self):
        if not self.proc_running:
            return
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            self.append_output("\n[abort requested]\n", "warn")
            self.status_label.configure(text="Status: aborting...")

    def _drain_queue(self):
        try:
            while True:
                item = self.output_q.get_nowait()
                if item == "__DONE__":
                    self.proc_running = False
                    self.run_btn.configure(state="normal")
                    self.abort_btn.configure(state="disabled")
                    self.status_label.configure(text="Status: complete")
                    self.progress.stop()
                else:
                    self.append_output(item)
        except queue.Empty:
            pass
        self.after(120, self._drain_queue)

    def _animate_status(self):
        if self.proc_running:
            frame = self.spinner_frames[self.spinner_idx % len(self.spinner_frames)]
            self.spinner_idx += 1
            self.status_label.configure(text=f"Status: running {frame}")
        self.after(150, self._animate_status)


if __name__ == "__main__":
    app = App()
    app.mainloop()
