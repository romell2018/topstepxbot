import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

import tkinter as tk
from tkinter import messagebox, ttk

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency for GUI conveniences
    yaml = None

ROOT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = ROOT_DIR / "Backend"
FRONTEND_URL = "http://127.0.0.1:5000"  # Quart default
PREFS_PATH = BACKEND_DIR / "gui_prefs.json"


def detect_python_interpreter() -> str:
    venv = BACKEND_DIR / ".venv"
    candidates = [
        venv / "bin" / "python",
        venv / "bin" / "python3",
        venv / "Scripts" / "python.exe",
        venv / "Scripts" / "python3.exe",
    ]
    for path in candidates:
        if path.exists():
            return str(path)
    # fallback to current interpreter
    return sys.executable


def load_base_config() -> Dict[str, Any]:
    cfg_path = BACKEND_DIR / "config.yaml"
    if cfg_path.exists() and yaml is not None:
        try:
            with cfg_path.open() as fh:
                data = yaml.safe_load(fh) or {}
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {}


def load_ui_prefs() -> Dict[str, Any]:
    if PREFS_PATH.exists():
        try:
            return json.loads(PREFS_PATH.read_text())
        except Exception:
            pass
    return {}


def save_ui_prefs(data: Dict[str, Any]) -> None:
    try:
        PREFS_PATH.write_text(json.dumps(data, indent=2))
    except Exception:
        pass


class BotDesktopApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("TopstepX Bot Desktop")
        self.root.geometry("760x560")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        base_cfg = load_base_config()
        prefs = load_ui_prefs()

        tsx = base_cfg.get("topstepx", {})
        auth = base_cfg.get("auth", {})

        username = prefs.get("username") or tsx.get("username") or auth.get("username") or ""
        account_id = prefs.get("account_id") or tsx.get("account_id") or auth.get("account_id") or ""
        api_key = ""
        symbol = prefs.get("symbol") or base_cfg.get("symbol") or tsx.get("symbol") or "MNQ"
        api_base = prefs.get("api_base") or base_cfg.get("api_base") or "https://api.topstepx.com"

        self.username_var = tk.StringVar(value=username)
        self.account_var = tk.StringVar(value=str(account_id) if account_id else "")
        self.api_key_var = tk.StringVar(value=api_key)
        self.symbol_var = tk.StringVar(value=str(symbol).upper())
        self.api_base_var = tk.StringVar(value=api_base)

        self.trading_status_var = tk.StringVar(value="Not running")
        self.balance_var = tk.StringVar(value="—")
        self.equity_var = tk.StringVar(value="—")
        self.max_loss_var = tk.StringVar(value="—")
        self.last_price_var = tk.StringVar(value="—")
        self.updated_var = tk.StringVar(value="—")
        self.info_var = tk.StringVar(value="Idle")

        self.bot_process: Optional[subprocess.Popen[str]] = None
        self.log_lines: list[str] = []
        self.poll_job: Optional[str] = None
        self.tail_lock = threading.Lock()

        self.python_interpreter = detect_python_interpreter()
        self.bot_script = str(BACKEND_DIR / "duvenchy_topstepx_projectx_bot.py")

        self._build_layout()

    # UI -----------------------------------------------------------------
    def _build_layout(self) -> None:
        outer = ttk.Frame(self.root, padding=12)
        outer.pack(expand=True, fill=tk.BOTH)

        form = ttk.LabelFrame(outer, text="Credentials", padding=12)
        form.pack(fill=tk.X)

        # Row helpers
        def add_row(row: int, label: str, widget: tk.Widget) -> None:
            lbl = ttk.Label(form, text=label)
            lbl.grid(row=row, column=0, sticky=tk.W, padx=(0, 12), pady=6)
            widget.grid(row=row, column=1, sticky=tk.EW, pady=6)

        username_entry = ttk.Entry(form, textvariable=self.username_var)
        api_key_entry = ttk.Entry(form, textvariable=self.api_key_var, show="*")
        account_entry = ttk.Entry(form, textvariable=self.account_var)
        symbol_entry = ttk.Entry(form, textvariable=self.symbol_var)
        base_entry = ttk.Entry(form, textvariable=self.api_base_var)

        add_row(0, "Username", username_entry)
        add_row(1, "API Key", api_key_entry)
        add_row(2, "Account ID", account_entry)
        add_row(3, "Symbol", symbol_entry)
        add_row(4, "API Base", base_entry)

        form.columnconfigure(1, weight=1)

        button_bar = ttk.Frame(outer, padding=(0, 12, 0, 12))
        button_bar.pack(fill=tk.X)

        self.start_btn = ttk.Button(button_bar, text="Start Bot", command=self.start_bot)
        self.stop_btn = ttk.Button(button_bar, text="Stop Bot", command=self.stop_bot, state=tk.DISABLED)
        self.dashboard_btn = ttk.Button(button_bar, text="Open Dashboard", command=self.open_dashboard)

        self.start_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.stop_btn.pack(side=tk.LEFT, padx=(0, 8))
        self.dashboard_btn.pack(side=tk.LEFT)

        status_frame = ttk.LabelFrame(outer, text="Status", padding=12)
        status_frame.pack(fill=tk.X)

        def status_row(row: int, label: str, var: tk.StringVar) -> None:
            ttk.Label(status_frame, text=label).grid(row=row, column=0, sticky=tk.W)
            ttk.Label(status_frame, textvariable=var, font=("TkDefaultFont", 10, "bold")).grid(row=row, column=1, sticky=tk.W, padx=(12, 0))

        status_row(0, "Trading", self.trading_status_var)
        status_row(1, "Balance", self.balance_var)
        status_row(2, "Equity", self.equity_var)
        status_row(3, "Max Loss", self.max_loss_var)
        status_row(4, "Last Price", self.last_price_var)
        status_row(5, "Updated", self.updated_var)

        status_frame.columnconfigure(1, weight=1)

        info_frame = ttk.Frame(outer, padding=(0, 8, 0, 8))
        info_frame.pack(fill=tk.X)
        ttk.Label(info_frame, textvariable=self.info_var).pack(anchor=tk.W)

        log_frame = ttk.LabelFrame(outer, text="Log", padding=12)
        log_frame.pack(expand=True, fill=tk.BOTH)

        self.log_widget = tk.Text(log_frame, height=12, wrap=tk.WORD, state=tk.DISABLED)
        self.log_widget.pack(expand=True, fill=tk.BOTH)

    # Bot lifecycle ------------------------------------------------------
    def start_bot(self) -> None:
        if self.bot_process and self.bot_process.poll() is None:
            messagebox.showinfo("Bot running", "The bot is already running.")
            return

        username = self.username_var.get().strip()
        api_key = self.api_key_var.get().strip()
        account_id = self.account_var.get().strip()
        symbol = self.symbol_var.get().strip().upper()
        api_base = self.api_base_var.get().strip() or "https://api.topstepx.com"

        if not username or not api_key or not account_id:
            messagebox.showerror("Missing fields", "Username, API key, and Account ID are required.")
            return

        try:
            account_int = int(account_id)
        except ValueError:
            messagebox.showerror("Account ID", "Account ID must be a number.")
            return

        config_payload = load_base_config()
        if not config_payload:
            config_payload = {}
        config_payload.setdefault("api_base", api_base)
        tsx = config_payload.setdefault("topstepx", {})
        tsx.update({
            "username": username,
            "api_key": api_key,
            "account_id": account_int,
            "symbol": symbol,
        })
        config_payload.setdefault("symbol", symbol)
        config_payload.setdefault("market", {})
        auth = config_payload.setdefault("auth", {})
        auth.update({
            "username": username,
            "api_key": api_key,
            "account_id": account_int,
        })
        config_payload["account_id"] = account_int

        # Save prefs without API key for convenience
        save_ui_prefs({
            "username": username,
            "account_id": account_int,
            "symbol": symbol,
            "api_base": api_base,
        })

        env = os.environ.copy()
        if yaml is not None:
            config_blob = yaml.safe_dump(config_payload)
            env["TOPSTEPX_CONFIG"] = config_blob
        else:
            env["TOPSTEPX_CONFIG_JSON"] = json.dumps(config_payload)

        try:
            self.bot_process = subprocess.Popen(
                [self.python_interpreter, self.bot_script],
                cwd=str(BACKEND_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
            )
        except FileNotFoundError:
            messagebox.showerror("Python not found", f"Unable to launch interpreter: {self.python_interpreter}")
            return
        except Exception as exc:
            messagebox.showerror("Launch failed", str(exc))
            return

        self._append_log(f"Bot started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.info_var.set("Bot starting…")
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)

        threading.Thread(target=self._consume_output, daemon=True).start()
        self._schedule_poll()

    def stop_bot(self) -> None:
        if not self.bot_process:
            return
        proc = self.bot_process
        self.bot_process = None
        self.info_var.set("Stopping bot…")
        try:
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
        finally:
            self.info_var.set("Bot stopped")
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            self.trading_status_var.set("Not running")
            self.balance_var.set("—")
            self.equity_var.set("—")
            self.max_loss_var.set("—")
            self.last_price_var.set("—")
            self.updated_var.set("—")
        if self.poll_job:
            self.root.after_cancel(self.poll_job)
            self.poll_job = None

    def open_dashboard(self) -> None:
        import webbrowser
        webbrowser.open(f"{FRONTEND_URL}/")

    def _consume_output(self) -> None:
        if not self.bot_process or not self.bot_process.stdout:
            return
        for line in self.bot_process.stdout:
            clean = line.rstrip()
            if clean:
                self._append_log(clean)
        self._append_log("Bot process finished.")
        self.root.after(0, self.stop_bot)

    def _append_log(self, message: str) -> None:
        with self.tail_lock:
            self.log_lines.append(message)
            if len(self.log_lines) > 500:
                self.log_lines = self.log_lines[-500:]
        self.root.after(0, self._render_log)

    def _render_log(self) -> None:
        self.log_widget.config(state=tk.NORMAL)
        self.log_widget.delete("1.0", tk.END)
        self.log_widget.insert(tk.END, "\n".join(self.log_lines))
        self.log_widget.see(tk.END)
        self.log_widget.config(state=tk.DISABLED)

    def _schedule_poll(self) -> None:
        if self.poll_job:
            self.root.after_cancel(self.poll_job)
        self.poll_job = self.root.after(2000, self._poll_status)

    def _poll_status(self) -> None:
        if not self.bot_process or self.bot_process.poll() is not None:
            self.stop_bot()
            return
        try:
            data = self._fetch_ui_state()
            if data:
                self.trading_status_var.set("Disabled" if data.get("tradingDisabled") else "Active")
                self.balance_var.set(self._fmt_currency(data.get("balance")))
                self.equity_var.set(self._fmt_currency(data.get("equity")))
                self.max_loss_var.set(self._fmt_currency(data.get("maximumLoss")))
                self.last_price_var.set(self._fmt_number(data.get("lastPrice")))
                ts = data.get("timestamp")
                if ts:
                    self.updated_var.set(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)))
                else:
                    self.updated_var.set("—")
                if data.get("error"):
                    self.info_var.set(f"API error: {data['error']}")
                else:
                    self.info_var.set("Connected")
            else:
                self.info_var.set("No data")
        except OSError as exc:
            self.info_var.set(f"No connection: {exc}")
        finally:
            self._schedule_poll()

    @staticmethod
    def _fmt_currency(value: Any) -> str:
        if value is None:
            return "—"
        try:
            return f"${float(value):,.2f}"
        except Exception:
            return str(value)

    @staticmethod
    def _fmt_number(value: Any) -> str:
        if value is None:
            return "—"
        try:
            return f"{float(value):,.2f}"
        except Exception:
            return str(value)

    def _fetch_ui_state(self) -> Optional[Dict[str, Any]]:
        req = urllib.request.Request(f"{FRONTEND_URL}/ui/state", headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status != 200:
                    self.info_var.set(f"Status HTTP {resp.status}")
                    return None
                payload = resp.read().decode("utf-8")
        except urllib.error.URLError as exc:
            raise OSError(str(exc))
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            self.info_var.set("Invalid JSON payload")
            return None

    def on_close(self) -> None:
        if self.bot_process and self.bot_process.poll() is None:
            if not messagebox.askyesno("Exit", "Stop the bot and quit?"):
                return
        self.stop_bot()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    app = BotDesktopApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
