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
        # App identity and mode
        self.app_name = "Pine MNQ Strategy"
        # When True, the app expects a local HTTP server (Quart) to poll state and toggle trading
        # Pine strategy runner does not expose a server, so disable these features
        self.server_enabled = False

        self.root.title(self.app_name)
        self.root.geometry("820x660")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.root.minsize(720, 600)

        base_cfg = load_base_config()
        prefs = load_ui_prefs()

        tsx = base_cfg.get("topstepx", {})
        auth = base_cfg.get("auth", {})

        username = prefs.get("username") or tsx.get("username") or auth.get("username") or ""
        account_id = prefs.get("account_id") or tsx.get("account_id") or auth.get("account_id") or ""
        api_key = ""
        symbol = prefs.get("symbol") or base_cfg.get("symbol") or tsx.get("symbol") or "MNQ"
        api_base = prefs.get("api_base") or base_cfg.get("api_base") or "https://api.topstepx.com"
        strategy_cfg = (base_cfg.get("strategy") or {})
        max_contracts_default = prefs.get("max_contracts") or strategy_cfg.get("contractSizeMax")
        trail_ticks_default = prefs.get("trail_distance_ticks") or strategy_cfg.get("trailDistanceTicks")

        self.username_var = tk.StringVar(value=username)
        self.account_var = tk.StringVar(value=str(account_id) if account_id else "")
        self.api_key_var = tk.StringVar(value=api_key)
        self.symbol_var = tk.StringVar(value=str(symbol).upper())
        self.api_base_var = tk.StringVar(value=api_base)
        self.max_contracts_var = tk.StringVar(value=str(max_contracts_default) if max_contracts_default else "")
        self.trail_ticks_var = tk.StringVar(value=str(trail_ticks_default) if trail_ticks_default else "")

        self.trading_status_var = tk.StringVar(value="Not running")
        self.balance_var = tk.StringVar(value="—")
        self.equity_var = tk.StringVar(value="—")
        self.max_loss_var = tk.StringVar(value="—")
        self.last_price_var = tk.StringVar(value="—")
        self.updated_var = tk.StringVar(value="—")
        self.open_orders_var = tk.StringVar(value="—")
        self.account_status_var = tk.StringVar(value="—")
        self.username_display_var = tk.StringVar(value=username or "—")
        self.account_display_var = tk.StringVar(value=str(account_id) if account_id else "—")
        self.symbol_display_var = tk.StringVar(value=str(symbol).upper() if symbol else "—")
        self.api_key_display_var = tk.StringVar(value="—")
        self.max_contracts_display_var = tk.StringVar(value=str(max_contracts_default) if max_contracts_default else "—")
        self.trail_ticks_display_var = tk.StringVar(value=str(trail_ticks_default) if trail_ticks_default else "—")
        self.info_var = tk.StringVar(value="Idle")

        self.bot_process: Optional[subprocess.Popen[str]] = None
        self.log_lines: list[str] = []
        self.poll_job: Optional[str] = None
        self.tail_lock = threading.Lock()
        self.current_state: Optional[Dict[str, Any]] = None
        self.credentials_window: Optional[tk.Toplevel] = None

        self.python_interpreter = detect_python_interpreter()
        # Launch the Pine MNQ strategy runner directly
        self.bot_script = str(BACKEND_DIR / "strategies" / "pine_mnq_strategy.py")

        self._build_layout()

    # UI -----------------------------------------------------------------
    def _build_layout(self) -> None:
        self._apply_theme()

        # Dark theme palette inspired by provided CSS
        root_bg = "#1c1e23"      # body background
        card_bg = "#121212"      # panel/card background
        tile_bg = "#151616"      # tile background
        border_color = "#2a2c31"  # subtle borders

        outer = tk.Frame(self.root, bg=root_bg)
        outer.pack(expand=True, fill=tk.BOTH, padx=0, pady=0)

        # Scrollable content container (so the UI can scroll vertically)
        scroll_frame = tk.Frame(outer, bg=root_bg)
        scroll_frame.pack(expand=True, fill=tk.BOTH)

        scroll_canvas = tk.Canvas(scroll_frame, bg=root_bg, highlightthickness=0, bd=0)
        vscroll = ttk.Scrollbar(scroll_frame, orient=tk.VERTICAL, command=scroll_canvas.yview, style="Dark.Vertical.TScrollbar")
        vscroll.pack(side=tk.RIGHT, fill=tk.Y)
        scroll_canvas.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)
        scroll_canvas.configure(yscrollcommand=vscroll.set)

        content = tk.Frame(scroll_canvas, bg=root_bg)
        content_id = scroll_canvas.create_window((0, 0), window=content, anchor="nw")

        def _on_content_configure(event):
            try:
                scroll_canvas.configure(scrollregion=scroll_canvas.bbox("all"))
            except Exception:
                pass

        def _on_canvas_configure(event):
            try:
                scroll_canvas.itemconfigure(content_id, width=event.width)
            except Exception:
                pass

        content.bind("<Configure>", _on_content_configure)
        scroll_canvas.bind("<Configure>", _on_canvas_configure)

        # Mouse wheel support (Windows/Mac) and Button-4/5 (Linux)
        def _wheel_event(event):
            # Avoid hijacking scroll when over the log text widget
            try:
                if isinstance(event.widget, tk.Text) or event.widget is self.log_widget:
                    return
            except Exception:
                pass
            delta = 0
            if hasattr(event, 'delta') and event.delta:
                delta = -1 if event.delta > 0 else 1
            elif getattr(event, 'num', None) == 4:
                delta = -1
            elif getattr(event, 'num', None) == 5:
                delta = 1
            if delta:
                scroll_canvas.yview_scroll(delta, "units")

        # Bind wheel events
        scroll_canvas.bind_all("<MouseWheel>", _wheel_event)
        scroll_canvas.bind_all("<Button-4>", _wheel_event)
        scroll_canvas.bind_all("<Button-5>", _wheel_event)

        card = tk.Frame(
            content,
            bg=card_bg,
            highlightbackground=border_color,
            highlightthickness=1,
            bd=0,
            relief=tk.FLAT,
        )
        card.pack(fill=tk.X, expand=False, padx=24, pady=24)

        header = tk.Frame(card, bg=card_bg)
        header.pack(fill=tk.X, padx=24, pady=(24, 16))
        tk.Label(
            header,
            text=f"{self.app_name}",
            font=(self._font_head, 20, "bold"),
            fg="#e6e8ef",
            bg=card_bg,
        ).pack(anchor=tk.W)
        tk.Label(
            header,
            text="Start/stop the strategy and view logs.",
            font=(self._font_base, 11),
            fg="#a9adb7",
            bg=card_bg,
        ).pack(anchor=tk.W, pady=(6, 0))

        self.status_label = tk.Label(
            card,
            text="Awaiting start…",
            font=(self._font_base, 11, "bold"),
            bg="#1f2630",
            fg="#cfe1ff",
            padx=18,
            pady=10,
        )
        self.status_label.pack(fill=tk.X, padx=24, pady=(0, 20))

        grid_frame = tk.Frame(card, bg=card_bg)
        grid_frame.pack(fill=tk.X, padx=24)

        self.metric_vars: Dict[str, tk.StringVar] = {
            "Username": self.username_display_var,
            "Account ID": self.account_display_var,
            "Symbol": self.symbol_display_var,
            "API Key": self.api_key_display_var,
            "Trading": self.trading_status_var,
            "Open Orders": self.open_orders_var,
            "Balance": self.balance_var,
            "Equity": self.equity_var,
            "Max Loss": self.max_loss_var,
            "Last Price": self.last_price_var,
            "Account Status": self.account_status_var,
            "Updated": self.updated_var,
            "Max Contracts": self.max_contracts_display_var,
            "Trail Distance (ticks)": self.trail_ticks_display_var,
        }
        self.metric_tiles: Dict[str, tk.Label] = {}
        columns = 3
        for idx, (label_text, var) in enumerate(self.metric_vars.items()):
            tile = tk.Frame(
                grid_frame,
                bg=tile_bg,
                highlightbackground=border_color,
                highlightthickness=1,
                bd=0,
                relief=tk.FLAT,
            )
            r = idx // columns
            c = idx % columns
            tile.grid(row=r, column=c, sticky="nsew", padx=8, pady=8)

            lbl = tk.Label(
                tile,
                text=label_text.upper(),
                font=(self._font_base, 8, "bold"),
                fg="#a9adb7",
                bg=tile_bg,
            )
            lbl.pack(anchor=tk.W, padx=12, pady=(10, 4))

            value_lbl = tk.Label(
                tile,
                textvariable=var,
                font=(self._font_base, 13, "bold"),
                fg="#e6e8ef",
                bg=tile_bg,
            )
            value_lbl.pack(anchor=tk.W, padx=12, pady=(0, 10))
            self.metric_tiles[label_text] = value_lbl

        for i in range(columns):
            grid_frame.columnconfigure(i, weight=1)

        actions = tk.Frame(card, bg=card_bg)
        actions.pack(fill=tk.X, padx=24, pady=(8, 12))

        self.start_btn = ttk.Button(actions, text=f"Start {self.app_name}", command=self.start_bot, style="Primary.TButton")
        self.stop_btn = ttk.Button(actions, text="Stop Bot", command=self.stop_bot, style="Danger.TButton")
        self.stop_btn.state(["disabled"])
        self.toggle_btn = ttk.Button(actions, text="Enable Trading", command=self.toggle_trading, style="Primary.TButton")
        self.toggle_btn.state(["disabled"])
        self.refresh_btn = ttk.Button(actions, text="Refresh", command=self.manual_refresh, style="Ghost.TButton")
        self.dashboard_btn = ttk.Button(actions, text="Open Dashboard", command=self.open_dashboard, style="Ghost.TButton")
        self.credentials_btn = ttk.Button(actions, text="Settings", command=self.show_credentials_modal, style="Accent.TButton")

        for btn in (self.start_btn, self.stop_btn, self.toggle_btn, self.refresh_btn, self.dashboard_btn, self.credentials_btn):
            btn.pack(side=tk.LEFT, padx=6)

        info_frame = tk.Frame(card, bg=card_bg)
        info_frame.pack(fill=tk.X, padx=24, pady=(0, 24))
        self.info_label = tk.Label(info_frame, textvariable=self.info_var, fg="#a9adb7", bg=card_bg, font=(self._font_base, 10))
        self.info_label.pack(anchor=tk.W)

        log_frame = tk.Frame(content, bg=root_bg)
        log_frame.pack(expand=True, fill=tk.BOTH, padx=24, pady=(0, 24))
        tk.Label(
            log_frame,
            text="Log",
            fg="#e6e8ef",
            bg=root_bg,
            font=(self._font_head, 12, "bold"),
        ).pack(anchor=tk.W, pady=(0, 8))

        text_frame = tk.Frame(log_frame, bg=root_bg)
        text_frame.pack(expand=True, fill=tk.BOTH)

        self.log_widget = tk.Text(
            text_frame,
            height=12,
            wrap=tk.WORD,
            state=tk.DISABLED,
            bg="#0f1114",
            fg="#e6e8ef",
            insertbackground="#3498ff",
            highlightbackground=border_color,
            highlightthickness=1,
            relief=tk.FLAT,
            bd=0,
        )
        self.log_widget.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)

        scrollbar = ttk.Scrollbar(text_frame, command=self.log_widget.yview, style="Dark.Vertical.TScrollbar")
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_widget.configure(yscrollcommand=scrollbar.set)

        if not self.username_var.get() or not self.api_key_var.get() or not self.account_var.get():
            self.root.after(200, self.show_credentials_modal)

        # Server-dependent actions are disabled for Pine strategy (no local server)
        if not self.server_enabled:
            self.refresh_btn.state(["disabled"])
            self.toggle_btn.state(["disabled"])
            self.dashboard_btn.state(["disabled"])
        else:
            self.refresh_btn.state(["disabled"])
        self._set_status("Awaiting start…", "loading")
        self._update_toggle_button()

    def _apply_theme(self) -> None:
        # Global background and fonts
        self.root.configure(bg="#1c1e23")
        # Dynamic font selection to emulate "Roboto Condensed"
        try:
            import tkinter.font as tkfont
            families = set(tkfont.families())
        except Exception:
            families = set()
        roboto = "Roboto Condensed"
        fallback = "Segoe UI" if "Segoe UI" in families else ("Arial" if "Arial" in families else "TkDefaultFont")
        use_base = roboto if roboto in families else fallback
        self._font_base = use_base
        self._font_head = use_base
        self.style = ttk.Style()
        try:
            self.style.theme_use("clam")
        except tk.TclError:
            pass

        padding = (18, 8)

        # Button styles (CSS-inspired)
        self.style.configure(
            "Primary.TButton",
            background="#3498ff",
            foreground="#ffffff",
            padding=padding,
            borderwidth=0,
            focusthickness=0,
        )
        self.style.map(
            "Primary.TButton",
            background=[("active", "#2589f5"), ("disabled", "#2a2c31")],
            foreground=[("disabled", "#8e8e93")],
        )

        self.style.configure(
            "Danger.TButton",
            background="#e74c3c",
            foreground="#ffffff",
            padding=padding,
            borderwidth=0,
            focusthickness=0,
        )
        self.style.map(
            "Danger.TButton",
            background=[("active", "#c0392b"), ("disabled", "#2a2c31")],
            foreground=[("disabled", "#8e8e93")],
        )

        self.style.configure(
            "Ghost.TButton",
            background="#151616",
            foreground="#e6e8ef",
            padding=padding,
            borderwidth=1,
            focusthickness=0,
            relief="flat",
        )
        self.style.map(
            "Ghost.TButton",
            background=[("active", "#1b1d21"), ("disabled", "#1a1b1f")],
            foreground=[("disabled", "#8e8e93")],
        )

        self.style.configure(
            "Accent.TButton",
            background="#8b5cf6",
            foreground="#ffffff",
            padding=padding,
            borderwidth=0,
            focusthickness=0,
        )
        self.style.map(
            "Accent.TButton",
            background=[("active", "#7c3aed"), ("disabled", "#2a2c31")],
            foreground=[("disabled", "#8e8e93")],
        )

        # Scrollbar styling similar to web CSS
        try:
            self.style.configure(
                "Dark.Vertical.TScrollbar",
                gripcount=0,
                background="#4b4b4b",
                darkcolor="#4b4b4b",
                lightcolor="#4b4b4b",
                troughcolor="#151616",
                bordercolor="#151616",
                arrowcolor="#cccccc",
            )
        except Exception:
            pass

    def _set_status(self, message: str, variant: str) -> None:
        if not hasattr(self, "status_label"):
            return
        palette = {
            "loading": ("#1f2630", "#cfe1ff", "#334155"),
            "ok": ("#134e4a", "#bbf7d0", "#0f766e"),
            "warn": ("#4d3a0e", "#ffdd57", "#b38600"),
            "error": ("#3d0f0f", "#ffc9c9", "#a31616"),
        }
        bg, fg, border = palette.get(variant, palette["loading"])
        self.status_label.configure(text=message, bg=bg, fg=fg)
        self.status_label.configure(highlightbackground=border, highlightthickness=1)

    def _mask_secret(self, secret: str, visible: int = 4) -> str:
        secret = secret.strip()
        if not secret:
            return "—"
        if len(secret) <= 2:
            return "*" * len(secret)
        if len(secret) <= visible * 2:
            return secret[0] + "*" * (len(secret) - 2) + secret[-1]
        return f"{secret[:visible]}{'*' * (len(secret) - (visible * 2))}{secret[-visible:]}"

    def _update_toggle_button(self) -> None:
        if not hasattr(self, "toggle_btn"):
            return
        if not self.bot_process or (self.bot_process and self.bot_process.poll() is not None):
            self.toggle_btn.configure(text="Enable Trading", style="Primary.TButton")
            self.toggle_btn.state(["disabled"])
            return
        if not self.current_state:
            self.toggle_btn.configure(text="Enable Trading", style="Primary.TButton")
            self.toggle_btn.state(["disabled"])
            return
        trading_disabled = bool(self.current_state.get("tradingDisabled"))
        if trading_disabled:
            self.toggle_btn.configure(text="Enable Trading", style="Primary.TButton")
        else:
            self.toggle_btn.configure(text="Disable Trading", style="Danger.TButton")
        self.toggle_btn.state(["!disabled"])

    def manual_refresh(self) -> None:
        if not self.server_enabled:
            self.info_var.set("Refresh not available: no server.")
            return
        if not self.bot_process or self.bot_process.poll() is not None:
            self.info_var.set(f"Start {self.app_name} to refresh state.")
            return
        if self.poll_job:
            self.root.after_cancel(self.poll_job)
            self.poll_job = None
        self.info_var.set("Refreshing…")
        self._poll_status()

    def toggle_trading(self) -> None:
        if not self.server_enabled:
            self.info_var.set("Trading toggle not available: no server.")
            return
        if not self.bot_process or self.bot_process.poll() is not None:
            self.info_var.set(f"Start {self.app_name} first.")
            return
        trading_disabled = True
        if self.current_state is not None:
            trading_disabled = bool(self.current_state.get("tradingDisabled"))
        endpoint = "/trading/enable" if trading_disabled else "/trading/disable"
        payload = None if trading_disabled else {"reason": "manual toggle via desktop"}
        self.toggle_btn.state(["disabled"])
        success = self._post_endpoint(endpoint, payload)
        if success:
            self.info_var.set("Trading toggle request sent.")
            self.manual_refresh()
        else:
            self.info_var.set("Failed to toggle trading.")
        self._update_toggle_button()

    def _post_endpoint(self, path: str, payload: Optional[Dict[str, Any]]) -> bool:
        if payload is None:
            body = json.dumps({}).encode("utf-8")
        else:
            body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{FRONTEND_URL}{path}",
            data=body,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=4) as resp:
                return 200 <= resp.status < 300
        except urllib.error.URLError as exc:
            self.info_var.set(f"Request failed: {exc}")
            return False

    def show_credentials_modal(self) -> None:
        if self.credentials_window and tk.Toplevel.winfo_exists(self.credentials_window):
            self.credentials_window.lift()
            return

        root_bg = "#0f172a"
        card_bg = "#101c36"
        border_color = "#243b61"

        win = tk.Toplevel(self.root)
        win.title(f"{self.app_name} Settings")
        win.configure(bg=root_bg)
        win.resizable(False, False)
        win.transient(self.root)
        win.grab_set()
        self.credentials_window = win

        container = tk.Frame(
            win,
            bg=card_bg,
            highlightbackground=border_color,
            highlightthickness=1,
            bd=0,
        )
        container.pack(padx=20, pady=20, fill=tk.BOTH, expand=True)

        tk.Label(
            container,
            text="Connection Settings",
            font=("Segoe UI", 14, "bold"),
            fg="#f8fafc",
            bg=card_bg,
        ).pack(anchor=tk.W, padx=20, pady=(20, 8))
        tk.Label(
            container,
            text="Update your TopstepX credentials. These values are kept locally.",
            font=("Segoe UI", 10),
            fg="#94a3b8",
            bg=card_bg,
        ).pack(anchor=tk.W, padx=20, pady=(0, 16))

        form = tk.Frame(container, bg=card_bg)
        form.pack(fill=tk.X, padx=20)

        def add_field(row: int, label: str, var: tk.StringVar, show: Optional[str] = None) -> None:
            tk.Label(form, text=label, fg="#e2e8f0", bg=card_bg, font=("Segoe UI", 10)).grid(row=row, column=0, sticky=tk.W, pady=6)
            entry = ttk.Entry(form, textvariable=var, show=show)
            entry.grid(row=row, column=1, sticky=tk.EW, pady=6, padx=(16, 0))

        add_field(0, "Username", self.username_var)
        add_field(1, "API Key", self.api_key_var, show="*")
        add_field(2, "Account ID", self.account_var)
        add_field(3, "Symbol", self.symbol_var)
        add_field(4, "API Base", self.api_base_var)
        add_field(5, "Max Contracts", self.max_contracts_var)
        add_field(6, "Trail Distance (ticks)", self.trail_ticks_var)
        form.columnconfigure(1, weight=1)

        btns = tk.Frame(container, bg=card_bg)
        btns.pack(fill=tk.X, padx=20, pady=(20, 20))

        def close(save: bool) -> None:
            if save:
                self._persist_credentials()
            if self.credentials_window and tk.Toplevel.winfo_exists(self.credentials_window):
                self.credentials_window.destroy()
            self.credentials_window = None

        ttk.Button(btns, text="Save", command=lambda: close(True), style="Primary.TButton").pack(side=tk.LEFT)
        ttk.Button(btns, text="Cancel", command=lambda: close(False), style="Ghost.TButton").pack(side=tk.LEFT, padx=(12, 0))

        win.protocol("WM_DELETE_WINDOW", lambda: close(False))

    def _persist_credentials(self) -> None:
        username = self.username_var.get().strip()
        account = self.account_var.get().strip()
        symbol = self.symbol_var.get().strip().upper()
        api_base = self.api_base_var.get().strip() or "https://api.topstepx.com"
        api_key = self.api_key_var.get().strip()
        max_contracts = self.max_contracts_var.get().strip()
        trail_ticks = self.trail_ticks_var.get().strip()

        if username:
            self.username_display_var.set(username)
            self.username_var.set(username)
        if account:
            self.account_display_var.set(account)
            self.account_var.set(account)
        if symbol:
            self.symbol_display_var.set(symbol)
            self.symbol_var.set(symbol)
        if api_key:
            self.api_key_display_var.set(self._mask_secret(api_key))
        self.api_base_var.set(api_base)
        if max_contracts:
            self.max_contracts_display_var.set(max_contracts)
            self.max_contracts_var.set(max_contracts)
        if trail_ticks:
            self.trail_ticks_display_var.set(trail_ticks)
            self.trail_ticks_var.set(trail_ticks)

        prefs = {
            "username": username,
            "account_id": account,
            "symbol": symbol,
            "api_base": api_base,
            "max_contracts": int(max_contracts) if max_contracts.isdigit() else max_contracts,
            "trail_distance_ticks": int(trail_ticks) if trail_ticks.isdigit() else trail_ticks,
        }
        save_ui_prefs(prefs)

    def _apply_state(self, data: Dict[str, Any]) -> None:
        self.current_state = data

        trading_disabled = bool(data.get("tradingDisabled"))
        self.trading_status_var.set("Disabled" if trading_disabled else "Active")
        self.balance_var.set(self._fmt_currency(data.get("balance")))
        self.equity_var.set(self._fmt_currency(data.get("equity")))
        self.max_loss_var.set(self._fmt_currency(data.get("maximumLoss")))
        self.last_price_var.set(self._fmt_number(data.get("lastPrice")))
        open_orders = data.get("openOrders")
        self.open_orders_var.set(str(open_orders) if open_orders is not None else "—")

        timestamp = data.get("timestamp")
        if timestamp:
            self.updated_var.set(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp)))
        else:
            self.updated_var.set("—")

        username = data.get("username") or self.username_var.get()
        account = data.get("accountId") or self.account_var.get()
        symbol = data.get("symbol") or self.symbol_var.get()
        api_mask = data.get("apiKeyMasked") or self._mask_secret(self.api_key_var.get())

        self.username_display_var.set(username or "—")
        self.account_display_var.set(str(account) if account else "—")
        self.symbol_display_var.set(symbol or "—")
        self.api_key_display_var.set(api_mask or "—")
        self.account_status_var.set(data.get("accountStatus") or "—")

        if not data.get("tokenValid", True):
            self._set_status("Authentication failed. Double-check API credentials.", "error")
        elif trading_disabled:
            reason = data.get("tradingDisabledReason")
            extra = f" — {reason}" if reason else ""
            self._set_status(f"Trading paused{extra}", "warn")
        elif data.get("connected"):
            self._set_status("Bot is connected and trading is enabled.", "ok")
        else:
            self._set_status("Waiting for market stream…", "warn")

        if data.get("error"):
            self.info_var.set(f"API error: {data['error']}")
        else:
            self.info_var.set("Connected")

        self.refresh_btn.state(["!disabled"])
        self.credentials_btn.state(["disabled"])
        self.toggle_btn.state(["disabled"])
        self._update_toggle_button()

        max_contracts_local = self.max_contracts_var.get().strip()
        trail_ticks_local = self.trail_ticks_var.get().strip()
        if max_contracts_local:
            self.max_contracts_display_var.set(max_contracts_local)
        if trail_ticks_local:
            self.trail_ticks_display_var.set(trail_ticks_local)

        self.refresh_btn.state(["disabled"])
        self._set_status("Awaiting start…", "loading")
        self._update_toggle_button()


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

        max_contracts = self.max_contracts_var.get().strip()
        trail_ticks = self.trail_ticks_var.get().strip()

        if max_contracts:
            try:
                max_contracts_int = max(1, int(max_contracts))
            except ValueError:
                messagebox.showerror("Max Contracts", "Enter a whole number for max contracts.")
                return
        else:
            max_contracts_int = None

        if trail_ticks:
            try:
                trail_ticks_int = max(1, int(trail_ticks))
            except ValueError:
                messagebox.showerror("Trail Distance", "Enter a whole number for trailing distance ticks.")
                return
        else:
            trail_ticks_int = None

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

        strategy = config_payload.setdefault("strategy", {})
        if max_contracts_int is not None:
            strategy["contractSizeMax"] = int(max_contracts_int)
        if trail_ticks_int is not None:
            strategy["trailDistanceTicks"] = int(trail_ticks_int)

        if trail_ticks_int is not None:
            strategy.setdefault("trailingStopEnabled", True)

        # Save prefs without API key for convenience
        save_ui_prefs({
            "username": username,
            "account_id": account_int,
            "symbol": symbol,
            "api_base": api_base,
            "max_contracts": max_contracts_int,
            "trail_distance_ticks": trail_ticks_int,
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

        self._append_log(f"{self.app_name} started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.info_var.set("Bot starting…")
        self._set_status(f"Starting {self.app_name}…", "loading")
        self.username_display_var.set(username)
        self.account_display_var.set(str(account_int))
        self.symbol_display_var.set(symbol)
        if api_key:
            self.api_key_display_var.set(self._mask_secret(api_key))
        if max_contracts_int is not None:
            self.max_contracts_display_var.set(str(max_contracts_int))
        if trail_ticks_int is not None:
            self.trail_ticks_display_var.set(str(trail_ticks_int))
        self.start_btn.state(["disabled"])
        self.stop_btn.state(["!disabled"])
        self.refresh_btn.state(["!disabled"])
        self.credentials_btn.state(["disabled"])
        self.toggle_btn.state(["disabled"])  # always disabled for Pine runner
        self._update_toggle_button()

        threading.Thread(target=self._consume_output, daemon=True).start()
        # No server to poll in Pine runner
        if self.server_enabled:
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
            self.current_state = None
            self.info_var.set("Bot stopped")
            self._set_status(f"{self.app_name} stopped.", "warn")
            self.start_btn.state(["!disabled"])
            self.stop_btn.state(["disabled"])
            self.refresh_btn.state(["disabled"])  # server-only
            self.toggle_btn.state(["disabled"])   # server-only
            self.credentials_btn.state(["!disabled"])
            self.trading_status_var.set("Not running")
            self.balance_var.set("—")
            self.equity_var.set("—")
            self.max_loss_var.set("—")
            self.last_price_var.set("—")
            self.updated_var.set("—")
            self.open_orders_var.set("—")
            self.account_status_var.set("—")
            self.api_key_display_var.set(self._mask_secret(self.api_key_var.get()))
            max_contracts = self.max_contracts_var.get().strip()
            trail_ticks = self.trail_ticks_var.get().strip()
            self.max_contracts_display_var.set(max_contracts if max_contracts else "—")
            self.trail_ticks_display_var.set(trail_ticks if trail_ticks else "—")
            self._update_toggle_button()
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
        self._append_log(f"{self.app_name} process finished.")
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
        if not self.server_enabled:
            # Pine runner does not expose /ui; skip polling
            return
        try:
            data = self._fetch_ui_state()
            if data:
                self._apply_state(data)
            else:
                self.info_var.set("No data")
                self._set_status(f"No data from {self.app_name}.", "warn")
                self.current_state = None
                self._update_toggle_button()
        except OSError as exc:
            self.info_var.set(f"No connection: {exc}")
            self._set_status("Unable to reach local server.", "error")
            self.current_state = None
            self._update_toggle_button()
        finally:
            if self.server_enabled:
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
