import argparse
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure Backend/ is on sys.path so 'topstepx_bot' is importable
BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from topstepx_bot.api import get_token as _api_get_token
from topstepx_bot.market import load_contracts as market_load_contracts
from topstepx_bot.streamer import MarketStreamer

try:
    import yaml  # type: ignore
except Exception:
    yaml = None


def load_config(cfg_path: Optional[Path] = None) -> Dict[str, Any]:
    base = cfg_path or (Path(__file__).resolve().parents[1] / "config.yaml")
    if yaml is None:
        raise RuntimeError("PyYAML is required. pip install pyyaml")
    with base.open() as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError("config.yaml is not a mapping")
    return data


class TickRecorder(MarketStreamer):
    def __init__(self, ctx: Dict[str, Any], symbol: str, contract_id: Any, out_path: Path):
        super().__init__(ctx, symbol, contract_id, unit=2, unit_n=1)
        self.out_path = out_path
        self._fp = out_path.open("a", buffering=1)
        self._lock = threading.Lock()

    def _write(self, obj: Dict[str, Any]):
        try:
            line = json.dumps(obj, separators=(",", ":"))
            with self._lock:
                self._fp.write(line + "\n")
        except Exception:
            pass

    def _on_quote(self, args):
        try:
            if not isinstance(args, list) or len(args) < 2:
                return
            _cid, data = str(args[0]), args[1]
            bid = data.get("bestBid")
            ask = data.get("bestAsk")
            last = data.get("lastPrice")
            ts = data.get("timestamp") or data.get("lastUpdated")
            vol = data.get("lastSize") or 0
            if not ts:
                return
            obj = {"type": "quote", "ts": ts, "volume": float(vol)}
            if bid is not None:
                obj["bid"] = float(bid)
            if ask is not None:
                obj["ask"] = float(ask)
            if last is not None:
                obj["last"] = float(last)
            self._write(obj)
        except Exception:
            pass
        # Also feed parent to maintain minute bars if desired
        super()._on_quote(args)

    def _on_trade(self, args):
        try:
            if not isinstance(args, list) or len(args) < 2:
                return
            _cid, data = str(args[0]), args[1]
            price = data.get("price") or data.get("lastPrice")
            vol = data.get("volume") or data.get("lastSize") or 0
            ts = data.get("timestamp")
            if not price or not ts:
                return
            self._write({"type": "trade", "ts": ts, "last": float(price), "volume": float(vol)})
        except Exception:
            pass
        super()._on_trade(args)

    def close(self):
        try:
            with self._lock:
                self._fp.flush()
                self._fp.close()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser(description="Record ticks (quotes + trades) to JSONL for backtesting")
    ap.add_argument("--symbol", default="MNQ")
    ap.add_argument("--out", required=True, help="Output JSONL file path")
    args = ap.parse_args()

    cfg = load_config()
    API_URL = cfg.get("api_base") or "https://api.topstepx.com"
    AUTH = cfg.get("auth", {}) or {}
    USERNAME = cfg.get("username") or AUTH.get("username") or AUTH.get("email") or ""
    API_KEY = cfg.get("api_key") or AUTH.get("api_key") or ""
    SYMBOL = (args.symbol or cfg.get("symbol") or "MNQ").upper()
    MARKET_HUB = cfg.get("market_hub") or "https://rtc.topstepx.com/hubs/market"

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # Compose minimal context for MarketStreamer
    contract_map: Dict[str, Dict[str, Any]] = {}
    def get_token():
        return _api_get_token(API_URL, USERNAME, API_KEY)
    ctx: Dict[str, Any] = {
        'MARKET_HUB': MARKET_HUB,
        'ATR_LENGTH': 14,
        'get_token': get_token,
        'api_post': lambda *a, **k: {},
        'get_open_orders_count': lambda *a, **k: 0,
        'EMA_SHORT': 9,
        'EMA_LONG': 21,
        'DEBUG': False,
        'contract_map': contract_map,
        'bars_by_symbol': {},
        'bars_lock': threading.Lock(),
        'indicator_state': {},
        'last_price': {},
        'ACCOUNT_ID': 0,
        'snap_to_tick': lambda p, ts, d: p,
        'fmt_num': lambda x: str(x),
    }

    # Load contracts to resolve contractId
    market_load_contracts(get_token, contract_map, cfg, SYMBOL)
    contract = (contract_map.get(SYMBOL)
                or next((v for k, v in contract_map.items() if k.upper().startswith(f"{SYMBOL}.") or k.upper().endswith(f".{SYMBOL}")), None))
    if not contract:
        print("No contract resolved; ensure config.yaml has CONTRACT_ID or valid auth.")
        sys.exit(1)
    cid = contract["contractId"]

    out_path = Path(args.out).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rec = TickRecorder(ctx, SYMBOL, cid, out_path)

    stopping = threading.Event()
    def _stop(*_):
        stopping.set()
        try:
            if rec.conn:
                rec.conn.stop()
        except Exception:
            pass
        rec.close()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    token = get_token()
    if not token:
        print("Auth failed; check config.yaml")
        sys.exit(1)
    rec.start(token)
    print(f"Recording ticks for {SYMBOL} ({cid}) to {out_path}. Press Ctrl+C to stop.")
    while not stopping.is_set():
        time.sleep(0.5)


if __name__ == "__main__":
    main()
