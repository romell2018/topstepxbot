import time
import math
import json
import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3
import yaml


# ---------- Minimal, single-file helpers ----------

def iso_utc_z(ts: dt.datetime) -> str:
    ts_utc = ts.astimezone(dt.timezone.utc).replace(microsecond=0, tzinfo=None)
    return ts_utc.isoformat() + "Z"


def snap_to_tick(price: float, tick_size: float, decimals: int) -> float:
    try:
        if tick_size and tick_size > 0:
            ticks = round(price / tick_size)
            return round(ticks * tick_size, int(decimals))
    except Exception:
        pass
    return float(round(price, 4))


class ATR:
    def __init__(self, period: int):
        self.period = int(period)
        self.mult = 1.0 / float(self.period)
        self.value: Optional[float] = None
        self.prev_close: Optional[float] = None

    @staticmethod
    def _tr(h: float, l: float, pc: Optional[float]) -> float:
        if pc is None:
            return float(h - l)
        return max(h - l, abs(h - pc), abs(l - pc))

    def update(self, h: float, l: float, c: float) -> Optional[float]:
        tr = self._tr(h, l, self.prev_close)
        if self.value is None:
            self.value = float(tr)
        else:
            self.value = (tr - self.value) * self.mult + self.value
        self.prev_close = float(c)
        return self.value


class EMA:
    def __init__(self, length: int):
        n = max(1, int(length))
        self.alpha = 2.0 / float(n + 1)
        self.value: Optional[float] = None
        self.seed_count = 0
        self.seed_sum = 0.0
        self.length = n

    def update(self, x: float) -> float:
        if self.value is None:
            # Simple MA seed
            self.seed_sum += x
            self.seed_count += 1
            if self.seed_count >= self.length:
                self.value = self.seed_sum / float(self.length)
                return self.value
            return x
        self.value = self.alpha * x + (1.0 - self.alpha) * self.value
        return self.value


# ---------- API wrappers (no project imports) ----------

def get_token(api_url: str, username: str, api_key: str) -> Optional[str]:
    try:
        res = requests.post(
            f"{api_url}/api/Auth/loginKey",
            json={"userName": username, "apiKey": api_key},
            headers={"Content-Type": "application/json"},
            timeout=10,
            verify=False,
        )
        res.raise_for_status()
        data = res.json()
        return data.get("token") if data.get("success") else None
    except Exception as e:
        logging.error(f"Auth error: {e}")
        return None


def api_post(api_url: str, token: str, endpoint: str, payload: dict) -> dict:
    try:
        res = requests.post(
            f"{api_url}{endpoint}",
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
                "x-app-type": "px-desktop",
                "x-app-version": "1.21.1",
            },
            verify=False,
        )
        res.raise_for_status()
        return res.json()
    except Exception as e:
        logging.error(f"API error on {endpoint}: {e}")
        return {}


def get_account_info(token: str) -> Optional[dict]:
    try:
        res = requests.get(
            "https://userapi.topstepx.com/TradingAccount",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0",
                "x-app-type": "px-desktop",
                "x-app-version": "1.21.1",
            },
            verify=False,
        )
        res.raise_for_status()
        accounts = res.json()
        if isinstance(accounts, list) and accounts:
            return accounts[0]
        return None
    except Exception as e:
        logging.error(f"Account info fetch error: {e}")
        return None


# ---------- Strategy runner ----------

def load_config() -> dict:
    with open("config.yaml") as f:
        return yaml.safe_load(f)


def risk_per_point(instrument: dict, default_rpp: float = 5.0) -> float:
    try:
        v = instrument.get("riskPerPoint")
        if v:
            return float(v)
    except Exception:
        pass
    return float(default_rpp)


def compute_size(risk_dollars: float, stop_points: float, dollars_per_point: float, max_size: int) -> int:
    try:
        base = int(max(0, int(risk_dollars / max(1e-6, stop_points * dollars_per_point))))
    except Exception:
        base = 0
    return max(0, min(int(base), int(max_size)))


def place_market_with_brackets(cfg: dict, token: str, contract_id: Any, side: int, op: float,
                               atr_val: float, last_close: float) -> None:
    strat = cfg['strategy']
    instr = cfg.get('instrument') or {}
    api_url = cfg['api_base']
    account_id = int(cfg.get('account_id') or 0)
    tick_size = float(instr.get('tickSize') or 0.0)
    decimals = int(instr.get('decimalPlaces') or 2)
    use_brackets_payload = bool((cfg.get('trade') or {}).get('useBracketsPayload', True))

    # Targets
    use_fixed = bool((cfg.get('trade') or {}).get('useFixedTargets', False))
    if use_fixed:
        tp_points = float((cfg.get('trade') or {}).get('tpPoints', 0) or 0)
        sl_points = float((cfg.get('trade') or {}).get('slPoints', 0) or 0)
        if side == 0:
            stop_points = sl_points
            tgt_points = tp_points
            sl = op - stop_points
            tp = op + tgt_points
        else:
            stop_points = sl_points
            tgt_points = tp_points
            sl = op + stop_points
            tp = op - tgt_points
    else:
        if side == 0:
            stop_points = float(atr_val) * 2.0
            tgt_points = stop_points * 3.0
            sl = op - stop_points
            tp = op + tgt_points
        else:
            stop_points = float(atr_val) * 1.5
            tgt_points = stop_points * 2.0
            sl = op + stop_points
            tp = op - tgt_points

    # Optional pad by ticks
    pad_ticks = int(strat.get('padTicks', 0) or 0)
    if pad_ticks and tick_size > 0:
        pad_dist = pad_ticks * tick_size
        if side == 0:
            sl -= pad_dist
            tp += pad_dist
        else:
            sl += pad_dist
            tp -= pad_dist

    sl = snap_to_tick(sl, tick_size, decimals)
    tp = snap_to_tick(tp, tick_size, decimals)

    # Sizing
    dollars_per_point = risk_per_point(instr, default_rpp=5.0)
    risk_per_trade = float(strat.get('riskPerTrade', 500) or 500)
    max_size = int(strat.get('contractSizeMax', 10) or 10)
    size = compute_size(risk_per_trade, stop_points, dollars_per_point, max_size)
    if size < 1:
        logging.info(
            "Skip entry: size<1 (atr=%.4f stop_pts=%.4f $/pt=%.4f risk=%.2f)",
            atr_val, stop_points, dollars_per_point, risk_per_trade
        )
        return

    payload = {
        "accountId": account_id,
        "contractId": contract_id,
        "type": 2,
        "side": side,
        "size": int(size),
        "customTag": f"ema_cross_{int(time.time())}",
    }

    if use_brackets_payload and tick_size > 0:
        try:
            sl_ticks = int(max(1, round(abs(op - sl) / tick_size)))
            tp_ticks = int(max(1, round(abs(tp - op) / tick_size)))
            payload["stopLossBracket"] = {"ticks": sl_ticks, "type": 0}
            payload["takeProfitBracket"] = {"ticks": tp_ticks, "type": 0}
        except Exception:
            pass

    logging.info(
        "Placing MARKET side=%s op=%.2f size=%d SL=%.2f TP=%.2f",
        "BUY" if side == 0 else "SELL", op, int(size), sl, tp,
    )
    resp = api_post(api_url, token, "/api/Order/place", payload)
    if not resp.get("success"):
        logging.error(f"Entry failed: {resp}")


def retrieve_bars(cfg: dict, token: str, contract_id: Any, start: dt.datetime, end: dt.datetime,
                  unit: int = 2, unit_n: int = 1, live: bool = True) -> List[dict]:
    api_url = cfg['api_base']
    payload = {
        "contractId": contract_id,
        "live": bool(live),
        "startTime": iso_utc_z(start),
        "endTime": iso_utc_z(end),
        "unit": unit,
        "unitNumber": unit_n,
        "limit": 20000,
        "includePartialBar": False,
    }
    j = api_post(api_url, token, "/api/History/retrieveBars", payload)
    items = j.get("bars", j.get("candles", [])) or []
    # Fallback: if live history empty, retry with live=False
    if (not items) and live:
        try:
            payload_fb = dict(payload)
            payload_fb["live"] = False
            j = api_post(api_url, token, "/api/History/retrieveBars", payload_fb)
            items = j.get("bars", j.get("candles", [])) or []
            if not items:
                logging.info("History fallback (live=False) also returned 0 bars")
            else:
                logging.info("History fallback succeeded with live=False (%d bars)", len(items))
        except Exception:
            pass
    out = []
    for b in items:
        try:
            tval = b.get("t") or b.get("time") or b.get("timestamp")
            if isinstance(tval, (int, float)):
                ts = float(tval)
                if ts > 1e12:
                    ts = ts / 1000.0
                t = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
            else:
                t = dt.datetime.fromisoformat(str(tval).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            out.append({
                "t": t,
                "o": float(b.get("o") or b.get("open")),
                "h": float(b.get("h") or b.get("high")),
                "l": float(b.get("l") or b.get("low")),
                "c": float(b.get("c") or b.get("close")),
                "v": float(b.get("v") or b.get("volume") or 0),
            })
        except Exception:
            continue
    return out


def run_single_file():
    # Setup
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    cfg = load_config()

    api_url = cfg.get("api_base") or cfg.get("API_URL") or "https://api.topstepx.com"
    cfg['api_base'] = api_url
    auth = cfg.get("auth", {}) or {}
    username = cfg.get("username") or auth.get("username") or auth.get("email") or ""
    api_key = cfg.get("api_key") or auth.get("api_key") or ""
    account_id = int(cfg.get("account_id") or (cfg.get("account") or {}).get("id") or 0)
    symbol = (cfg.get("symbol") or "MNQ").upper()
    contract_id = cfg.get("CONTRACT_ID") or auth.get("CONTRACT_ID")
    if not contract_id:
        logging.error("CONTRACT_ID missing in config.yaml; please set it.")
        return

    strat = (cfg.get("strategy") or {})
    ema_short_n = int(strat.get("emaShort", 9))
    ema_long_n = int(strat.get("emaLong", 21))
    atr_len = int(strat.get("atrLength", 14))
    confirm_bars = int(strat.get("confirmBars", 0))
    long_only = bool(strat.get("longOnly", True))
    use_vwap = bool(strat.get("vwapEnabled", False))
    trade_cooldown = int((cfg.get("risk") or {}).get("trade_cooldown_sec", 10))
    live = bool((cfg.get("market") or {}).get("live", True))

    token = get_token(api_url, username, api_key)
    if not token:
        logging.error("Auth failed; check API key/username in config.yaml")
        return

    # Warmup bars (last N hours)
    bootstrap_hrs = int(cfg.get("bootstrap_history_hours", 3) or 3)
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=max(1, bootstrap_hrs))
    logging.info("Loading history: %s -> %s (live=%s)", start.isoformat(), end.isoformat(), str(live))
    bars = retrieve_bars(cfg, token, contract_id, start, end, unit=2, unit_n=1, live=live)
    if not bars:
        # Expand window to 24h and force live=False as a robust fallback
        start2 = end - dt.timedelta(hours=24)
        logging.info("No bars from initial request; retrying 24h window, live=False")
        bars = retrieve_bars(cfg, token, contract_id, start2, end, unit=2, unit_n=1, live=False)
    bars = sorted(bars, key=lambda x: x['t'])[-300:]
    if not bars:
        logging.error("No bars returned; cannot start strategy.")
        return

    # Indicator state
    ema_fast = EMA(ema_short_n)
    ema_slow = EMA(ema_long_n)
    atr = ATR(atr_len)
    cum_vol = 0.0
    cum_pv = 0.0
    prev_rel: Optional[float] = None
    last_signal_ts = 0.0

    for b in bars:
        c = float(b['c']); h = float(b['h']); l = float(b['l']); v = float(b['v'])
        ef = ema_fast.update(c)
        es = ema_slow.update(c)
        atr.update(h, l, c)
        cum_vol += v
        cum_pv += c * v
        vw = (cum_pv / cum_vol) if (cum_vol > 0 and use_vwap) else None
        rel = (ef - es) if (ema_fast.value is not None and ema_slow.value is not None) else None
        if rel is not None:
            prev_rel = rel

    logging.info("Warmup complete | last close=%.2f EMA%d=%.2f EMA%d=%.2f ATR=%.4f",
                 bars[-1]['c'], ema_short_n, ema_fast.value or float('nan'), ema_long_n, ema_slow.value or float('nan'), atr.value or float('nan'))

    # Poll loop for new bars and trade on cross
    last_bar_time: dt.datetime = bars[-1]['t']
    while True:
        try:
            time.sleep(10)
            end = dt.datetime.now(dt.timezone.utc)
            start = last_bar_time - dt.timedelta(minutes=2)
            new_bars = retrieve_bars(cfg, token, contract_id, start, end, unit=2, unit_n=1, live=live)
            new_bars = [b for b in new_bars if b['t'] > last_bar_time]
            if not new_bars:
                continue
            for b in new_bars:
                c = float(b['c']); h = float(b['h']); l = float(b['l']); v = float(b['v'])
                ef = ema_fast.update(c)
                es = ema_slow.update(c)
                av = atr.update(h, l, c)
                cum_vol += v
                cum_pv += c * v
                vw = (cum_pv / cum_vol) if (cum_vol > 0 and use_vwap) else None

                if (ef is not None) and (es is not None) and (av is not None):
                    rel = ef - es
                    cross_up = (prev_rel is not None) and (prev_rel <= 0) and (rel > 0)
                    cross_dn = (prev_rel is not None) and (prev_rel >= 0) and (rel < 0)
                    prev_rel = rel

                    now = time.time()
                    if (now - last_signal_ts) < trade_cooldown:
                        continue

                    vwap_long_ok = True
                    vwap_short_ok = True
                    if use_vwap and vw is not None:
                        vwap_long_ok = c > vw
                        vwap_short_ok = c < vw

                    if cross_up and vwap_long_ok:
                        last_signal_ts = now
                        place_market_with_brackets(cfg, token, contract_id, side=0, op=float(c), atr_val=float(av), last_close=float(c))
                    elif (not long_only) and cross_dn and vwap_short_ok:
                        last_signal_ts = now
                        place_market_with_brackets(cfg, token, contract_id, side=1, op=float(c), atr_val=float(av), last_close=float(c))

                last_bar_time = b['t']
        except KeyboardInterrupt:
            logging.info("Stopping...")
            break
        except Exception as e:
            logging.warning(f"Loop error: {e}")


if __name__ == "__main__":
    run_single_file()
