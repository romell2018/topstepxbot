import logging
import datetime as dt
from typing import Any, Dict, List, Optional, Callable
import threading
import pandas as pd

from .utils import iso_utc_z
from .indicators import ATR, compute_indicators


def seed_contract_from_config(config: dict, symbol: str, contract_map: Dict[str, dict]) -> bool:
    try:
        sym = (symbol or config.get("symbol") or "").upper()
        auth = (config.get("auth") or {})
        cid = config.get("CONTRACT_ID") or auth.get("CONTRACT_ID")
        instr = (config.get("instrument") or {})
        tick_size = float(instr.get("tickSize") or 0.0)
        decimals = int(instr.get("decimalPlaces") or 2)
        if not sym or not cid:
            return False
        rec = {
            "contractId": cid,
            "tickValue": float(instr.get("tickValue") or 0.0),
            "tickSize": tick_size,
            "pointValue": float(instr.get("pointValue") or 0.0),
            "exchangeFee": 0.0,
            "regulatoryFee": 0.0,
            "totalFees": 0.0,
            "decimalPlaces": decimals,
            "priceScale": decimals,
        }
        contract_map[sym] = rec
        try:
            parts = str(cid).split(".")
            if len(parts) >= 4:
                base_sym = parts[-2].upper()
                mon = parts[-1].upper()
                contract_map[base_sym] = rec
                contract_map[f"{base_sym}.{mon}"] = rec
        except Exception:
            pass
        logging.info(f"Seeded contract from config for {sym}: {cid}")
        return True
    except Exception:
        return False


def load_contracts(api_get_token: Callable[[], Optional[str]], contract_map: Dict[str, dict], config: dict, symbol: str) -> None:
    token = api_get_token()
    if not token:
        logging.error("Contract preload failed: auth error")
        seed_contract_from_config(config, symbol, contract_map)
        return
    import requests
    try:
        res = requests.get(
            "https://userapi.topstepx.com/UserContract/active/nonprofesional",
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
        contracts = res.json()
        if not isinstance(contracts, list):
            logging.warning("Unexpected contract format; seeding from config if available.")
            seed_contract_from_config(config, symbol, contract_map)
            return
        for c in contracts:
            if c.get("disabled"):
                continue
            product_id = c.get("productId")
            if not product_id or not c.get("contractId"):
                continue
            parts = product_id.split(".")
            cid = str(c.get("contractId"))
            cid_parts = cid.split(".")
            sym_from_cid = cid_parts[-2] if len(cid_parts) >= 2 else None
            mon_from_cid = cid_parts[-1] if len(cid_parts) >= 1 else None
            sym_from_pid_last = parts[-1] if len(parts) >= 1 else None
            base_sym = sym_from_cid or sym_from_pid_last
            mon = mon_from_cid if (mon_from_cid and mon_from_cid != base_sym) else None
            if not base_sym:
                continue
            rec = {
                "contractId": c["contractId"],
                "tickValue": c["tickValue"],
                "tickSize": c["tickSize"],
                "pointValue": c["pointValue"],
                "exchangeFee": c["exchangeFee"],
                "regulatoryFee": c["regulatoryFee"],
                "totalFees": c["totalFees"],
                "decimalPlaces": c["decimalPlaces"],
                "priceScale": c["priceScale"],
            }
            contract_map[base_sym.upper()] = rec
            if base_sym and mon:
                contract_map[f"{base_sym.upper()}.{mon.upper()}"] = rec
            if len(parts) >= 2:
                contract_map[f"{parts[-2].upper()}.{parts[-1].upper()}"] = rec
        if not contract_map:
            seed_contract_from_config(config, symbol, contract_map)
        logging.info(f"Loaded {len(contract_map)} contracts")
        print("\n--- Contract Map (keys -> contractId) ---")
        for k, v in contract_map.items():
            try:
                print(f"{k}: {v['contractId']}")
            except Exception:
                pass
    except Exception as e:
        logging.error(f"UserContract load error: {e}")
        seed_contract_from_config(config, symbol, contract_map)


def warmup_bars(api_post: Callable[[str, str, dict], dict], token: str,
                symbol: str, contract_id: Any, days: int, unit: int, unit_n: int, live: bool,
                bars_by_symbol: Dict[str, List[Dict[str, Any]]], bars_lock: threading.Lock,
                indicator_state: Dict[str, Dict[str, Optional[float]]], atr_length: int,
                ema_short: int, ema_long: int, ema_source: str, rth_only: bool, tzname: str) -> None:
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=max(1, days))
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
    j = api_post("/api/History/retrieveBars", payload)
    items = j.get("bars", j.get("candles", []))
    # Fallback: some tenants return no history for live=True — retry once with live=False to seed EMAs
    if (not items) and live:
        try:
            payload_fallback = dict(payload)
            payload_fallback["live"] = False
            j = api_post("/api/History/retrieveBars", payload_fallback)
            items = j.get("bars", j.get("candles", []))
            logging.info("Warmup fallback: used non-live history to seed indicators")
        except Exception:
            pass
    loaded = 0
    atr = ATR(atr_length)
    with bars_lock:
        lst = bars_by_symbol.setdefault(symbol, [])
        for b in items or []:
            try:
                tval = b.get("t") or b.get("time") or b.get("timestamp")
                if isinstance(tval, (int, float)):
                    ts = float(tval)
                    if ts > 1e12:
                        ts = ts / 1000.0
                    t = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
                else:
                    t = dt.datetime.fromisoformat(str(tval).replace("Z", "+00:00")).astimezone(dt.timezone.utc)
                o = float(b.get("o") or b.get("open"))
                h = float(b.get("h") or b.get("high"))
                l = float(b.get("l") or b.get("low"))
                c = float(b.get("c") or b.get("close"))
                v = float(b.get("v") or b.get("volume") or 0)
                lst.append({"t": t.isoformat().replace("+00:00", "Z"), "o": o, "h": h, "l": l, "c": c, "v": v})
                atr.update(h, l, c)
                loaded += 1
            except Exception:
                continue
        bars_by_symbol[symbol] = lst[-300:]
    try:
        df = pd.DataFrame(bars_by_symbol.get(symbol, []))
        if not df.empty:
            df = df.rename(columns={'c': 'close', 'v': 'volume', 't': 'time', 'o': 'open', 'h': 'high', 'l': 'low'})
            df['time'] = pd.to_datetime(df['time'], utc=True)
            df = df.set_index('time')
            ind = compute_indicators(df, ema_short, ema_long, ema_source, rth_only, tzname)
            last = ind.iloc[-1] if not ind.empty else None
            ef_key = f'ema{ema_short}'
            es_key = f'ema{ema_long}'
            ef = float(last[ef_key]) if last is not None and pd.notna(last.get(ef_key)) else None
            es = float(last[es_key]) if last is not None and pd.notna(last.get(es_key)) else None
            vw = float(last['vwap']) if last is not None and pd.notna(last.get('vwap')) else None
        else:
            ef = es = vw = None
    except Exception:
        ef = es = vw = None
    indicator_state[symbol] = {"emaFast": ef, "emaSlow": es, "vwap": vw, "atr": atr.value}
    logging.info(f"Warmup loaded {loaded} bars for {symbol} | EMA{ema_short}={ef} EMA{ema_long}={es} VWAP={vw}")


def seed_streamer_from_warmup(ms, bars_by_symbol, bars_lock, indicator_state) -> None:
    try:
        sym = ms.symbol
        with bars_lock:
            last_bar = (bars_by_symbol.get(sym) or [])[-1] if bars_by_symbol.get(sym) else None
        state = indicator_state.get(sym) or {}
        av = state.get("atr")
        if av is not None:
            ms.atr.value = float(av)
        if last_bar and last_bar.get("c") is not None:
            try:
                ms.atr.prev_close = float(last_bar.get("c"))
            except Exception:
                pass
        try:
            snap = ms._pd_indicator_snapshot() or {}
            ms._last_rel = snap.get("prev_rel")
        except Exception:
            pass
        logging.info(
            "Seeded indicators for %s | atr=%s",
            sym,
            str(ms.atr.value)
        )
    except Exception as e:
        logging.warning(f"Warmup seeding skipped due to error: {e}")
