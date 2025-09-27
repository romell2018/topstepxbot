"""
Example: Place a native Trailing Stop order (type=5) via TopstepX REST.

WARNING: This script will place a LIVE order if your credentials point to a live account.
         Use a demo account or proceed only if you understand the risk.

It tries multiple trailing field variants (trailTicks, trailingDistance, etc.) to
maximize compatibility, mirroring the bot's internal fallbacks.

Usage:
  python examples/trailing_stop_example.py \
    --symbol MNQ \
    --side long \
    --qty 1 \
    --trail-ticks 100 \
    [--auto-cancel-sec 20]

Reads credentials and account info from config.yaml at project root.
"""

import argparse
import logging
import os
import sys
import time
from typing import Any, Dict, Optional

import yaml

# allow running from examples/ by ensuring Backend package is importable
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..'))
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

from topstepx_bot.api import get_token as _api_get_token, api_post as _api_post, cancel_order as _cancel_order
from topstepx_bot.market import load_contracts as _load_contracts


logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("trail_example")


def load_config() -> Dict[str, Any]:
    cfg_path = os.path.join(os.path.dirname(__file__), '..', 'config.yaml')
    cfg_path = os.path.abspath(cfg_path)
    with open(cfg_path, 'r') as f:
        return yaml.safe_load(f) or {}


def resolve_api_auth(cfg: Dict[str, Any]) -> Dict[str, Any]:
    # Mirrors the main app's resolution approach
    api_url = cfg.get("api_base") or cfg.get("API_URL") or "https://api.topstepx.com"
    auth = (cfg.get("auth") or cfg.get("topstepx") or {})
    username = cfg.get("username") or auth.get("username") or auth.get("email") or ""
    api_key = cfg.get("api_key") or auth.get("api_key") or ""
    account_id = cfg.get("account_id") or auth.get("account_id") or (cfg.get("account") or {}).get("id")
    symbol = (cfg.get("symbol") or "MNQ").upper()
    return {
        "api_url": api_url,
        "username": username,
        "api_key": api_key,
        "account_id": int(account_id) if account_id else None,
        "symbol": symbol,
        "config": cfg,
    }


def get_token_func(api_url: str, username: str, api_key: str):
    return lambda: _api_get_token(api_url, username, api_key)


def resolve_contract_id(auth_res: Dict[str, Any]) -> Dict[str, Any]:
    cfg = auth_res["config"]
    symbol = auth_res["symbol"]
    token_fn = get_token_func(auth_res["api_url"], auth_res["username"], auth_res["api_key"])
    contract_map: Dict[str, Dict[str, Any]] = {}
    _load_contracts(token_fn, contract_map, cfg, symbol)
    rec = contract_map.get(symbol)
    if not rec:
        # Fallback to config CONTRACT_ID
        cid = cfg.get("CONTRACT_ID") or (cfg.get("auth") or {}).get("CONTRACT_ID") or (cfg.get("topstepx") or {}).get("CONTRACT_ID")
        if not cid:
            raise RuntimeError(f"Could not resolve contract for symbol {symbol}; set CONTRACT_ID in config.yaml.")
        rec = {"contractId": cid, "tickSize": float((cfg.get("instrument") or {}).get("tickSize") or 0.0), "decimalPlaces": int((cfg.get("instrument") or {}).get("decimalPlaces") or 2)}
    return {"contract": rec, "contract_map": contract_map}


def try_place_trailing(api_url: str, token: str, account_id: int, contract_id: Any,
                       stop_side: int, qty: int, trail_ticks: int,
                       entry_price: Optional[float], tick_size: Optional[float], decimals: Optional[int]) -> Dict[str, Any]:
    """Attempt several payload variants for native trailing stop (type=5)."""
    base = {
        "accountId": account_id,
        "contractId": contract_id,
        "type": 5,
        "side": int(stop_side),  # 0=Buy, 1=Sell
        "size": int(qty),
    }
    # Compute an absolute starting stop price if we have an entry and tick size
    stop_init = None
    if entry_price is not None and tick_size and tick_size > 0:
        try:
            offset_points = abs(int(trail_ticks)) * float(tick_size)
            stop_init = float(entry_price - offset_points) if stop_side == 1 else float(entry_price + offset_points)
            if decimals is not None:
                stop_init = round(stop_init, int(decimals))
        except Exception:
            stop_init = None

    # Prepare attempts (ordered by known-good variants)
    attempts = []
    # trailTicks: signed ticks often accepted; but since we pass stop side directly,
    # use positive distance and let venue infer direction, or keep signed as in bot logic.
    try:
        t_signed = int(trail_ticks)
        # If protecting a LONG, the stop is a SELL; many venues expect positive distance
        # but some accept signed. Try signed first to match bot behavior.
        if stop_side == 1:  # SELL (protect long)
            t_signed = +int(trail_ticks)
        else:  # BUY (protect short)
            t_signed = +int(trail_ticks)
    except Exception:
        t_signed = int(trail_ticks)
    attempts.append(("trailTicks", dict(base, trailTicks=int(t_signed))))
    if stop_init is not None:
        attempts.append(("distancePrice", dict(base, distancePrice=float(stop_init))))
        attempts.append(("trailPrice", dict(base, trailPrice=float(stop_init))))
    attempts.append(("trailDistance", dict(base, trailDistance=int(max(1, int(trail_ticks))))))
    attempts.append(("distance", dict(base, distance=int(max(1, int(trail_ticks))))))
    attempts.append(("distance+trailDistance", dict(base, distance=int(max(1, int(trail_ticks))), trailDistance=int(max(1, int(trail_ticks))))))
    attempts.append(("distanceTicks", dict(base, distanceTicks=int(max(1, int(trail_ticks))))))
    attempts.append(("trailingDistance", dict(base, trailingDistance=int(max(1, int(trail_ticks))))))

    for name, payload in attempts:
        log.info("Trying trailing variant=%s payloadKeys=%s", name, list(payload.keys()))
        r = _api_post(api_url, token, "/api/Order/place", payload)
        ok = bool(r.get("success")) and bool(r.get("orderId"))
        log.info("Resp: success=%s orderId=%s errorCode=%s errorMessage=%s", r.get("success"), r.get("orderId"), r.get("errorCode"), r.get("errorMessage"))
        if ok:
            return r
    return r  # return last response if none succeeded


def main():
    parser = argparse.ArgumentParser(description="Place a native trailing stop order with fallbacks.")
    parser.add_argument("--symbol", default=None, help="Symbol, e.g., MNQ (defaults from config.yaml)")
    parser.add_argument("--side", choices=["long", "short"], default="long", help="Position to protect: long -> sell trail, short -> buy trail")
    parser.add_argument("--qty", type=int, default=1, help="Quantity (contracts)")
    parser.add_argument("--trail-ticks", type=int, default=100, help="Trailing distance in ticks")
    parser.add_argument("--entry", type=float, default=None, help="Entry price (optional, improves initial stop placement)")
    parser.add_argument("--auto-cancel-sec", type=int, default=0, help="If >0, auto-cancel after N seconds")
    args = parser.parse_args()

    cfg = load_config()
    auth = resolve_api_auth(cfg)
    if args.symbol:
        auth["symbol"] = args.symbol.upper()

    api_url = auth["api_url"]
    username = auth["username"]
    api_key = auth["api_key"]
    account_id = auth["account_id"]
    symbol = auth["symbol"]
    if not account_id:
        raise RuntimeError("account_id is missing in config.yaml")
    if not username or not api_key:
        raise RuntimeError("username/api_key missing in config.yaml -> auth or topstepx section")

    log.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    log.warning("!!! WARNING: THIS WILL PLACE A LIVE TRAILING STOP ORDER IF LIVE ACCOUNT !!!")
    log.warning("!!! Verify your config.yaml. Symbol=%s Qty=%d Trail=%d ticks           !!!", symbol, args.qty, args.trail_ticks)
    log.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    confirm = input("Type 'YES_PLACE_TRAILING_STOP' to continue: ")
    if confirm.strip() != "YES_PLACE_TRAILING_STOP":
        log.info("Cancelled by user.")
        return

    token = _api_get_token(api_url, username, api_key)
    if not token:
        raise RuntimeError("Authentication failed (token not obtained)")

    res = resolve_contract_id(auth)
    contract = res["contract"]
    contract_id = contract.get("contractId")
    tick_size = float(contract.get("tickSize") or 0.0)
    decimals = int(contract.get("decimalPlaces") or 2)
    log.info("Using contractId=%s tickSize=%s decimals=%s", contract_id, tick_size, decimals)

    # Determine stop side: protect long -> SELL (1), protect short -> BUY (0)
    stop_side = 1 if args.side == "long" else 0

    # Place trailing stop
    resp = try_place_trailing(
        api_url=api_url,
        token=token,
        account_id=int(account_id),
        contract_id=contract_id,
        stop_side=stop_side,
        qty=int(args.qty),
        trail_ticks=int(args.trail_ticks),
        entry_price=float(args.entry) if args.entry is not None else None,
        tick_size=float(tick_size) if tick_size else None,
        decimals=int(decimals) if decimals is not None else None,
    )
    if not resp or not resp.get("success") or not resp.get("orderId"):
        log.error("Trailing stop placement failed: %s", resp)
        return
    order_id = resp.get("orderId")
    log.info("Trailing stop placed successfully. Order ID: %s", order_id)

    if args.auto_cancel_sec and args.auto_cancel_sec > 0:
        log.info("Sleeping %d sec before auto-cancel for cleanup...", int(args.auto_cancel_sec))
        time.sleep(int(args.auto_cancel_sec))
        ok = _cancel_order(api_url, token, int(account_id), int(order_id))
        log.info("Auto-cancel %s", "OK" if ok else "FAILED")


if __name__ == "__main__":
    main()
