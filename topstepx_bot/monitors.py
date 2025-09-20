import asyncio
import logging
from typing import Any, Dict, Optional


def make_get_open_orders_count(ctx):
    def get_open_orders_count(contract_id: Any = None) -> int:
        token = ctx['get_token']()
        if not token:
            return 0
        resp = ctx['api_post'](token, "/api/Order/searchOpen", {"accountId": ctx['ACCOUNT_ID']})
        orders = resp.get("orders", [])
        if not isinstance(orders, list):
            return 0
        if contract_id is None:
            return len(orders)
        cnt = 0
        for o in orders:
            try:
                cid = o.get("contractId") or (o.get("contract") or {}).get("id")
                if cid == contract_id:
                    cnt += 1
            except Exception:
                continue
        return cnt
    return get_open_orders_count


def make_monitor_oco_orders(ctx):
    async def monitor_oco_orders():
        while True:
            if not ctx['oco_orders']:
                await asyncio.sleep(0.3)
                continue
            token = ctx['get_token']()
            if not token:
                await asyncio.sleep(0.3)
                continue
            response = ctx['api_post'](token, "/api/Order/searchOpen", {"accountId": ctx['ACCOUNT_ID']})
            orders = response.get("orders", [])
            active_ids = set()
            if isinstance(orders, list):
                for o in orders:
                    oid = o.get("id") or o.get("orderId")
                    if oid is not None:
                        active_ids.add(oid)
            for entry_id, linked_ids in list(ctx['oco_orders'].items()):
                if not entry_id or not all(linked_ids):
                    continue
                tp_id, sl_id = linked_ids
                tp_missing = tp_id not in active_ids
                sl_missing = sl_id not in active_ids
                if tp_missing or sl_missing:
                    remaining_id = sl_id if tp_missing else tp_id
                    if remaining_id in active_ids:
                        success = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], remaining_id)
                        if success:
                            logging.info(f"Canceled remaining OCO leg: {remaining_id}")
                        else:
                            logging.warning(f"Failed to cancel remaining leg: {remaining_id}")
                    else:
                        logging.info(f"Remaining leg already inactive: {remaining_id}")
                    del ctx['oco_orders'][entry_id]
                    if entry_id in ctx['active_entries']:
                        del ctx['active_entries'][entry_id]
            await asyncio.sleep(0.3)
    return monitor_oco_orders


def make_monitor_account_snapshot(ctx):
    async def monitor_account_snapshot():
        while True:
            try:
                token = ctx['get_token']()
                if not token:
                    await asyncio.sleep(2.0)
                    continue
                acct = ctx['get_account_info'](token) or {}
                bal = None
                for k in ("balance", "cash", "cashBalance", "balanceValue"):
                    v = acct.get(k)
                    if v is not None:
                        try:
                            bal = float(v)
                            break
                        except Exception:
                            pass
                eq = None
                for k in ("equity", "netLiq", "netLiquidation", "accountEquity", "balance"):
                    v = acct.get(k)
                    if v is not None:
                        try:
                            eq = float(v)
                            break
                        except Exception:
                            pass
                ctx['account_snapshot']["balance"] = bal
                ctx['account_snapshot']["equity"] = eq
            except Exception:
                pass
            await asyncio.sleep(5.0)
    return monitor_account_snapshot


def make_monitor_break_even(ctx):
    async def monitor_break_even():
        while True:
            try:
                if not ctx['BREAK_EVEN_ENABLED'] or not ctx['active_entries']:
                    await asyncio.sleep(0.3)
                    continue
                token = ctx['get_token']()
                if not token:
                    await asyncio.sleep(0.3)
                    continue
                for entry_id, info in list(ctx['active_entries'].items()):
                    if info.get("be_done"):
                        continue
                    symbol = info.get("symbol")
                    side = int(info.get("side", 0))
                    entry_px = float(info.get("entry"))
                    stop_pts = float(info.get("stop_points") or 0.0)
                    sl_id = info.get("sl_id")
                    if sl_id is None:
                        continue
                    lp = ctx['last_price'].get(symbol)
                    if lp is None:
                        continue
                    if side == 0:
                        trigger = entry_px + stop_pts * float(ctx['BREAK_EVEN_LONG_FACTOR'])
                        triggered = lp >= trigger
                        new_sl = entry_px
                    else:
                        trigger = entry_px - stop_pts * float(ctx['BREAK_EVEN_SHORT_FACTOR'])
                        triggered = lp <= trigger
                        new_sl = entry_px
                    if not triggered:
                        continue
                    tick_size = float(info.get("tickSize") or 0.0)
                    decimals = int(info.get("decimals") or 2)
                    new_sl = ctx['snap_to_tick'](new_sl, tick_size, decimals)
                    ok = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], sl_id)
                    if not ok:
                        logging.warning(f"Break-even: failed to cancel SL {sl_id} for {entry_id}")
                        continue
                    resp = ctx['api_post'](token, "/api/Order/place", {
                        "accountId": ctx['ACCOUNT_ID'],
                        "contractId": info.get("contractId"),
                        "type": 4,
                        "side": 1 - side,
                        "size": int(info.get("size", 1)),
                        "stopPrice": new_sl,
                        "linkedOrderId": entry_id,
                    })
                    if not resp.get("success"):
                        logging.warning(f"Break-even: replace SL failed for {entry_id}: {resp}")
                        continue
                    new_sl_id = resp.get("orderId")
                    info["sl_id"] = new_sl_id
                    info["be_done"] = True
                    ctx['active_entries'][entry_id] = info
                    if entry_id in ctx['oco_orders'] and isinstance(ctx['oco_orders'][entry_id], list) and len(ctx['oco_orders'][entry_id]) == 2:
                        ctx['oco_orders'][entry_id][1] = new_sl_id
                    logging.info(f"Break-even: moved SL to entry for {entry_id} at {new_sl}")
            except Exception:
                pass
            await asyncio.sleep(0.3)
    return monitor_break_even

