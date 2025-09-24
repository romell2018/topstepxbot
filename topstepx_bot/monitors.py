import asyncio
import logging
from typing import Any, Dict, Optional
import time


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
            id_to_order = {}
            if isinstance(orders, list):
                for o in orders:
                    oid = o.get("id") or o.get("orderId")
                    if oid is not None:
                        active_ids.add(oid)
                        id_to_order[oid] = o
            # Try to discover open positions by contract to detect manual flat scenarios
            open_pos_contracts = set()
            try:
                pos_resp = ctx['api_post'](token, "/api/Position/searchOpen", {"accountId": ctx['ACCOUNT_ID']})
                # Accept either {positions: [...]} or a plain list
                pos_list = pos_resp.get("positions") if isinstance(pos_resp, dict) else None
                if pos_list is None and isinstance(pos_resp, list):
                    pos_list = pos_resp
                if isinstance(pos_list, list):
                    for p in pos_list:
                        try:
                            # Try common keys
                            qty = p.get("quantity") or p.get("qty") or p.get("size")
                            if qty is None:
                                continue
                            if float(qty) == 0:
                                continue
                            cid = p.get("contractId") or (p.get("contract") or {}).get("id")
                            if cid is not None:
                                open_pos_contracts.add(cid)
                        except Exception:
                            continue
            except Exception:
                pass
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
                    continue
                # Orphan cleanup: if both legs are still open but position is flat for this contract, cancel both
                try:
                    info = ctx['active_entries'].get(entry_id) or {}
                    cid = info.get("contractId")
                    if cid is not None and (cid not in open_pos_contracts) and (tp_id in active_ids) and (sl_id in active_ids):
                        ok1 = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], tp_id)
                        ok2 = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], sl_id)
                        logging.info(
                            "Orphan OCO cleanup for %s (contract %s): cancel TP=%s(%s) SL=%s(%s)",
                            str(entry_id), str(cid), str(tp_id), str(ok1), str(sl_id), str(ok2)
                        )
                        del ctx['oco_orders'][entry_id]
                        if entry_id in ctx['active_entries']:
                            del ctx['active_entries'][entry_id]
                except Exception:
                    pass
            # Global orphan child cleanup: cancel any child (linked) exit orders when their parent isn't active and no position is open
            try:
                for o in list(orders or []):
                    try:
                        oid = o.get("id") or o.get("orderId")
                        if oid is None:
                            continue
                        # only consider bracket children with a linked parent
                        linked = o.get("linkedOrderId") or o.get("parentId") or o.get("parentOrderId")
                        if linked is None:
                            continue
                        # order types: 1=limit, 4=stop, 5=trailing -> likely exit legs
                        otype = int(o.get("type") or 0)
                        if otype not in (1, 4, 5):
                            continue
                        cid = o.get("contractId") or (o.get("contract") or {}).get("id")
                        if cid is None:
                            continue
                        # If parent is not active AND no open position for this contract, cancel the orphaned child
                        if (linked not in active_ids) and (cid not in open_pos_contracts):
                            ok = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], oid)
                            logging.info(
                                "Global orphan cleanup: canceled child order %s (type=%s, contract=%s) parent=%s active_parent=%s",
                                str(oid), str(otype), str(cid), str(linked), str(linked in active_ids)
                            )
                    except Exception:
                        continue
            except Exception:
                pass
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
                # Detect closed/suspended account states and gate trading proactively
                try:
                    def _val_s(obj, key):
                        v = obj.get(key)
                        return str(v).strip().lower() if v is not None else None

                    closed = False
                    reason = None
                    # Boolean flags commonly seen
                    for kb in ('isClosed', 'closed', 'tradingDisabled', 'disabled'):
                        vb = acct.get(kb)
                        if isinstance(vb, bool) and vb:
                            closed = True
                            reason = f"{kb}=true"
                            break
                    # String status fields
                    if not closed:
                        for ks in ('status', 'state', 'accountStatus', 'tradingStatus', 'lifecycleState'):
                            vs = _val_s(acct, ks)
                            if not vs:
                                continue
                            if any(term in vs for term in ('closed', 'suspend', 'disable', 'locked', 'terminated')):
                                closed = True
                                reason = f"{ks}={vs}"
                                break
                    if closed and not ctx.get('TRADING_DISABLED'):
                        ctx['TRADING_DISABLED'] = True
                        ctx['TRADING_DISABLED_REASON'] = reason or 'account closed'
                        logging.error("Trading disabled: %s", ctx['TRADING_DISABLED_REASON'])
                    # Auto re-enable if account reopens and setting allows
                    if (not closed) and ctx.get('TRADING_DISABLED') and ctx.get('AUTO_REENABLE_TRADING_ON_OPEN'):
                        prev_reason = str(ctx.get('TRADING_DISABLED_REASON') or '').lower()
                        # Only auto-enable if it was disabled due to account state, not manual kill
                        if any(term in prev_reason for term in ('closed', 'suspend', 'disable', 'locked', 'terminated')):
                            ctx['TRADING_DISABLED'] = False
                            ctx['TRADING_DISABLED_REASON'] = None
                            logging.info("Trading re-enabled: account state reported open/active")
                    # Optionally annotate last seen account state
                    try:
                        ctx['ACCOUNT_STATE'] = {
                            'closed': bool(closed),
                            'reason': reason,
                        }
                    except Exception:
                        pass
                except Exception:
                    pass
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


def make_monitor_synth_trailing(ctx):
    async def monitor_synth_trailing():
        while True:
            try:
                if not ctx.get('SYNTH_TRAILING_ENABLED') or not ctx['active_entries']:
                    await asyncio.sleep(0.3)
                    continue
                token = ctx['get_token']()
                if not token:
                    await asyncio.sleep(0.5)
                    continue
                for entry_id, info in list(ctx['active_entries'].items()):
                    try:
                        sl_id = info.get("sl_id")
                        if sl_id is None:
                            # nothing to trail (e.g., venue-managed bracket)
                            continue
                        # If we've already converted this leg to a native trailing stop, skip further synthetic moves
                        if bool(info.get("native_trail")):
                            continue
                        symbol = info.get("symbol")
                        side = int(info.get("side", 0))
                        size = int(info.get("size", 1))
                        entry_px = float(info.get("entry"))
                        lp = ctx['last_price'].get(symbol)
                        if lp is None:
                            continue
                        # compute trailing distance from current ATR and multipliers
                        st = ctx['indicator_state'].get(symbol) or {}
                        atr_val = st.get('atr')
                        if atr_val is None:
                            continue
                        k = ctx['ATR_TRAIL_K_LONG'] if side == 0 else ctx['ATR_TRAIL_K_SHORT']
                        trail_points = float(atr_val) * float(k)
                        # snap and compute candidate new SL
                        tick_size = float(info.get("tickSize") or 0.0)
                        decimals = int(info.get("decimals") or 2)
                        min_ticks = int(ctx.get('SYNTH_TRAIL_MIN_TICKS', 1) or 1)
                        if side == 0:
                            target = lp - trail_points
                        else:
                            target = lp + trail_points
                        target = ctx['snap_to_tick'](target, tick_size, decimals) if (tick_size and tick_size > 0) else target
                        # do not widen stop
                        cur_sl_id = sl_id
                        cur_sl_px = None
                        # we don't query current SL price; use strict monotonic rule vs previous stored stop if present
                        prev_stop_pts = float(info.get("stop_points") or 0.0)
                        # derive previous SL from entry and stored points
                        prev_sl_guess = entry_px - prev_stop_pts if side == 0 else entry_px + prev_stop_pts
                        # only update if improved at least min_ticks
                        tick_move = (abs(target - prev_sl_guess) / tick_size) if (tick_size and tick_size > 0) else abs(target - prev_sl_guess)
                        improve = False
                        if side == 0:
                            improve = target > prev_sl_guess and tick_move >= min_ticks
                        else:
                            improve = target < prev_sl_guess and tick_move >= min_ticks
                        if not improve:
                            continue
                        # replace SL
                        ok = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], cur_sl_id)
                        if not ok:
                            logging.warning(f"Synth trail: failed to cancel SL {cur_sl_id} for {entry_id}")
                            continue
                        # Prefer replacing with a native trailing stop (type=5) when trailing is enabled; otherwise fallback to fixed stop
                        use_native_trail = bool(ctx.get('TRAILING_STOP_ENABLED', True))
                        resp = None
                        if use_native_trail and (tick_size and tick_size > 0):
                            # Try multiple native trailing variants, including price-based trailPrice + stopPrice
                            try:
                                t_ticks = int(max(1, int(ctx.get('TRAIL_TICKS_FIXED', 5))))
                            except Exception:
                                t_ticks = 5
                            offset_points = float(t_ticks) * float(tick_size)
                            stop_init = float(entry_px - offset_points) if side == 0 else float(entry_px + offset_points)
                            stop_init = ctx['snap_to_tick'](stop_init, float(tick_size), int(decimals)) if (tick_size and decimals is not None) else stop_init
                            base = {
                                "accountId": ctx['ACCOUNT_ID'],
                                "contractId": info.get("contractId"),
                                "type": 5,
                                "side": 1 - side,
                                "size": size,
                                "linkedOrderId": entry_id,
                            }
                            resp = None
                            preferred = str(ctx.get('TRAIL_VARIANT') or '').strip().lower()
                            attempts = []
                            # 1) trailTicks signed
                            try:
                                t_signed = -int(t_ticks) if side == 0 else int(t_ticks)
                            except Exception:
                                t_signed = int(t_ticks)
                            attempts.append(("trailticks", dict(base, trailTicks=int(t_signed))))
                            # Venue expects absolute start price. Try distancePrice then trailPrice.
                            attempts.append(("distanceprice", dict(base, distancePrice=float(stop_init))))
                            attempts.append(("trailprice", dict(base, trailPrice=float(stop_init))))
                            attempts.append(("traildistance", dict(base, trailDistance=int(t_ticks))))
                            attempts.append(("distance", dict(base, distance=int(t_ticks))))
                            attempts.append(("distance+traildistance", dict(base, distance=int(t_ticks), trailDistance=int(t_ticks))))
                            attempts.append(("distanceticks", dict(base, distanceTicks=int(t_ticks))))
                            attempts.append(("trailingdistance", dict(base, trailingDistance=int(t_ticks))))
                            if preferred:
                                attempts.sort(key=lambda x: (x[0] != preferred,))
                            # diagnostic buffer of trail attempts
                            trail_log = ctx.setdefault('LAST_TRAIL_ATTEMPTS', [])
                            for name, payload in attempts:
                                resp = ctx['api_post'](token, "/api/Order/place", payload)
                                try:
                                    rec = {
                                        'ts': int(time.time()),
                                        'variant': name,
                                        'payloadKeys': sorted(list(payload.keys())),
                                        'trailFields': {k: payload.get(k) for k in ('trailTicks','trailDistance','distance','distanceTicks','trailingDistance','trailPrice','distancePrice','stopPrice') if k in payload},
                                        'resp': {'success': resp.get('success'), 'orderId': resp.get('orderId'), 'errorCode': resp.get('errorCode'), 'errorMessage': resp.get('errorMessage')},
                                        'linkedId': entry_id,
                                    }
                                    trail_log.append(rec)
                                    if len(trail_log) > 50:
                                        del trail_log[:len(trail_log)-50]
                                    ctx['LAST_TRAIL_ATTEMPTS'] = trail_log
                                except Exception:
                                    pass
                                if resp.get("success") and resp.get("orderId"):
                                    ctx['TRAIL_VARIANT'] = name
                                    break
                        if not resp or (not resp.get("success")):
                            # Fallback to fixed STOP at target price
                            payload = {
                                "accountId": ctx['ACCOUNT_ID'],
                                "contractId": info.get("contractId"),
                                "type": 4,
                                "side": 1 - side,
                                "size": size,
                                "stopPrice": target,
                                "linkedOrderId": entry_id,
                            }
                            resp = ctx['api_post'](token, "/api/Order/place", payload)
                            if not resp.get("success"):
                                logging.warning(f"Synth trail: replace SL failed for {entry_id}: {resp}")
                                continue
                            new_sl_id = resp.get("orderId")
                            info["sl_id"] = new_sl_id
                            # update stored stop_points to reflect tighter stop
                            info["stop_points"] = abs(entry_px - target)
                            ctx['active_entries'][entry_id] = info
                            if entry_id in ctx['oco_orders'] and isinstance(ctx['oco_orders'][entry_id], list) and len(ctx['oco_orders'][entry_id]) == 2:
                                ctx['oco_orders'][entry_id][1] = new_sl_id
                            logging.info("Synth trail moved STOP to %.4f (entry %.4f, ATR %.4f, k %.2f)", target, entry_px, atr_val, k)
                        else:
                            # Native trailing placed successfully
                            new_sl_id = resp.get("orderId")
                            info["sl_id"] = new_sl_id
                            info["native_trail"] = True
                            # store distance in points for reference
                            info["stop_points"] = float(trail_points)
                            ctx['active_entries'][entry_id] = info
                            # keep OCO map in sync if present
                            if entry_id in ctx['oco_orders'] and isinstance(ctx['oco_orders'][entry_id], list) and len(ctx['oco_orders'][entry_id]) == 2:
                                ctx['oco_orders'][entry_id][1] = new_sl_id
                            logging.info("Synth trail switched to TRAILING stop (ticks=%s) for entry %s",
                                         (str(int(max(1, round(abs(trail_points) / tick_size)))) if (tick_size and tick_size > 0) else 'N/A'), str(entry_id))
                    except Exception:
                        continue
            except Exception:
                pass
            await asyncio.sleep(float(ctx.get('SYNTH_TRAIL_POLL_SEC', 0.5) or 0.5))
    return monitor_synth_trailing


def make_monitor_profit_trail(ctx):
    async def monitor_profit_trail():
        while True:
            try:
                # Gate feature
                if not bool(ctx.get('PROFIT_TRAIL_ENABLED', False)):
                    await asyncio.sleep(0.5)
                    continue
                if not ctx['active_entries']:
                    await asyncio.sleep(0.3)
                    continue
                token = ctx['get_token']()
                if not token:
                    await asyncio.sleep(0.5)
                    continue
                for entry_id, info in list(ctx['active_entries'].items()):
                    try:
                        # If already moved to native trailing or already activated, skip
                        if bool(info.get('native_trail')) or bool(info.get('profit_trail_done')):
                            continue
                        sl_id = info.get('sl_id')
                        if sl_id is None:
                            continue
                        symbol = info.get('symbol')
                        side = int(info.get('side', 0))  # 0=long, 1=short
                        entry_px = float(info.get('entry'))
                        size = int(info.get('size', 1))
                        tick_size = float(info.get('tickSize') or 0.0)
                        decimals = int(info.get('decimals') or 2)
                        lp = ctx['last_price'].get(symbol)
                        if lp is None:
                            continue
                        # Resolve config: offsets and trail points (points)
                        off_l = float(ctx.get('TRAIL_OFFSET_LONG', ctx.get('TRAIL_OFFSET', 0.0)) or 0.0)
                        off_s = float(ctx.get('TRAIL_OFFSET_SHORT', ctx.get('TRAIL_OFFSET', 0.0)) or 0.0)
                        pts_l = float(ctx.get('TRAIL_POINTS_LONG', ctx.get('TRAIL_POINTS', 0.0)) or 0.0)
                        pts_s = float(ctx.get('TRAIL_POINTS_SHORT', ctx.get('TRAIL_POINTS', 0.0)) or 0.0)
                        # Pick side-specific values with fallback
                        trail_offset = off_l if side == 0 else off_s
                        trail_points = pts_l if side == 0 else pts_s
                        if trail_points <= 0 or (tick_size is None) or tick_size <= 0:
                            continue  # need a valid trailing distance and tick size
                        # Activation condition: price must be beyond entry by offset
                        if side == 0:
                            activated = lp >= (entry_px + trail_offset)
                        else:
                            activated = lp <= (entry_px - trail_offset)
                        if not activated:
                            continue
                        # Compute trail ticks and initial stop suggestion near entry
                        try:
                            t_ticks = int(max(1, round(abs(float(trail_points)) / float(tick_size))))
                        except Exception:
                            t_ticks = 1
                        # initial stop level slightly beyond entry by trail distance
                        try:
                            offset_points = float(t_ticks) * float(tick_size)
                            stop_init = float(entry_px - offset_points) if side == 0 else float(entry_px + offset_points)
                            stop_init = ctx['snap_to_tick'](stop_init, float(tick_size), int(decimals))
                        except Exception:
                            stop_init = None
                        # Cancel current fixed SL before placing trailing; if place fails, fallback to re-placing stop
                        prev_stop_points = float(info.get('stop_points') or 0.0)
                        prev_stop_px = (entry_px - prev_stop_points) if side == 0 else (entry_px + prev_stop_points)
                        ok = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], sl_id)
                        if not ok:
                            logging.warning(f"Profit-trail: failed to cancel SL {sl_id} for {entry_id}")
                            continue
                        # Try native trailing (type=5) with several variants
                        base = {
                            "accountId": ctx['ACCOUNT_ID'],
                            "contractId": info.get("contractId"),
                            "type": 5,
                            "side": 1 - side,
                            "size": size,
                            "linkedOrderId": entry_id,
                        }
                        # Build attempts (try signed trailTicks first, then price-based, then others)
                        attempts = []
                        try:
                            t_signed_pos = int(t_ticks)
                            t_signed_neg = -int(t_ticks)
                        except Exception:
                            t_signed_pos = int(t_ticks)
                            t_signed_neg = -int(t_ticks)
                        attempts.append(("trailticks+", dict(base, trailTicks=int(t_signed_pos))))
                        attempts.append(("trailticks-", dict(base, trailTicks=int(t_signed_neg))))
                        if stop_init is not None:
                            attempts.append(("distanceprice", dict(base, distancePrice=float(stop_init))))
                            attempts.append(("trailprice", dict(base, trailPrice=float(stop_init))))
                        attempts.append(("traildistance", dict(base, trailDistance=int(max(1, int(t_ticks))))))
                        attempts.append(("distance", dict(base, distance=int(max(1, int(t_ticks))))))
                        attempts.append(("distance+traildistance", dict(base, distance=int(max(1, int(t_ticks))), trailDistance=int(max(1, int(t_ticks))))))
                        attempts.append(("distanceticks", dict(base, distanceTicks=int(max(1, int(t_ticks))))))
                        attempts.append(("trailingdistance", dict(base, trailingDistance=int(max(1, int(t_ticks))))))
                        preferred = str(ctx.get('TRAIL_VARIANT') or '').strip().lower()
                        if preferred:
                            attempts.sort(key=lambda x: (x[0] != preferred,))
                        resp = None
                        trail_log = ctx.setdefault('LAST_TRAIL_ATTEMPTS', [])
                        for name, payload in attempts:
                            r = ctx['api_post'](token, "/api/Order/place", payload)
                            try:
                                rec = {
                                    'ts': int(time.time()),
                                    'variant': name,
                                    'payloadKeys': sorted(list(payload.keys())),
                                    'trailFields': {k: payload.get(k) for k in ('trailTicks','trailDistance','distance','distanceTicks','trailingDistance','trailPrice','distancePrice','stopPrice') if k in payload},
                                    'resp': {'success': r.get('success'), 'orderId': r.get('orderId'), 'errorCode': r.get('errorCode'), 'errorMessage': r.get('errorMessage')},
                                    'linkedId': entry_id,
                                }
                                trail_log.append(rec)
                                if len(trail_log) > 50:
                                    del trail_log[:len(trail_log)-50]
                                ctx['LAST_TRAIL_ATTEMPTS'] = trail_log
                            except Exception:
                                pass
                            if r.get("success") and r.get("orderId"):
                                ctx['TRAIL_VARIANT'] = name
                                resp = r
                                break
                        if not resp or (not resp.get("success")):
                            # Fallback to a fixed STOP at prior stop level to avoid being unprotected
                            payload = {
                                "accountId": ctx['ACCOUNT_ID'],
                                "contractId": info.get("contractId"),
                                "type": 4,
                                "side": 1 - side,
                                "size": size,
                                "stopPrice": ctx['snap_to_tick'](prev_stop_px, tick_size, decimals) if (tick_size and tick_size > 0) else prev_stop_px,
                                "linkedOrderId": entry_id,
                            }
                            r2 = ctx['api_post'](token, "/api/Order/place", payload)
                            if not r2.get("success"):
                                logging.warning(f"Profit-trail: replace SL failed for {entry_id}: {r2}")
                                continue
                            new_sl_id = r2.get("orderId")
                            info["sl_id"] = new_sl_id
                            info["stop_points"] = abs(entry_px - prev_stop_px)
                            ctx['active_entries'][entry_id] = info
                            if entry_id in ctx['oco_orders'] and isinstance(ctx['oco_orders'][entry_id], list) and len(ctx['oco_orders'][entry_id]) == 2:
                                ctx['oco_orders'][entry_id][1] = new_sl_id
                            logging.info("Profit-trail: activation failed; restored fixed SL @ %.4f", prev_stop_px)
                        else:
                            # Success: switched to native trailing
                            new_sl_id = resp.get("orderId")
                            info["sl_id"] = new_sl_id
                            info["native_trail"] = True
                            info["profit_trail_done"] = True
                            info["stop_points"] = float(trail_points)
                            ctx['active_entries'][entry_id] = info
                            if entry_id in ctx['oco_orders'] and isinstance(ctx['oco_orders'][entry_id], list) and len(ctx['oco_orders'][entry_id]) == 2:
                                ctx['oco_orders'][entry_id][1] = new_sl_id
                            logging.info("Profit-trail: activated TRAILING stop (ticks=%d, offset=%.4f) for entry %s", int(t_ticks), float(trail_offset), str(entry_id))
                    except Exception:
                        continue
            except Exception:
                pass
            await asyncio.sleep(0.3)
    return monitor_profit_trail


def make_monitor_tv_trailing(ctx):
    async def monitor_tv_trailing():
        while True:
            try:
                if not bool(ctx.get('TV_TRAILING_ENABLED', False)):
                    await asyncio.sleep(0.5)
                    continue
                if not ctx['active_entries']:
                    await asyncio.sleep(0.3)
                    continue
                token = ctx['get_token']()
                if not token:
                    await asyncio.sleep(0.5)
                    continue
                for entry_id, info in list(ctx['active_entries'].items()):
                    try:
                        if not bool(info.get('tv_trail')):
                            continue
                        sl_id = info.get("sl_id")
                        if sl_id is None:
                            continue
                        symbol = info.get("symbol")
                        side = int(info.get("side", 0))
                        entry_px = float(info.get("entry"))
                        tick_size = float(info.get("tickSize") or 0.0)
                        decimals = int(info.get("decimals") or 2)
                        size = int(info.get("size", 1))
                        lp = ctx['last_price'].get(symbol)
                        if lp is None or (not tick_size) or tick_size <= 0:
                            continue
                        # update best high/low since entry
                        if side == 0:
                            best = float(info.get('best_high', entry_px))
                            if lp > best:
                                best = lp
                                info['best_high'] = best
                        else:
                            best = float(info.get('best_low', entry_px))
                            if lp < best:
                                best = lp
                                info['best_low'] = best
                        # Resolve side-specific trail points
                        t_points = float(ctx.get('TV_TRAIL_POINTS_LONG') if side == 0 else ctx.get('TV_TRAIL_POINTS_SHORT') or 0.0)
                        if t_points <= 0:
                            continue
                        # Compute target stop based on TV rule; never loosen
                        if side == 0:
                            target = best - t_points
                        else:
                            target = best + t_points
                        target = ctx['snap_to_tick'](target, tick_size, decimals)
                        prev_stop_pts = float(info.get("stop_points") or 0.0)
                        prev_sl = (entry_px - prev_stop_pts) if side == 0 else (entry_px + prev_stop_pts)
                        tick_move = abs(target - prev_sl) / tick_size
                        improve = (target > prev_sl and tick_move >= 1.0) if side == 0 else (target < prev_sl and tick_move >= 1.0)
                        if not improve:
                            continue
                        ok = ctx['cancel_order'](token, ctx['ACCOUNT_ID'], sl_id)
                        if not ok:
                            logging.warning(f"TV trail: failed to cancel SL {sl_id} for {entry_id}")
                            continue
                        payload = {
                            "accountId": ctx['ACCOUNT_ID'],
                            "contractId": info.get("contractId"),
                            "type": 4,
                            "side": 1 - side,
                            "size": size,
                            "stopPrice": target,
                            "linkedOrderId": entry_id,
                        }
                        resp = ctx['api_post'](token, "/api/Order/place", payload)
                        if not resp.get("success"):
                            logging.warning(f"TV trail: replace SL failed for {entry_id}: {resp}")
                            continue
                        new_sl_id = resp.get("orderId")
                        info["sl_id"] = new_sl_id
                        info["stop_points"] = abs(entry_px - target)
                        ctx['active_entries'][entry_id] = info
                        if entry_id in ctx['oco_orders'] and isinstance(ctx['oco_orders'][entry_id], list) and len(ctx['oco_orders'][entry_id]) == 2:
                            ctx['oco_orders'][entry_id][1] = new_sl_id
                        logging.info("TV trail moved STOP to %.4f (entry %.4f, best %.4f, points %.4f)", target, entry_px, best, t_points)
                    except Exception:
                        continue
            except Exception:
                pass
            await asyncio.sleep(0.25)
    return monitor_tv_trailing
