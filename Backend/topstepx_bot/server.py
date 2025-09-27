import asyncio
import logging
import time
import threading
import datetime as dt
from pathlib import Path
from typing import Any, Dict
from quart import Quart, request, jsonify, send_from_directory


def create_app(ctx: Dict[str, Any]) -> Quart:
    app = Quart(__name__)

    get_token = ctx['get_token']
    api_post = ctx['api_post']
    get_account_info = ctx['get_account_info']
    cancel_order = ctx['cancel_order']
    contract_map = ctx['contract_map']
    bars_by_symbol = ctx['bars_by_symbol']
    _bars_lock = ctx['bars_lock']
    indicator_state = ctx['indicator_state']
    last_price = ctx['last_price']
    oco_orders = ctx['oco_orders']
    active_entries = ctx['active_entries']
    account_snapshot = ctx['account_snapshot']
    streamers = ctx['streamers']
    SYMBOL = ctx['SYMBOL']
    ACCOUNT_ID = ctx['ACCOUNT_ID']
    LIVE_FLAG = ctx['LIVE_FLAG']
    DEBUG = ctx.get('DEBUG', False)
    snap_to_tick = ctx['snap_to_tick']
    fmt_num = ctx['fmt_num']

    raw_frontend_root = ctx.get('FRONTEND_ROOT')
    try:
        frontend_root = Path(raw_frontend_root).resolve() if raw_frontend_root else Path(__file__).resolve().parents[2] / "Frontend"
    except Exception:
        frontend_root = Path(__file__).resolve().parents[2] / "Frontend"
    frontend_index = str(ctx.get('FRONTEND_INDEX') or 'index.html')

    def mask_secret(secret: Any, visible: int = 4) -> str:
        if not secret:
            return ""
        secret_str = str(secret)
        if len(secret_str) <= 2:
            return "*" * len(secret_str)
        if len(secret_str) <= visible * 2:
            return secret_str[0] + ("*" * (len(secret_str) - 2)) + secret_str[-1]
        return f"{secret_str[:visible]}{'*' * (len(secret_str) - (visible * 2))}{secret_str[-visible:]}"

    async def serve_frontend_asset(filename: str):
        if not frontend_root.exists():
            return jsonify({"error": "UI assets not found"}), 404
        try:
            return await send_from_directory(str(frontend_root), filename)
        except Exception:
            return jsonify({"error": "Asset not found"}), 404

    @app.get("/")
    async def ui_index():
        return await serve_frontend_asset(frontend_index)

    @app.get("/ui/")
    async def ui_root():
        return await serve_frontend_asset(frontend_index)


    async def place_oco_generic(data, entry_type):
        quantity = int(data.get("quantity", 1))
        op = data.get("op")
        tp = data.get("tp")
        sl = data.get("sl")
        symbol = data.get("symbol", "").upper()
        use_trail = bool(data.get("useTrail", False))
        try:
            trail_ticks_req = int(data.get("trailTicks")) if data.get("trailTicks") is not None else None
        except Exception:
            trail_ticks_req = None
        if not symbol:
            symbol = SYMBOL

        contract = contract_map.get(symbol)
        if not contract:
            return jsonify({"error": f"Unknown symbol: {symbol}"}), 400

        tick_size = contract["tickSize"]
        tick_value = contract["tickValue"]
        contract_id = contract["contractId"]
        decimals = int(contract.get("decimalPlaces", 2))

        token = get_token()
        if not token:
            return jsonify({"error": "Authentication failed"}), 500

        account_info = get_account_info(token)
        if not account_info:
            return jsonify({"error": "Failed to fetch account data"}), 500

        balance = account_info.get("balance")
        maximum_loss = account_info.get("maximumLoss")
        if balance is None or maximum_loss is None:
            return jsonify({"error": "Missing account data"}), 500

        try:
            op = snap_to_tick(float(op), float(tick_size), decimals)
            tp = snap_to_tick(float(tp), float(tick_size), decimals)
            sl = snap_to_tick(float(sl), float(tick_size), decimals)
        except Exception:
            pass

        sl_ticks = abs(op - sl) / tick_size
        if sl_ticks == 0:
            return jsonify({"error": "SL too close to OP"}), 400

        risk_budget = (balance - maximum_loss) * 0.24
        quantity = int(risk_budget / (sl_ticks * tick_value))
        if quantity <= 0:
            return jsonify({"error": "Calculated quantity is zero"}), 400

        micro_to_standard = {"MNQ": "NQ", "MYM": "YM", "MGC": "GC", "MES": "ES"}
        if quantity >= 10 and symbol in micro_to_standard:
            symbol = micro_to_standard[symbol]
            contract = contract_map.get(symbol)
            if not contract:
                return jsonify({"error": f"Standard symbol not found: {symbol}"}), 400
            contract_id = contract["contractId"]
            tick_size = contract["tickSize"]
            tick_value = contract["tickValue"]
            quantity = int(risk_budget / (sl_ticks * tick_value))

        if quantity > 3:
            quantity = 3

        side = 0 if op < tp else 1
        size = abs(quantity)
        entry = api_post(token, "/api/Order/place", {
            "accountId": ACCOUNT_ID,
            "contractId": contract_id,
            "type": entry_type,
            "side": side,
            "size": size,
            "limitPrice": op if entry_type == 1 else None,
            "stopPrice": op if entry_type == 4 else None,
        })
        entry_id = entry.get("orderId")
        if not entry.get("success") or not entry_id:
            return jsonify({"error": "Entry order failed"}), 500
        await asyncio.sleep(0.3)
        tp_order = api_post(token, "/api/Order/place", {
            "accountId": ACCOUNT_ID,
            "contractId": contract_id,
            "type": 1,
            "side": 1 - side,
            "size": size,
            "limitPrice": tp,
            "linkedOrderId": entry_id,
        })
        await asyncio.sleep(0.3)
        sl_order = None
        sl_is_trailing = False
        if use_trail:
            # Try native trailing stop (type=5) using several variants
            try:
                trail_ticks = int(max(1, int(trail_ticks_req if trail_ticks_req is not None else int(ctx.get('TRAIL_TICKS_FIXED', 5)))) )
            except Exception:
                trail_ticks = 5
            stop_side = 1 - side
            # compute absolute start price when possible
            stop_init = None
            try:
                if op is not None and tick_size and float(tick_size) > 0:
                    offset_points = float(abs(int(trail_ticks))) * float(tick_size)
                    # protect long -> SELL stop below OP; protect short -> BUY stop above OP
                    stop_init = float(op) - offset_points if stop_side == 1 else float(op) + offset_points
                    try:
                        stop_init = snap_to_tick(float(stop_init), float(tick_size), int(decimals))
                    except Exception:
                        pass
            except Exception:
                stop_init = None

            base = {
                "accountId": ACCOUNT_ID,
                "contractId": contract_id,
                "type": 5,
                "side": int(stop_side),
                "size": int(size),
                "linkedOrderId": entry_id,
            }
            attempts = []
            # signed and positive trail ticks variants
            try:
                t_signed_pos = int(trail_ticks)
                t_signed_neg = -int(trail_ticks)
            except Exception:
                t_signed_pos = int(trail_ticks)
                t_signed_neg = -int(trail_ticks)
            attempts.append(("trailticks+", dict(base, trailTicks=int(t_signed_pos))))
            attempts.append(("trailticks-", dict(base, trailTicks=int(t_signed_neg))))
            if stop_init is not None:
                attempts.append(("distanceprice", dict(base, distancePrice=float(stop_init))))
                attempts.append(("trailprice", dict(base, trailPrice=float(stop_init))))
            attempts.append(("traildistance", dict(base, trailDistance=int(trail_ticks))))
            attempts.append(("distance", dict(base, distance=int(trail_ticks))))
            attempts.append(("distance+traildistance", dict(base, distance=int(trail_ticks), trailDistance=int(trail_ticks))))
            attempts.append(("distanceticks", dict(base, distanceTicks=int(trail_ticks))))
            attempts.append(("trailingdistance", dict(base, trailingDistance=int(trail_ticks))))

            preferred = str(ctx.get('TRAIL_VARIANT') or '').strip().lower()
            if preferred:
                attempts.sort(key=lambda x: (x[0] != preferred,))

            # diagnostic buffer of trail attempts
            trail_log = ctx.setdefault('LAST_TRAIL_ATTEMPTS', [])
            for name, payload in attempts:
                resp = api_post(token, "/api/Order/place", payload)
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
                    sl_order = resp
                    sl_is_trailing = True
                    # Remember working variant for future preference
                    try:
                        ctx['TRAIL_VARIANT'] = name
                    except Exception:
                        pass
                    break
            # slight cooldown between child orders
            await asyncio.sleep(0.2)

        if sl_order is None:
            # Fallback to fixed stop
            sl_order = api_post(token, "/api/Order/place", {
                "accountId": ACCOUNT_ID,
                "contractId": contract_id,
                "type": 4,
                "side": 1 - side,
                "size": size,
                "stopPrice": sl,
                "linkedOrderId": entry_id,
            })
        tp_id = tp_order.get("orderId")
        sl_id = sl_order.get("orderId")
        oco_orders[entry_id] = [tp_id, sl_id]
        # Track in active_entries to allow robust cleanup (e.g., manual flat)
        try:
            active_entries[entry_id] = {
                "symbol": symbol,
                "contractId": contract_id,
                "side": int(side),
                "size": int(size),
                "entry": float(op) if op is not None else None,
                "stop_points": abs(float(op) - float(sl)) if (op is not None and sl is not None) else None,
                "tp_id": tp_id,
                "sl_id": sl_id,
                "native_trail": False,
                "tickSize": float(tick_size),
                "decimals": int(decimals),
            }
        except Exception:
            pass
        return jsonify({
            "entryOrderId": entry_id,
            "takeProfitOrderId": tp_id,
            "stopLossOrderId": sl_id,
            "contractId": contract_id,
            "tickSize": contract["tickSize"],
            "tickValue": contract["tickValue"],
            "balance": balance,
            "maximum_loss": maximum_loss,
            "risk_budget": risk_budget,
            "message": "OCO placed (trailing SL)" if sl_is_trailing else "OCO placed",
            "slIsTrailing": bool(sl_is_trailing),
        })

    @app.route("/place-oco", methods=["POST"])
    async def place_oco():
        data = await request.get_json()
        return await place_oco_generic(data, entry_type=1)

    @app.route("/place-oco-stop", methods=["POST"])
    async def place_oco_stop():
        data = await request.get_json()
        return await place_oco_generic(data, entry_type=4)

    @app.route("/balance", methods=["GET"])
    async def balance():
        token = get_token()
        if not token:
            return jsonify({"error": "Authentication failed"}), 500
        account_info = get_account_info(token)
        if not account_info:
            return jsonify({"error": "Failed to fetch account data"}), 500
        balance = account_info.get("balance")
        maximum_loss = account_info.get("maximumLoss")
        return jsonify({"balance": balance, "maximumLoss": maximum_loss})

    @app.get("/bars")
    async def get_bars():
        symbol = (request.args.get("symbol") or SYMBOL).upper()
        limit = int(request.args.get("limit", 200))
        include_current = str(request.args.get("includeCurrent", "0")).lower() in ("1", "true", "yes")
        with _bars_lock:
            out = list(bars_by_symbol.get(symbol, []))[-limit:]
        if include_current:
            try:
                ms = (ctx.get('streamers') or {}).get(symbol)
                if ms and getattr(ms, 'cur_bar', None):
                    cur = dict(ms.cur_bar)
                    # avoid duplicate minute if it matches last finalized bar
                    if not out or out[-1].get('t') != cur.get('t'):
                        out = (out + [cur])[-limit:]
            except Exception:
                pass
        return jsonify({"symbol": symbol, "bars": out})

    @app.get("/diag/trail")
    async def diag_trail():
        try:
            attempts = ctx.get('LAST_TRAIL_ATTEMPTS') or []
            return jsonify({"count": len(attempts), "attempts": attempts})
        except Exception:
            return jsonify({"count": 0, "attempts": []})

    @app.get("/price")
    async def get_price():
        symbol = (request.args.get("symbol") or SYMBOL).upper()
        px = last_price.get(symbol)
        return jsonify({"symbol": symbol, "price": px})

    @app.get("/indicators")
    async def get_indicators():
        symbol = (request.args.get("symbol") or SYMBOL).upper()
        real_only = str(request.args.get("realOnly", "0")).lower() in ("1", "true", "yes")
        state = indicator_state.get(symbol) or {}
        # Compute bar counts and real bar progress
        try:
            with _bars_lock:
                bars = list(bars_by_symbol.get(symbol, []))
            total_bars = len(bars)
            syn_bars = sum(1 for b in bars if b.get('syn') is True)
            real_bars = max(0, total_bars - syn_bars)
        except Exception:
            total_bars = syn_bars = real_bars = 0
        rb_stream = None
        try:
            ms = (streamers or {}).get(symbol)
            rb_stream = int(getattr(ms, "_real_bars", 0)) if ms else None
        except Exception:
            rb_stream = None
        out = dict(state)
        if real_only:
            ef_r = out.get("emaFastReal"); es_r = out.get("emaSlowReal")
            out["emaFast"] = ef_r if ef_r is not None else None
            out["emaSlow"] = es_r if es_r is not None else None
        # Annotate with counts and which variant is primary
        try:
            uses_real_primary = (out.get("emaFast") == (out.get("emaFastReal") if out.get("emaFastReal") is not None else out.get("emaFast")))
        except Exception:
            uses_real_primary = False
        out.update({
            "barCounts": {"total": total_bars, "real": real_bars, "synthetic": syn_bars},
            "realBars": rb_stream if rb_stream is not None else real_bars,
            "usesRealPrimary": bool(out.get("emaFastReal") is not None and out.get("emaSlowReal") is not None),
        })
        return jsonify({"symbol": symbol, **out})

    @app.get("/wiring")
    async def wiring():
        try:
            synth_min = int(ctx.get('SYNTH_WARMUP_MINUTES') or 0)
        except Exception:
            synth_min = 0
        info = {
            "symbol": SYMBOL,
            "accountId": ACCOUNT_ID,
            "EMA_SHORT": int(ctx.get('EMA_SHORT')),
            "EMA_LONG": int(ctx.get('EMA_LONG')),
            "EMA_SOURCE": ctx.get('EMA_SOURCE', 'close'),
            "RTH_ONLY": bool(ctx.get('RTH_ONLY', False)),
            "USE_VWAP": bool(ctx.get('USE_VWAP', False)),
            "LIVE_FLAG": bool(LIVE_FLAG),
            "SYNTH_WARMUP_MINUTES": synth_min,
            "MIN_REAL_BARS_BEFORE_TRADING": int(ctx.get('MIN_REAL_BARS_BEFORE_TRADING', 0) or 0),
        }
        return jsonify(info)

    def get_open_orders_count(contract_id: Any = None) -> int:
        token = get_token()
        if not token:
            return 0
        resp = api_post(token, "/api/Order/searchOpen", {"accountId": ACCOUNT_ID})
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

    @app.get("/trading/status")
    async def trading_status():
        cnt = get_open_orders_count()
        return jsonify({"accountId": ACCOUNT_ID, "symbol": SYMBOL, "openOrders": cnt, "active": cnt > 0})

    @app.get("/ui/state")
    async def ui_state():
        balance = account_snapshot.get('balance')
        equity = account_snapshot.get('equity')
        maximum_loss = None
        account_status = None
        account_name = None
        token_ok = False
        fetch_error = None
        try:
            token = get_token()
            if token:
                token_ok = True
                acct = get_account_info(token) or {}
                balance = acct.get('balance', balance)
                equity = acct.get('equity', equity)
                maximum_loss = acct.get('maximumLoss')
                account_status = (acct.get('status') or acct.get('state') or acct.get('accountStatus') or acct.get('lifecycleState'))
                account_name = acct.get('name') or acct.get('accountName') or acct.get('displayName')
        except Exception as exc:
            fetch_error = str(exc).split('\n')[0][:160]
        try:
            open_orders = get_open_orders_count()
        except Exception:
            open_orders = None
        streamer = streamers.get(SYMBOL)
        return jsonify({
            "username": ctx.get('USERNAME') or "",
            "apiKeyMasked": mask_secret(ctx.get('API_KEY')), 
            "accountId": ACCOUNT_ID,
            "accountName": account_name,
            "symbol": SYMBOL,
            "emaShort": ctx.get('EMA_SHORT'),
            "emaLong": ctx.get('EMA_LONG'),
            "emaSource": ctx.get('EMA_SOURCE'),
            "useVWAP": bool(ctx.get('USE_VWAP')),
            "rthOnly": bool(ctx.get('RTH_ONLY', False)),
            "tokenValid": bool(token_ok),
            "tradingDisabled": bool(ctx.get('TRADING_DISABLED')),
            "tradingDisabledReason": ctx.get('TRADING_DISABLED_REASON'),
            "openOrders": open_orders,
            "balance": balance,
            "equity": equity,
            "maximumLoss": maximum_loss,
            "accountStatus": account_status,
            "connected": bool(streamer),
            "lastPrice": last_price.get(SYMBOL),
            "error": fetch_error,
            "timestamp": int(time.time()),
        })

    @app.get("/ui/<path:filename>")
    async def ui_static(filename: str):
        return await serve_frontend_asset(filename)

    @app.get("/diag/status")
    async def diag_status():
        return jsonify({
            'accountId': ACCOUNT_ID,
            'symbol': SYMBOL,
            'tradingDisabled': bool(ctx.get('TRADING_DISABLED')),
            'reason': ctx.get('TRADING_DISABLED_REASON'),
            'accountState': ctx.get('ACCOUNT_STATE'),
            'balance': ctx.get('account_snapshot', {}).get('balance'),
            'equity': ctx.get('account_snapshot', {}).get('equity'),
        })

    @app.get("/diag/last-order-fail")
    async def diag_last_order_fail():
        lof = ctx.get('LAST_ORDER_FAIL')
        return jsonify(lof or {})

    @app.post("/trading/enable")
    async def trading_enable():
        # manual override to clear kill-switch
        ctx['TRADING_DISABLED'] = False
        ctx['TRADING_DISABLED_REASON'] = None
        return jsonify({"ok": True})

    @app.post("/trading/disable")
    async def trading_disable():
        data = await request.get_json(silent=True) or {}
        reason = data.get('reason') or 'manual toggle'
        ctx['TRADING_DISABLED'] = True
        ctx['TRADING_DISABLED_REASON'] = reason
        return jsonify({"ok": True, "reason": reason})

    @app.before_serving
    async def startup():
        # Load contracts and start monitors
        ctx['load_contracts']()
        asyncio.create_task(ctx['monitor_oco_orders']())
        asyncio.create_task(ctx['monitor_synth_trailing']())
        asyncio.create_task(ctx['monitor_account_snapshot']())
        # Proactive account-state gate before warmup/stream
        try:
            tok0 = get_token()
            if tok0:
                acct0 = get_account_info(tok0) or {}
                def _val_s(obj, key):
                    v = obj.get(key)
                    return str(v).strip().lower() if v is not None else None
                closed = False
                reason = None
                try:
                    aid = acct0.get('id') or acct0.get('accountId') or acct0.get('tradingAccountId')
                    aname = acct0.get('name') or acct0.get('accountName') or acct0.get('displayName')
                    logging.info("Startup account snapshot | accountId=%s name=%s", str(aid), str(aname))
                except Exception:
                    pass
                for kb in ('isClosed', 'closed', 'tradingDisabled', 'disabled'):
                    vb = acct0.get(kb)
                    if isinstance(vb, bool) and vb:
                        closed = True
                        reason = f"{kb}=true"
                        break
                if not closed:
                    for ks in ('status', 'state', 'accountStatus', 'tradingStatus', 'lifecycleState'):
                        vs = _val_s(acct0, ks)
                        if not vs:
                            continue
                        if any(term in vs for term in ('closed', 'suspend', 'disable', 'locked', 'terminated')):
                            closed = True
                            reason = f"{ks}={vs}"
                            break
                if closed:
                    ctx['TRADING_DISABLED'] = True
                    ctx['TRADING_DISABLED_REASON'] = reason or 'account closed'
                    logging.error("Trading disabled: %s", ctx['TRADING_DISABLED_REASON'])
        except Exception:
            pass
        # Resolve contract
        contract = (contract_map.get(SYMBOL)
                    or contract_map.get(f"US.{SYMBOL}")
                    or next((v for k, v in contract_map.items() if k.upper().startswith(f"{SYMBOL}.") or k.upper().endswith(f".{SYMBOL}")), None))
        if not contract:
            logging.warning(f"No contract info for {SYMBOL}; verify symbol or load_contracts() output")
            return
        contract_id = contract["contractId"]
        try:
            logging.info(
                "Warmup params | symbol=%s contractId=%s live=%s hours=%s",
                SYMBOL,
                str(contract_id),
                str(LIVE_FLAG),
                str(ctx.get('BOOTSTRAP_HISTORY_HOURS')),
            )
            synth_min = int(ctx.get('SYNTH_WARMUP_MINUTES') or 0)
            if synth_min <= 0:
                logging.info("Warmup mode: synthetic disabled; waiting for real minute bars to accumulate")
            else:
                logging.info("Warmup mode: synthetic enabled (%d minutes)", synth_min)
        except Exception:
            pass
        ctx['warmup_bars'](SYMBOL, contract_id, days=1, unit=2, unit_n=1, live=LIVE_FLAG)

        def run_stream():
            tok = get_token()
            if not tok:
                logging.error("Market stream auth failed")
                return
            ms = ctx['MarketStreamer'](SYMBOL, contract_id, unit=2, unit_n=1)
            ctx['seed_streamer_from_warmup'](ms)
            try:
                # Prefer indicator_state values (which are real-first); fall back to snapshot when absent
                st = indicator_state.get(SYMBOL) or {}
                ef = st.get("emaFast") or st.get("emaFastReal") or st.get("emaFastSeeded")
                es = st.get("emaSlow") or st.get("emaSlowReal") or st.get("emaSlowSeeded")
                vw = st.get("vwap")
                if (ef is None) or (es is None):
                    snap = ms._pd_indicator_snapshot() or {}
                    ef = ef or snap.get("emaFast")
                    es = es or snap.get("emaSlow")
                    vw = vw or snap.get("vwap")
                    prev_rel = snap.get("prev_rel")
                    if prev_rel is not None:
                        ms._last_rel = float(prev_rel)
                op = last_price.get(SYMBOL)
                if op is None:
                    with _bars_lock:
                        bars = list(bars_by_symbol.get(SYMBOL, []))
                    if bars:
                        try:
                            op = float(bars[-1].get("c"))
                        except Exception:
                            op = None
                atr_val = ms.atr.value
                if DEBUG:
                    logging.info("Warmup check | price=%s ef=%s es=%s vwap=%s atr=%s", str(op), str(ef), str(es), str(vw), str(atr_val))
                if (op is not None) and (ef is not None) and (es is not None) and (atr_val is not None):
                    ms._maybe_trade(float(op), float(ef), float(es), vw, float(atr_val))
            except Exception:
                pass
            ms.start(tok)
            streamers[SYMBOL] = ms
            while True:
                time.sleep(1)

        threading.Thread(target=run_stream, daemon=True).start()

    return app
