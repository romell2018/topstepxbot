import asyncio
import logging
import time
import threading
import datetime as dt
from typing import Any, Dict
from quart import Quart, request, jsonify


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

    async def place_oco_generic(data, entry_type):
        quantity = int(data.get("quantity", 1))
        op = data.get("op")
        tp = data.get("tp")
        sl = data.get("sl")
        symbol = data.get("symbol", "").upper()
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
        sl_order = api_post(token, "/api/Order/place", {
            "accountId": ACCOUNT_ID,
            "contractId": contract_id,
            "type": 4,
            "side": 1 - side,
            "size": size,
            "stopPrice": sl,
            "linkedOrderId": entry_id,
        })
        oco_orders[entry_id] = [tp_order.get("orderId"), sl_order.get("orderId")]
        return jsonify({
            "entryOrderId": entry_id,
            "takeProfitOrderId": tp_order.get("orderId"),
            "stopLossOrderId": sl_order.get("orderId"),
            "contractId": contract_id,
            "tickSize": contract["tickSize"],
            "tickValue": contract["tickValue"],
            "balance": balance,
            "maximum_loss": maximum_loss,
            "risk_budget": risk_budget,
            "message": "OCO placed",
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
        if real_only:
            out = dict(state)
            ef_r = out.get("emaFastReal"); es_r = out.get("emaSlowReal")
            out["emaFast"] = ef_r if ef_r is not None else None
            out["emaSlow"] = es_r if es_r is not None else None
            return jsonify({"symbol": symbol, **out})
        return jsonify({"symbol": symbol, **state})

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

    @app.before_serving
    async def startup():
        # Load contracts and start monitors
        ctx['load_contracts']()
        asyncio.create_task(ctx['monitor_oco_orders']())
        asyncio.create_task(ctx['monitor_break_even']())
        asyncio.create_task(ctx['monitor_account_snapshot']())
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
