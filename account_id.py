#!/usr/bin/env python3
import os
import requests
import yaml
from datetime import datetime, timezone
from quart import Quart, request, jsonify

# ---------- Load config.yaml ----------
with open("config.yaml", "r") as f:
    CONFIG = yaml.safe_load(f) or {}

TSX = CONFIG.get("topstepx", {})  # <-- read nested section
API_BASE = TSX.get("base_url", "https://api.topstepx.com").rstrip("/")
USERNAME = TSX.get("username", "")
API_KEY  = TSX.get("api_key", "")
ACCOUNT_ID_INT = int(TSX.get("account_id", 0))         # integer accountId used by API
ACCOUNT_NUMBER = TSX.get("account_number", "")         # human-readable
CONTRACT_ID    = TSX.get("contract_id", "MNQZ5")       # e.g., MNQZ5 or your contract code

# Prefer v1 paths (Swagger), but we’ll fall back to /api/ casing if needed
PATHS = {
    "login":              "/api/Auth/loginKey",
    "order_place_v1":     "/v1/orders/place",
    "order_place_api":    "/api/Order/place",
    "orders_search_v1":   "/v1/orders/search",
    "positions_search_v1":"/v1/positions/search",
    "bars_retrieve_v1":   "/v1/market/bars/retrieve",
    # Balance discovery candidates (we'll probe):
    "me_accounts_v1":     "/v1/me/accounts",
    "accounts_v1":        "/v1/accounts",
    "account_by_id_v1":   "/v1/accounts/{id}",
    "account_summary_v1": "/v1/accounts/{id}/summary",
    "account_balance_v1": "/v1/accounts/{id}/balance",
}

# ---------- HTTP helpers ----------
def _hdr(token: str | None):
    h = {"Accept": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def _abs(path: str) -> str:
    return API_BASE + path

def _json(res: requests.Response):
    if "application/json" in res.headers.get("content-type", "").lower():
        return res.json()
    return None

# ---------- Auth: try loginKey first; else use API key directly ----------
def get_token() -> str | None:
    # Try loginKey (username + apiKey) if available
    try:
        url = _abs(PATHS["login"])
        res = requests.post(url, json={"userName": USERNAME, "apiKey": API_KEY}, timeout=10)
        if res.ok:
            data = _json(res) or {}
            if data.get("success") and data.get("token"):
                return data["token"]
    except Exception:
        pass
    # Fallback: some deployments accept API key directly as Bearer
    return API_KEY or None

# ---------- Orders (parent + linked children) ----------
def place_order(token: str, body: dict):
    """
    Try v1 first (/v1/orders/place). If 404, try /api/Order/place.
    Returns (ok, dict_or_text, status)
    """
    # v1 path
    try:
        res = requests.post(_abs(PATHS["order_place_v1"]), json=body, headers=_hdr(token) | {"Content-Type":"application/json"}, timeout=10)
        if res.ok:
            return True, _json(res) or res.text, res.status_code
        if res.status_code != 404:
            return False, res.text, res.status_code
    except Exception as e:
        last_err = str(e)
    # /api path fallback
    try:
        res = requests.post(_abs(PATHS["order_place_api"]), json=body, headers=_hdr(token) | {"Content-Type":"application/json"}, timeout=10)
        return (res.ok, _json(res) or res.text, res.status_code)
    except Exception as e:
        return False, f"exception: {e}", 0

def place_parent_and_bracket(token: str, qty: int, entry_price: float, tp_price: float, sl_price: float, side_buy: bool):
    """
    Places parent LIMIT order, then two linked children (TP: LIMIT, SL: STOP).
    Uses TopstepX enums: type (1=Limit,2=Market,4=Stop), side (0=Bid/BUY,1=Ask/SELL)
    """
    side_parent = 0 if side_buy else 1
    side_child  = 1 - side_parent
    # 1) Parent LIMIT
    parent_body = {
        "accountId": ACCOUNT_ID_INT,
        "contractId": CONTRACT_ID,
        "type": 1,             # Limit
        "side": side_parent,   # 0=BUY, 1=SELL
        "size": int(abs(qty)),
        "limitPrice": float(entry_price),
        "customTag": "duvenchy-oco"
    }
    ok, data, sc = place_order(token, parent_body)
    if not ok or not isinstance(data, dict) or not data.get("success"):
        return False, {"error": f"Parent order failed", "status": sc, "resp": data}
    parent_id = data.get("orderId")
    if not parent_id:
        return False, {"error": "Parent order missing orderId", "status": sc, "resp": data}

    # 2) TP LIMIT (linked)
    tp_body = {
        "accountId": ACCOUNT_ID_INT,
        "contractId": CONTRACT_ID,
        "type": 1,                # Limit
        "side": side_child,       # opposite side
        "size": int(abs(qty)),
        "limitPrice": float(tp_price),
        "linkedOrderId": int(parent_id),
        "customTag": "duvenchy-oco-tp"
    }
    ok_tp, data_tp, sc_tp = place_order(token, tp_body)
    if not ok_tp or not isinstance(data_tp, dict) or not data_tp.get("success"):
        return False, {"error": "TP order failed", "status": sc_tp, "resp": data_tp, "parentId": parent_id}

    # 3) SL STOP (linked)
    sl_body = {
        "accountId": ACCOUNT_ID_INT,
        "contractId": CONTRACT_ID,
        "type": 4,                # Stop
        "side": side_child,
        "size": int(abs(qty)),
        "stopPrice": float(sl_price),
        "linkedOrderId": int(parent_id),
        "customTag": "duvenchy-oco-sl"
    }
    ok_sl, data_sl, sc_sl = place_order(token, sl_body)
    if not ok_sl or not isinstance(data_sl, dict) or not data_sl.get("success"):
        return False, {"error": "SL order failed", "status": sc_sl, "resp": data_sl, "parentId": parent_id}

    return True, {
        "entryOrderId": parent_id,
        "takeProfitOrderId": data_tp.get("orderId"),
        "stopLossOrderId": data_sl.get("orderId"),
        "message": "OCO bracket placed"
    }

# ---------- Account balance (best-effort discovery) ----------
def discover_account_summary(token: str):
    """
    Try common endpoints, return normalized summary dict if found.
    """
    tries = []
    def try_get(url):
        try:
            r = requests.get(url, headers=_hdr(token), timeout=10)
            return r
        except Exception as e:
            return None

    base = API_BASE
    candidates = [
        _abs(PATHS["me_accounts_v1"]),
        _abs(PATHS["accounts_v1"]),
    ]
    if ACCOUNT_NUMBER:
        candidates += [
            f"{base}/v1/accounts?number={ACCOUNT_NUMBER}",
            f"{base}/v1/accounts?accountNumber={ACCOUNT_NUMBER}",
        ]
    if ACCOUNT_ID_INT:
        candidates += [
            _abs(PATHS["account_by_id_v1"].replace("{id}", str(ACCOUNT_ID_INT))),
            _abs(PATHS["account_summary_v1"].replace("{id}", str(ACCOUNT_ID_INT))),
            _abs(PATHS["account_balance_v1"].replace("{id}", str(ACCOUNT_ID_INT))),
        ]

    for url in candidates:
        r = try_get(url)
        code = r.status_code if r else 0
        ctype = r.headers.get("content-type","") if r else ""
        tries.append(f"{code} {url}")
        if not r or not r.ok or "application/json" not in ctype.lower():
            continue

        data = _json(r)
        if data is None:
            continue

        # normalize: list of accounts or single dict
        if isinstance(data, dict) and "accounts" in data and isinstance(data["accounts"], list):
            accounts = data["accounts"]
        elif isinstance(data, list):
            accounts = data
        elif isinstance(data, dict):
            accounts = [data]
        else:
            continue

        # Pick matching account
        chosen = None
        for acc in accounts:
            aid = acc.get("id") or acc.get("accountId") or acc.get("account_id")
            num = acc.get("number") or acc.get("accountNumber") or acc.get("name")
            if (isinstance(aid, int) and aid == ACCOUNT_ID_INT) or (ACCOUNT_NUMBER and str(num) == str(ACCOUNT_NUMBER)):
                chosen = acc
                break
        if chosen is None and accounts:
            chosen = accounts[0]

        def pick(obj, *keys, default=None):
            for k in keys:
                if k in obj and isinstance(obj[k], (int, float, str)):
                    return obj[k]
            return default

        summary = {
            "id": pick(chosen, "id","accountId","account_id"),
            "number": pick(chosen, "number","accountNumber","name"),
            "balance": pick(chosen, "balance","cash","cashBalance","balanceValue","buyingPower","netLiq","equity"),
            "equity": pick(chosen, "equity","netLiq","netLiquidation","accountEquity","balance"),
            "realizedPnL": pick(chosen, "realizedPnL","pnlRealized","realizedProfit","rpnl"),
            "unrealizedPnL": pick(chosen, "unrealizedPnL","pnlUnrealized","upnl"),
            "_source": url
        }
        return True, summary

    return False, {"error": "No JSON account endpoint matched", "tried": tries}

# ---------- Quart app ----------
app = Quart(__name__)

@app.route("/place-oco", methods=["POST"])
async def place_oco():
    payload = await request.get_json()
    if not payload:
        return jsonify({"error": "JSON body required"}), 400

    try:
        quantity = int(payload.get("quantity", 1))
        op = float(payload.get("op"))
        tp = float(payload.get("tp"))
        sl = float(payload.get("sl"))
    except Exception:
        return jsonify({"error": "quantity/op/tp/sl must be numeric"}), 400

    if op is None or tp is None or sl is None:
        return jsonify({"error": "Missing op, tp, or sl"}), 400

    side_buy = quantity > 0
    token = get_token()
    if not token:
        return jsonify({"error": "Authentication failed"}), 500

    ok, resp = place_parent_and_bracket(token, quantity, op, tp, sl, side_buy)
    return (jsonify(resp), 200) if ok else (jsonify(resp), 500)

@app.route("/balance", methods=["GET"])
async def balance():
    token = get_token()
    if not token:
        return jsonify({"error": "Authentication failed"}), 500

    ok, info = discover_account_summary(token)
    return (jsonify(info), 200) if ok else (jsonify(info), 404)

# ---------- Run ----------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    print(f"API base: {API_BASE}  |  accountId: {ACCOUNT_ID_INT}  |  contractId: {CONTRACT_ID}")
    app.run(port=port)
