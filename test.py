import requests
import yaml
from quart import Quart, request, jsonify

# --- Load config.yaml ---
with open("config.yaml") as f:
    config = yaml.safe_load(f)

API_URL = "https://api.topstepx.com"
USERNAME = config["username"]
API_KEY = config["api_key"]
ACCOUNT_ID = int(config["account_id"])
CONTRACT_ID = "CON.F.US.YM.U25"

app = Quart(__name__)

# --- Auth ---
def get_token():
    try:
        res = requests.post(
            f"{API_URL}/api/Auth/loginKey",
            json={"userName": USERNAME, "apiKey": API_KEY},
            headers={"Content-Type": "application/json"},
            timeout=10,
            verify=False  # Disable SSL verification for dev
        )
        res.raise_for_status()
        data = res.json()
        return data.get("token") if data.get("success") else None
    except Exception:
        return None

# --- API Calls ---
def api_post(token, endpoint, payload):
    res = requests.post(
        f"{API_URL}{endpoint}",
        json=payload,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        verify=False
    )
    res.raise_for_status()
    return res.json()

# --- OCO Bracket Route ---
@app.route("/place-oco", methods=["POST"])
async def place_oco():
    data = await request.get_json()
    quantity = int(data.get("quantity", 1))
    op = data.get("op")  # entry price
    tp = data.get("tp")  # take profit
    sl = data.get("sl")  # stop loss

    if not op or not tp or not sl:
        return jsonify({"error": "Missing op, tp, or sl"}), 400

    side = 0 if quantity > 0 else 1  # 0 = buy, 1 = sell
    size = abs(quantity)

    token = get_token()
    if not token:
        return jsonify({"error": "Authentication failed"}), 500

    try:
        # 1. Entry LIMIT order
        entry_order = api_post(token, "/api/Order/place", {
            "accountId": ACCOUNT_ID,
            "contractId": CONTRACT_ID,
            "type": 1,  # Limit
            "side": side,
            "size": size,
            "limitPrice": op
        })
        entry_id = entry_order.get("orderId")
        if not entry_order.get("success") or not entry_id:
            return jsonify({"error": "Entry order failed"}), 500

        # 2. Take-profit LIMIT
        tp_order = api_post(token, "/api/Order/place", {
            "accountId": ACCOUNT_ID,
            "contractId": CONTRACT_ID,
            "type": 1,
            "side": 1 - side,
            "size": size,
            "limitPrice": tp,
            "linkedOrderId": entry_id
        })

        # 3. Stop-loss STOP
        sl_order = api_post(token, "/api/Order/place", {
            "accountId": ACCOUNT_ID,
            "contractId": CONTRACT_ID,
            "type": 4,
            "side": 1 - side,
            "size": size,
            "stopPrice": sl,
            "linkedOrderId": entry_id
        })

        return jsonify({
            "entryOrderId": entry_id,
            "takeProfitOrderId": tp_order.get("orderId"),
            "stopLossOrderId": sl_order.get("orderId"),
            "message": "OCO bracket placed"
        })
    except Exception:
        return jsonify({"error": "OCO placement failed"}), 500

# --- Run ---
if __name__ == "__main__":
    app.run(port=5000)