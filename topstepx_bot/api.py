import requests
import logging


def get_token(api_url: str, username: str, api_key: str):
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


def api_post(api_url: str, token: str, endpoint: str, payload: dict):
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


def cancel_order(api_url: str, token: str, account_id, order_id):
    try:
        res = requests.post(
            f"{api_url}/api/Order/cancel",
            json={"accountId": account_id, "orderId": order_id},
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            verify=False,
        )
        res.raise_for_status()
        return res.json().get("success", False)
    except Exception as e:
        logging.error(f"Cancel failed for {order_id}: {e}")
        return False


def get_account_info(token: str):
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
        if not isinstance(accounts, list) or not accounts:
            logging.warning("No account data found.")
            return None
        return accounts[0]
    except Exception as e:
        logging.error(f"Account info fetch error: {e}")
        return None
