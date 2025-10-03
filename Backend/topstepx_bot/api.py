import requests
import logging
import time
import threading
import os


# Simple in-process token cache with throttle/backoff to avoid 429 storms
_token_lock = threading.Lock()
_token_cache = {
    'token': None,           # last good token
    'ts': 0.0,               # when token was obtained
    'ttl': 60.0 * 30,        # assume token valid for 30 minutes (adjust if needed)
    'last_attempt': 0.0,     # last login attempt ts
    'min_interval': 10.0,    # don't hammer login more often than this (sec)
    'backoff_until': 0.0,    # if 429, wait until this ts before next login
}


def get_token(api_url: str, username: str, api_key: str):
    now = time.time()
    with _token_lock:
        # Use cached token if still fresh
        if _token_cache['token'] and (now - _token_cache['ts'] < _token_cache['ttl']):
            return _token_cache['token']
        # Respect backoff after a 429
        if now < _token_cache['backoff_until']:
            # During backoff, return whatever we have (possibly None)
            return _token_cache['token']
        # Throttle login frequency across concurrent callers
        if (now - _token_cache['last_attempt']) < _token_cache['min_interval']:
            return _token_cache['token']
        _token_cache['last_attempt'] = now

    # Perform login outside the lock
    try:
        res = requests.post(
            f"{api_url}/api/Auth/loginKey",
            json={"userName": username, "apiKey": api_key},
            headers={"Content-Type": "application/json"},
            timeout=10,
            verify=_tls_verify_param(),
        )
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as he:  # type: ignore
            # Handle 429 explicitly with a backoff window
            status = getattr(he.response, 'status_code', None)
            if status == 429:
                retry_after = 0
                try:
                    ra = he.response.headers.get('Retry-After') if he.response is not None else None
                    if ra is not None:
                        retry_after = int(float(ra))
                except Exception:
                    retry_after = 0
                backoff = max(30.0, float(retry_after))
                with _token_lock:
                    _token_cache['backoff_until'] = time.time() + backoff
                logging.error(f"Auth error: 429 Too Many Requests. Backing off for {int(backoff)}s")
                return _token_cache['token']
            # Other HTTP errors
            logging.error(f"Auth error: {he}")
            return _token_cache['token']
        data = res.json()
        token = data.get("token") if data.get("success") else None
        if token:
            with _token_lock:
                _token_cache['token'] = token
                _token_cache['ts'] = time.time()
                # Reset backoff on success
                _token_cache['backoff_until'] = 0.0
        else:
            logging.error("Auth error: loginKey returned no token")
        return token
    except Exception as e:
        logging.error(f"Auth error: {e}")
        return _token_cache['token']


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
            verify=_tls_verify_param(),
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
            verify=_tls_verify_param(),
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
            verify=_tls_verify_param(),
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
def _tls_verify_param():
    """Return the 'verify' parameter for requests:
    - If TOPSTEPX_CA_BUNDLE/REQUESTS_CA_BUNDLE/SSL_CERT_FILE is set, return that path
    - Else if TOPSTEPX_TLS_VERIFY is set, coerce to boolean
    - Else default to True (verify TLS)
    """
    for key in ("TOPSTEPX_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE"):
        p = os.environ.get(key)
        if p:
            return p
    v = os.environ.get("TOPSTEPX_TLS_VERIFY")
    if v is not None:
        return str(v).strip().lower() in ("1", "true", "yes", "on")
    return True
