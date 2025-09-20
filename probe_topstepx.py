#!/usr/bin/env python3
# probe_topstepx.py
import json, sys, requests, yaml
from pathlib import Path

CONFIG_PATH = Path("config.yaml")
BASE = "https://api.topstepx.com"

def load_cfg():
    if not CONFIG_PATH.exists():
        print(f"[ERR] config.yaml not found next to this script.")
        sys.exit(1)
    cfg = yaml.safe_load(CONFIG_PATH.read_text()) or {}
    tsx = cfg.get("topstepx") or {}
    miss = [k for k in ("username","api_key") if not tsx.get(k)]
    if miss:
        print(f"[ERR] config.topstepx missing keys: {miss}")
        sys.exit(1)
    return tsx

def login_key(user, api_key):
    url = f"{BASE}/api/Auth/loginKey"
    try:
        r = requests.post(url, json={"userName": user, "apiKey": api_key},
                          headers={"Content-Type":"application/json"}, timeout=12)
    except Exception as e:
        return False, f"exc: {e}"
    ct = r.headers.get("content-type","").lower()
    if not r.ok:
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    if "application/json" not in ct:
        return False, f"non-JSON at {url}: {ct} {r.text[:120]}"
    try:
        j = r.json()
    except Exception as e:
        return False, f"JSON parse fail at {url}: {e}"
    if j.get("success") and j.get("token"):
        return True, j["token"]
    return False, f"loginKey returned: {j}"

def get_json(url, token):
    try:
        r = requests.get(url, headers={"Authorization": f"Bearer {token}",
                                       "Accept":"application/json"}, timeout=12)
    except Exception as e:
        return False, f"exc: {e}"
    ct = r.headers.get("content-type","").lower()
    if not r.ok:
        return False, f"HTTP {r.status_code}: {r.text[:200]}"
    if "application/json" not in ct:
        return False, f"non-JSON: {ct} {r.text[:120]}"
    try:
        return True, r.json()
    except Exception as e:
        return False, f"JSON parse fail: {e} body: {r.text[:200]}"

def normalize_accounts(obj):
    if isinstance(obj, dict) and "accounts" in obj and isinstance(obj["accounts"], list):
        arr = obj["accounts"]
    elif isinstance(obj, list):
        arr = obj
    elif isinstance(obj, dict):
        arr = [obj]
    else:
        arr = []
    out = []
    for a in arr:
        aid = a.get("id") or a.get("accountId")
        num = a.get("number") or a.get("accountNumber") or a.get("name")
        bal = a.get("balance") or a.get("cash") or a.get("cashBalance") or a.get("balanceValue") or a.get("netLiq")
        eq  = a.get("equity")  or a.get("netLiq") or a.get("netLiquidation") or a.get("accountEquity") or a.get("balance")
        out.append({"id": aid, "number": num, "balance": bal, "equity": eq, "_raw": a})
    return out

def main():
    tsx = load_cfg()
    user = tsx["username"]
    api_key = tsx["api_key"]
    print(f"[INFO] Using BASE={BASE} user={user}")

    ok, token_or_err = login_key(user, api_key)
    if not ok:
        print(f"[ERR] loginKey failed: {token_or_err}")
        print("[HINT] Double-check username/api_key in config.yaml under topstepx.")
        sys.exit(1)
    token = token_or_err
    print("[OK] Got loginKey token.")

    # Try accounts endpoints (both families)
    candidates = [
        "/api/Me/accounts",
        "/api/Account",
        "/v1/me/accounts",
        "/v1/accounts",
    ]
    found_accounts = []
    used_url = None
    for p in candidates:
        url = BASE + p
        ok, data = get_json(url, token)
        print(f"[PROBE] GET {p} => {'OK' if ok else data}")
        if not ok:
            continue
        accounts = normalize_accounts(data)
        if accounts:
            found_accounts = accounts
            used_url = p
            break

    if not found_accounts:
        print("[ERR] Could not retrieve any accounts from either /api or /v1 paths.")
        print("[NEXT] Open https://api.topstepx.com/swagger and check which Account/Me endpoints exist for your tenant.")
        sys.exit(1)

    print(f"[OK] Accounts JSON from {used_url}:")
    for a in found_accounts:
        print(f"  - id={a['id']} number={a['number']} balance={a['balance']} equity={a['equity']}")
    # Prefer match by account_number (if present in config), else first
    want_num = tsx.get("account_number")
    pick = None
    if want_num:
        for a in found_accounts:
            if str(a["number"]) == str(want_num):
                pick = a
                break
    if pick is None:
        pick = found_accounts[0]
    aid = pick["id"]
    if not isinstance(aid, int):
        try:
            aid = int(aid)
        except Exception:
            pass
    print(f"[SELECT] Using accountId={aid} (number={pick['number']})")

    # Try detail endpoints by id (both families)
    detail_paths = [
        f"/api/Account/{aid}/summary",
        f"/api/Account/{aid}/balance",
        f"/api/Account/{aid}",
        f"/v1/accounts/{aid}",
        f"/v1/accounts/{aid}/summary",
        f"/v1/accounts/{aid}/balance",
    ]
    for p in detail_paths:
        ok, data = get_json(BASE + p, token)
        print(f"[PROBE] GET {p} => {'OK' if ok else data}")
        if not ok:
            continue
        accs = normalize_accounts(data)
        if accs:
            a = accs[0]
        else:
            # single object form
            a = normalize_accounts([data])[0] if data else None
        if a:
            print(f"[OK] Detail from {p}: id={a['id']} number={a['number']} balance={a['balance']} equity={a['equity']}")
            break
    else:
        print("[WARN] No detail-by-id endpoint returned JSON, but we DID list accounts successfully above.")

if __name__ == "__main__":
    main()
