#!/usr/bin/env python3
import requests, json, sys, yaml
from pathlib import Path

BASE = "https://api.topstepx.com"
# Resolve config.yaml relative to this script's directory so it works
# regardless of the caller's current working directory.
CFG  = Path(__file__).resolve().parent / "config.yaml"

def load_cfg():
    if not CFG.exists():
        print("[ERR] config.yaml not found."); sys.exit(1)
    cfg = yaml.safe_load(CFG.read_text()) or {}
    tsx = cfg.get("topstepx") or {}
    for k in ("username","api_key"):
        if not tsx.get(k): print(f"[ERR] topstepx.{k} missing in config.yaml"); sys.exit(1)
    return tsx

def login_key(user, key):
    r = requests.post(f"{BASE}/api/Auth/loginKey",
                      json={"userName": user, "apiKey": key},
                      headers={"Content-Type":"application/json"},
                      timeout=15)
    if not r.ok or "application/json" not in r.headers.get("content-type","").lower():
        return None
    j = r.json()
    return j.get("token") if j.get("success") else None

def get_json(url, token, method="GET", payload=None):
    try:
        h = {"Authorization": f"Bearer {token}", "Accept":"application/json"}
        r = requests.request(method, url, headers=h, json=payload, timeout=15)
        ct = r.headers.get("content-type","").lower()
        if not r.ok or "application/json" not in ct:
            return False, f"HTTP {r.status_code} {ct} {r.text[:180]}"
        return True, r.json()
    except Exception as e:
        return False, f"exc: {e}"

def norm_accounts(obj):
    # normalize many possible shapes
    if isinstance(obj, dict) and "accounts" in obj and isinstance(obj["accounts"], list):
        arr = obj["accounts"]
    elif isinstance(obj, list):
        arr = obj
    elif isinstance(obj, dict):
        arr = [obj]
    else:
        arr = []
    out=[]
    for a in arr:
        aid = a.get("id") or a.get("accountId")
        num = a.get("number") or a.get("accountNumber") or a.get("name")
        bal = a.get("balance") or a.get("cash") or a.get("cashBalance") or a.get("balanceValue") or a.get("netLiq")
        eq  = a.get("equity")  or a.get("netLiq") or a.get("netLiquidation") or a.get("accountEquity") or a.get("balance")
        if aid or num:
            out.append({"id":aid,"number":num,"balance":bal,"equity":eq,"_raw":a})
    return out

def main():
    tsx = load_cfg()
    token = login_key(tsx["username"], tsx["api_key"])
    if not token:
        print("[ERR] loginKey failed (bad username/api_key or tenant)."); sys.exit(1)

    print("[OK] Token acquired. Probing account endpoints…")
    # Try a broad set of likely “list my accounts” routes
    probes = [
        # ProjectX/TopstepX common
        "/api/Me/accounts",
        "/api/Account",                 # often a list
        "/api/Account/list",
        "/api/Account/my",
        "/api/User/accounts",
        "/api/User/me",
        "/api/Auth/me",

        # v1 family (some tenants)
        "/v1/me/accounts",
        "/v1/accounts",
        "/v1/user/accounts",

        # search-style fallbacks (POST)
        ("POST","/api/Account/search", {}),  # sometimes returns your accounts
    ]

    found=None; src=None
    for p in probes:
        method="GET"; path=p
        payload=None
        if isinstance(p, tuple):
            method, path, payload = p
        ok, data = get_json(BASE+path, token, method, payload)
        print(f"[PROBE] {method} {path} =>", "OK" if ok else data)
        if not ok:
            continue
        accs = norm_accounts(data)
        if accs:
            found, src = accs, path
            break

    if not found:
        print("\n[ERR] No account list endpoint returned JSON on your tenant.")
        print("     Open the Swagger at your base (TopstepX -> Settings -> API -> Docs) and look for an 'Account' or 'Me' tag.")
        print("     Since TopstepX uses ProjectX, exact paths differ by tenant. (See Topstep’s API access notes.)")
        sys.exit(2)

    print(f"\n[OK] Accounts from {src}:")
    for a in found:
        print(f"  id={a['id']}  number={a['number']}  balance={a['balance']}  equity={a['equity']}")

    # Pick matching number from YAML if provided
    want = tsx.get("account_number")
    pick = None
    if want:
        for a in found:
            if str(a["number"]) == str(want):
                pick=a; break
    if not pick:
        pick = found[0]
    aid = pick["id"]
    print(f"\n[SELECT] accountId (integer) = {aid}  for number = {pick['number']}")
    print("\nPut this in config.yaml under topstepx.account_id and use it for orders & balance calls.")
    print("If you want detail-by-id, try these next (one of them should work):")
    for s in (f"/api/Account/{aid}/summary",
              f"/api/Account/{aid}/balance",
              f"/api/Account/{aid}",
              f"/v1/accounts/{aid}",
              f"/v1/accounts/{aid}/summary",
              f"/v1/accounts/{aid}/balance"):
        print("  ", BASE+s)

if __name__ == "__main__":
    main()
