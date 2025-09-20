# get_balance.py
import os, json, requests, yaml

with open("config.yaml","r") as f:
    cfg = yaml.safe_load(f)

BASE = (cfg["topstepx"]["base_url"] or "https://api.topstepx.com").rstrip("/")
USER = cfg["topstepx"]["username"]
KEY  = cfg["topstepx"]["api_key"]
AID  = int(cfg["topstepx"]["account_id"])

# 1) loginKey -> token
r = requests.post(f"{BASE}/api/Auth/loginKey",
                  json={"userName": USER, "apiKey": KEY},
                  headers={"Content-Type":"application/json","Accept":"application/json"}, timeout=15)
r.raise_for_status()
j = r.json()
token = j.get("token")
if not (j.get("success") and token):
    raise SystemExit(f"loginKey failed: {j}")

# 2) /api/Account/search -> find your account
r = requests.post(f"{BASE}/api/Account/search",
                  headers={"Authorization": f"Bearer {token}", "Accept":"application/json"},
                  json={}, timeout=15)
r.raise_for_status()
d = r.json()
accounts = d if isinstance(d,list) else \
           d.get("accounts") or d.get("data") or d.get("items") or d.get("results") or d.get("accountList") or []
if isinstance(accounts,dict): accounts=[accounts]
hit = next((a for a in accounts if a.get("id")==AID), None) or (accounts[0] if accounts else {})
print({
  "id": hit.get("id"),
  "number": hit.get("number") or hit.get("accountNumber"),
  "balance": hit.get("balance"),
  "equity": hit.get("equity"),
  "currency": hit.get("currency") or hit.get("ccy"),
})
