from typing import Any, Dict


def risk_per_point(contract_map: Dict[str, dict], risk_per_point_config: float,
                   symbol: str, contract_id: Any) -> float:
    if risk_per_point_config and risk_per_point_config > 0:
        return float(risk_per_point_config)
    c = contract_map.get(symbol)
    if not c:
        for v in contract_map.values():
            if v.get("contractId") == contract_id:
                c = v
                break
    try:
        if c and c.get("tickValue") and c.get("tickSize"):
            tick_value = float(c["tickValue"])
            tick_size = float(c["tickSize"])
            if tick_size > 0:
                return tick_value / tick_size
    except Exception:
        pass
    return 5.0


def get_risk_dollars(get_token_fn, get_account_info_fn,
                     risk_budget_fraction: float, risk_per_trade: float) -> float:
    if risk_budget_fraction and risk_budget_fraction > 0:
        token = get_token_fn()
        if not token:
            return risk_per_trade
        acct = get_account_info_fn(token)
        try:
            balance = float(acct.get("balance"))
            maximum_loss = float(acct.get("maximumLoss"))
            buffer = max(0.0, balance - maximum_loss)
            return max(0.0, buffer * float(risk_budget_fraction))
        except Exception:
            return risk_per_trade
    return risk_per_trade

