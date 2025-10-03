import json
import os
import sys
import urllib3
import logging
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from quart import Quart


# Ensure local backend package is importable when running as a script
BACKEND_ROOT = Path(__file__).resolve().parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - dependency optional for GUI flow
    yaml = None

from topstepx_bot.utils import snap_to_tick, fmt_num, iso_utc_z
from topstepx_bot.indicators import ATR as _ATR, compute_indicators as _compute_indicators
from topstepx_bot.api import (
    get_token as _api_get_token,
    api_post as _api_post,
    cancel_order as _api_cancel_order,
    get_account_info as _api_get_account_info,
)
from topstepx_bot.market import (
    load_contracts as market_load_contracts,
    warmup_bars as market_warmup_bars,
    seed_streamer_from_warmup as market_seed_streamer_from_warmup,
)
from topstepx_bot.monitors import (
    make_monitor_oco_orders,
    make_monitor_account_snapshot,
    make_monitor_synth_trailing,
    make_get_open_orders_count,
)
from topstepx_bot.risk import (
    risk_per_point as risk_per_point_module,
    get_risk_dollars as get_risk_dollars_module,
)
from topstepx_bot.server import create_app
from topstepx_bot.streamer import MarketStreamer as _MarketStreamer


def _try_parse_config(blob: str) -> Optional[Dict[str, Any]]:
    if not blob:
        return None
    if yaml is not None:
        try:
            cfg = yaml.safe_load(blob)
            if isinstance(cfg, dict):
                return cfg
        except Exception:
            logging.exception("Failed to parse config with yaml; will try JSON")
    try:
        cfg = json.loads(blob)
        if isinstance(cfg, dict):
            return cfg
    except Exception:
        logging.exception("Failed to parse config JSON override")
    return None


def load_config() -> Dict[str, Any]:
    inline_json = os.environ.get("TOPSTEPX_CONFIG_JSON")
    cfg = _try_parse_config(inline_json) if inline_json else None
    if cfg:
        return cfg
    inline = os.environ.get("TOPSTEPX_CONFIG")
    cfg = _try_parse_config(inline) if inline else None
    if cfg:
        return cfg
    cfg_path_env = os.environ.get("TOPSTEPX_CONFIG_PATH")
    cfg_path = Path(cfg_path_env).expanduser() if cfg_path_env else (BACKEND_ROOT / "config.yaml")
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to load config.yaml from disk. Install pyyaml or provide TOPSTEPX_CONFIG_JSON."
        )
    with cfg_path.open() as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise RuntimeError(f"Configuration at {cfg_path} is not a mapping")
    return data


# --- Load config ---
config = load_config()


# Connection- and market-level configuration derived from YAML
API_URL = config.get("api_base") or config.get("API_URL") or "https://api.topstepx.com"
AUTH = config.get("auth", {}) or {}
USERNAME = config.get("username") or AUTH.get("username") or AUTH.get("email") or ""
API_KEY = config.get("api_key") or AUTH.get("api_key") or ""
ACCOUNT_ID = int(config.get("account_id") or (config.get("account") or {}).get("id") or 0)
SYMBOL = (config.get("symbol") or "MNQ").upper()
MARKET_HUB = config.get("market_hub") or "https://rtc.topstepx.com/hubs/market"
LIVE_FLAG = bool((config.get("market") or {}).get("live", True))
INCLUDE_PARTIAL = bool((config.get("market") or {}).get("includePartialBar", True))
BOOTSTRAP_HISTORY_HOURS = int(config.get("bootstrap_history_hours", 3) or 3)


app = Quart(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global state
# Shared mutable state the streamer + monitors mutate
oco_orders: Dict[Any, List[Any]] = {}
contract_map: Dict[str, Dict[str, Any]] = {}
bars_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
last_price: Dict[str, float] = {}
_bars_lock = threading.Lock()
indicator_state: Dict[str, Dict[str, Optional[float]]] = {}
active_entries: Dict[Any, Dict[str, Any]] = {}
account_snapshot: Dict[str, Optional[float]] = {"balance": None, "equity": None}
streamers: Dict[str, Any] = {}

# Strategy params
DEBUG = bool(config.get("debug", False))
# Strategy tuning knobs (mirrors TradingView strategy inputs)
STRAT = (config.get("strategy") or {})
EMA_SHORT = int(STRAT.get("emaShort", 9))
EMA_LONG = int(STRAT.get("emaLong", 21))
EMA_SOURCE = str(STRAT.get("emaSource", "close")).strip().lower()
RTH_ONLY = bool(STRAT.get("rthOnly", False))
USE_VWAP = bool(STRAT.get("vwapEnabled", False))
ATR_LENGTH = int(STRAT.get("atrLength", 14))
RISK_PER_TRADE = float(STRAT.get("riskPerTrade", 500))
CONTRACT_SIZE_MAX = int(STRAT.get("contractSizeMax", 10))
INSTR = (config.get("instrument") or {})
RISK_PER_POINT_CONFIG = float(INSTR.get("riskPerPoint", 0) or 0)
TRAILING_STOP_ENABLED = bool(STRAT.get("trailingStopEnabled", False))
LONG_ONLY = bool(STRAT.get("longOnly", True))
ATR_TRAIL_K_LONG = float(STRAT.get("atrTrailKLong", STRAT.get("atrTrailK", 2.0)))
ATR_TRAIL_K_SHORT = float(STRAT.get("atrTrailKShort", STRAT.get("atrTrailK", 2.0)))
SHORT_SIZE_FACTOR = float(STRAT.get("shortSizeFactor", 0.75))
RISK_BUDGET_FRACTION = float(STRAT.get("riskBudgetFraction", 0.0))
TRADE_COOLDOWN_SEC = int((config.get("risk") or {}).get("trade_cooldown_sec", 10))
ORDER_SIZE = int((config.get("trade") or {}).get("order_size", 1))
PAD_TICKS = int(STRAT.get("padTicks", 0))
TRADE_CFG = (config.get("trade") or {})
FIXED_TP_POINTS = float(TRADE_CFG.get("tpPoints", 0) or 0)
FIXED_SL_POINTS = float(TRADE_CFG.get("slPoints", 0) or 0)
USE_FIXED_TARGETS = bool(TRADE_CFG.get("useFixedTargets", False))
AUTO_OCO_ENABLED = bool(STRAT.get("autoOCOBrackets", STRAT.get("autoOCOEnabled", False)))
POSITION_BRACKETS_ENABLED = bool(STRAT.get("positionBrackets", False))
def _to_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return str(v).strip().lower() in ("1","true","yes","on","y")
    return bool(v)

def _bool_any(d: Dict[str, any], keys, default=False) -> bool:
    for k in keys:
        if k in d and d.get(k) is not None:
            return _to_bool(d.get(k))
    return bool(default)

# Accept multiple aliases for this switch so config can use crossup/crossUp/forceCrossUp
FORCE_CROSS_UP = _bool_any(STRAT, ["forceCrossUp", "crossUp", "crossup", "force_cross_up"], False)
SYNTH_TRAILING_ENABLED = bool(STRAT.get("syntheticTrailingEnabled", True))
SYNTH_TRAIL_MIN_TICKS = int(STRAT.get("syntheticTrailMinTicks", 1))
SYNTH_TRAIL_POLL_SEC = float((config.get("runtime") or {}).get("synth_trail_poll_sec", 0.5))
FORCE_NATIVE_TRAIL = bool(STRAT.get("forceNativeTrailing", False))
FORCE_FIXED_TRAIL_TICKS = bool(STRAT.get("forceFixedTrailTicks", False))

ATR = _ATR

# Synthetic warmup sizing (after EMA_LONG is defined)
SYNTH_WARMUP_MINUTES = int(config.get("synthetic_warmup_minutes", 0) or 0)
if SYNTH_WARMUP_MINUTES <= 0:
    try:
        SYNTH_WARMUP_MINUTES = max(int(EMA_LONG), int(BOOTSTRAP_HISTORY_HOURS) * 60)
    except Exception:
        SYNTH_WARMUP_MINUTES = int(EMA_LONG)

# Optional guard: require N real finalized bars before trading
MIN_REAL_BARS_BEFORE_TRADING = int(STRAT.get("minRealBarsBeforeTrading", 2) or 0)


# API wrapper helpers keep Quart routes decoupled from direct SDK calls
def get_token():
    return _api_get_token(API_URL, USERNAME, API_KEY)


def api_post(token, endpoint, payload):
    return _api_post(API_URL, token, endpoint, payload)


def cancel_order(token, account_id, order_id):
    return _api_cancel_order(API_URL, token, account_id, order_id)


def get_account_info(token):
    return _api_get_account_info(token)


# Market helpers centralise historical warmup + stream seeding
def load_contracts():
    return market_load_contracts(get_token, contract_map, config, SYMBOL)


def seed_streamer_from_warmup(ms: Any) -> None:
    return market_seed_streamer_from_warmup(ms, bars_by_symbol, _bars_lock, indicator_state)


def warmup_bars(symbol: str, contract_id: Any, days: int = 1, unit: int = 2, unit_n: int = 1, live: bool = False):
    token = get_token()
    if not token:
        logging.warning("Warmup failed: no token")
        return
    # Align history requests to configured session timezone for indicator correctness
    tzname = config.get("tz") or "America/New_York"
    hours = BOOTSTRAP_HISTORY_HOURS if BOOTSTRAP_HISTORY_HOURS and BOOTSTRAP_HISTORY_HOURS > 0 else None
    return market_warmup_bars(
        lambda endpoint, payload: api_post(token, endpoint, payload),
        token,
        symbol,
        contract_id,
        days,
        unit,
        unit_n,
        live,
        bars_by_symbol,
        _bars_lock,
        indicator_state,
        ATR_LENGTH,
        EMA_SHORT,
        EMA_LONG,
        EMA_SOURCE,
        RTH_ONLY,
        tzname,
        hours,
        INCLUDE_PARTIAL,
    )


# Risk/helpers provide per-symbol sizing + account guardrails
def risk_per_point(symbol: str, contract_id: Any) -> float:
    return risk_per_point_module(contract_map, RISK_PER_POINT_CONFIG, symbol, contract_id)


def get_risk_dollars() -> float:
    return get_risk_dollars_module(get_token, get_account_info, RISK_BUDGET_FRACTION, RISK_PER_TRADE)


def compute_indicators(df):
    tzname = config.get("tz") or "America/New_York"
    return _compute_indicators(df, EMA_SHORT, EMA_LONG, EMA_SOURCE, RTH_ONLY, tzname)


def run_server():
    # Compose a dependency container the Quart app + background monitors consume
    ctx: Dict[str, Any] = {
        'get_token': get_token,
        'api_post': api_post,
        'get_account_info': get_account_info,
        'cancel_order': cancel_order,
        'contract_map': contract_map,
        'bars_by_symbol': bars_by_symbol,
        'bars_lock': _bars_lock,
        'indicator_state': indicator_state,
        'last_price': last_price,
        'oco_orders': oco_orders,
        'active_entries': active_entries,
        'account_snapshot': account_snapshot,
        'streamers': streamers,
        'SYMBOL': SYMBOL,
        'ACCOUNT_ID': ACCOUNT_ID,
        'LIVE_FLAG': LIVE_FLAG,
        'BOOTSTRAP_HISTORY_HOURS': BOOTSTRAP_HISTORY_HOURS,
        'DEBUG': DEBUG,
        'snap_to_tick': snap_to_tick,
        'fmt_num': fmt_num,
        'load_contracts': load_contracts,
        'warmup_bars': warmup_bars,
        'seed_streamer_from_warmup': seed_streamer_from_warmup,
        'EMA_SHORT': EMA_SHORT,
        'EMA_LONG': EMA_LONG,
        'USE_VWAP': USE_VWAP,
        'REQUIRE_PRICE_ABOVE_EMAS': bool(STRAT.get("requirePriceAboveEMAS", False)),
        'CONFIRM_BARS': int(STRAT.get("confirmBars", 0)),
        'STRICT_CROSS_ONLY': bool(STRAT.get("strictCrossOnly", False)),
        'INTRABAR_CROSS': bool(STRAT.get("intrabarCross", False)),
        'TRADE_COOLDOWN_SEC': TRADE_COOLDOWN_SEC,
        'TRAILING_STOP_ENABLED': TRAILING_STOP_ENABLED,
        'AUTO_OCO_ENABLED': AUTO_OCO_ENABLED,
        'POSITION_BRACKETS_ENABLED': POSITION_BRACKETS_ENABLED,
        'FORCE_CROSS_UP': FORCE_CROSS_UP,
        'SYNTH_TRAILING_ENABLED': SYNTH_TRAILING_ENABLED,
        'SYNTH_TRAIL_MIN_TICKS': int(max(1, SYNTH_TRAIL_MIN_TICKS)),
        'SYNTH_TRAIL_POLL_SEC': max(0.25, float(SYNTH_TRAIL_POLL_SEC)),
        'FORCE_NATIVE_TRAIL': FORCE_NATIVE_TRAIL,
        # fixed native trailing distance in ticks (type 5 orders)
        # Allow dynamic trailing distance (ATR based) unless explicitly provided in config
        'TRAIL_TICKS_FIXED': (int(STRAT.get('trailDistanceTicks')) if (STRAT.get('trailDistanceTicks') not in (None, '', False)) else None),
        'FORCE_FIXED_TRAIL_TICKS': bool(FORCE_FIXED_TRAIL_TICKS),
        'PAD_TICKS': PAD_TICKS,
        'FIXED_TP_POINTS': FIXED_TP_POINTS,
        'FIXED_SL_POINTS': FIXED_SL_POINTS,
        'USE_FIXED_TARGETS': USE_FIXED_TARGETS,
        'SHORT_SIZE_FACTOR': SHORT_SIZE_FACTOR,
        'CONTRACT_SIZE_MAX': CONTRACT_SIZE_MAX,
        'LONG_ONLY': LONG_ONLY,
        'ATR_TRAIL_K_LONG': ATR_TRAIL_K_LONG,
        'ATR_TRAIL_K_SHORT': ATR_TRAIL_K_SHORT,
        # Activation offsets for trailing (TradingView-like): start trailing only after price moves
        # favorably by offset_k * ATR. Default to 0.5 if specified in config.
        'TRAIL_OFFSET_K_LONG': float(STRAT.get('atrTrailOffsetKLong', STRAT.get('atrTrailOffsetK', 0.0))),
        'TRAIL_OFFSET_K_SHORT': float(STRAT.get('atrTrailOffsetKShort', STRAT.get('atrTrailOffsetK', 0.0))),
        'ATR_LENGTH': ATR_LENGTH,
        'MARKET_HUB': MARKET_HUB,
        'risk_per_point': risk_per_point,
        'get_risk_dollars': get_risk_dollars,
        'compute_indicators': compute_indicators,
        # bracket entry support
        'USE_BRACKETS_PAYLOAD': bool((config.get('trade') or {}).get('useBracketsPayload', True)),
        # warmup behavior
        'ALLOW_SYNTH_WARMUP': True,
        'SYNTH_WARMUP_MINUTES': int(min(300, max(0, SYNTH_WARMUP_MINUTES))),
        'MIN_REAL_BARS_BEFORE_TRADING': int(max(0, MIN_REAL_BARS_BEFORE_TRADING)),
        # safety gates
        'TRADING_DISABLED': False,
        'TRADING_DISABLED_REASON': None,
        # last failed order diagnostic blob
        'LAST_ORDER_FAIL': None,
        # allow auto re-enable when account reopens
        'AUTO_REENABLE_TRADING_ON_OPEN': True,
        # capability toggle: disable native trailing; use synthetic trailing only
        'NATIVE_TRAIL_SUPPORTED': False,
        # frontend metadata for the UI
        'USERNAME': USERNAME,
        'API_KEY': API_KEY,
        'FRONTEND_ROOT': (Path(__file__).resolve().parent.parent / "Frontend").resolve(),
        'FRONTEND_INDEX': 'index.html',
    }
    try:
        logging.info(
            "Strategy switches | forceCrossUp=%s longOnly=%s nativeTrailOnly=%s",
            str(FORCE_CROSS_UP), str(LONG_ONLY), str(bool(STRAT.get('onlyNativeTrailing', False)))
        )
    except Exception:
        pass
    # Instantiate monitor coroutines that manage exits, risk snapshots, and trailing logic
    ctx['monitor_oco_orders'] = make_monitor_oco_orders(ctx)
    ctx['monitor_account_snapshot'] = make_monitor_account_snapshot(ctx)
    ctx['monitor_synth_trailing'] = make_monitor_synth_trailing(ctx)
    ctx['get_open_orders_count'] = make_get_open_orders_count(ctx)
    ctx['MarketStreamer'] = lambda symbol, contract_id, unit=2, unit_n=1: _MarketStreamer(ctx, symbol, contract_id, unit, unit_n)
    global app
    app = create_app(ctx)
    app.run(port=5000)


if __name__ == "__main__":
    run_server()
