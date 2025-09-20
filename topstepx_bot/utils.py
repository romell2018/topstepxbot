import datetime as dt

def snap_to_tick(price: float, tick_size: float, decimals: int) -> float:
    try:
        if tick_size and tick_size > 0:
            ticks = round(price / tick_size)
            return round(ticks * tick_size, int(decimals))
    except Exception:
        pass
    return float(round(price, 4))


def fmt_num(x) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "NA"


def iso_utc_z(ts: dt.datetime) -> str:
    ts_utc = ts.astimezone(dt.timezone.utc).replace(microsecond=0, tzinfo=None)
    return ts_utc.isoformat() + "Z"

