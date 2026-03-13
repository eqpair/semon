"""
sector_signal.py

수익률 계산 방식:
  - 기준점(과거가): ohlcv_store 의 일봉 종가  (1시간마다 갱신)
  - 현재가        : current_price             (10분마다 갱신)
  - ret_Nd = (current_price - closes[-N]) / closes[-N]

이렇게 하면 프로그램 시작 즉시 과거 데이터를 활용할 수 있고,
장 중에는 실시간 현재가가 수익률에 반영된다.
"""
import logging
from config import SECTORS

logger = logging.getLogger(__name__)

# ── 저장소 ─────────────────────────────────────────────────────
# {code: {"closes": [float...62], "volumes": [float...62]}}
ohlcv_store: dict[str, dict] = {}

# {code: float}  — 10분마다 갱신되는 실시간 현재가
current_price: dict[str, float] = {}


# ── 업데이트 ───────────────────────────────────────────────────

def update_ohlcv(data: dict[str, dict | None]):
    """일봉 데이터 갱신 (1시간마다)"""
    for code, val in data.items():
        if val:
            ohlcv_store[code] = val


def update_prices(prices: dict[str, float | None]):
    """현재가 갱신 (10분마다)"""
    for code, price in prices.items():
        if price is not None:
            current_price[code] = price


# ── 수익률 계산 ────────────────────────────────────────────────

def _get_return(code: str, days: int) -> float | None:
    """
    N거래일 수익률
    분자: 실시간 현재가 (없으면 최신 일봉 종가 fallback)
    분모: closes[-days] — N거래일 전 종가
    """
    entry = ohlcv_store.get(code)
    if not entry:
        return None

    closes = entry["closes"]
    if len(closes) < days:
        return None

    now = current_price.get(code) or closes[-1]
    past = closes[-days]

    if past == 0:
        return None
    return (now - past) / past


def _get_vol_ratio(code: str) -> float | None:
    """당일 거래량 / 최근 20거래일 평균"""
    entry = ohlcv_store.get(code)
    if not entry:
        return None

    vols = entry["volumes"]
    if len(vols) < 2:
        return None

    today_vol = vols[-1]
    past_vols = vols[-21:-1]
    if not past_vols:
        return None

    avg = sum(past_vols) / len(past_vols)
    return (today_vol / avg) if avg > 0 else None


# ── RS 임계값 ──────────────────────────────────────────────────
RS_5D_THRESHOLD  = 0.85
RS_20D_THRESHOLD = 0.90
RS_60D_MIN       = 0.75
VOL_RATIO_MIN    = 0.70


# ── 섹터 신호 계산 ─────────────────────────────────────────────

def calc_sector_signals(sector: str, codes: list[tuple[str, str]]) -> dict:
    code_list = [c for c, _ in codes]
    name_map  = {c: n for c, n in codes}

    # 5일 수익률
    returns_5d = {}
    for code in code_list:
        r = _get_return(code, 5)
        if r is not None:
            returns_5d[code] = r

    if not returns_5d:
        logger.debug(f"[{sector}] 수익률 없음 — OHLCV 미로드")
        return _empty_sector(sector)

    # 섹터 지표
    sector_ret_5d  = sum(returns_5d.values()) / len(returns_5d)
    rising_count   = sum(1 for r in returns_5d.values() if r > 0)
    rising_ratio   = rising_count / len(returns_5d)
    is_rising      = sector_ret_5d > 0 and rising_ratio > 0.5

    sector_ret_20d = _sector_avg(code_list, 20)
    sector_ret_60d = _sector_avg(code_list, 60)

    candidates = []
    for code in code_list:
        r1  = _get_return(code, 1)
        r5  = returns_5d.get(code)
        r20 = _get_return(code, 20)
        r60 = _get_return(code, 60)
        vol = _get_vol_ratio(code)

        if r5 is None:
            continue

        rs_5d  = (r5  / sector_ret_5d)  if sector_ret_5d  else None
        rs_20d = (r20 / sector_ret_20d) if (r20 is not None and sector_ret_20d) else None
        rs_60d = (r60 / sector_ret_60d) if (r60 is not None and sector_ret_60d) else None

        # 현재가: 실시간 우선, 없으면 최신 종가
        price = current_price.get(code)
        if price is None:
            entry = ohlcv_store.get(code)
            price = entry["closes"][-1] if entry else None

        signal = _judge_signal(is_rising, rs_5d, rs_20d, rs_60d, vol)

        candidates.append({
            "code":      code,
            "name":      name_map[code],
            "price":     price,
            "ret_1d":    round(r1 * 100, 2) if r1 is not None else None,
            "ret_5d":    round(r5 * 100, 2),
            "rs_5d":     round(rs_5d,  3) if rs_5d  is not None else None,
            "rs_20d":    round(rs_20d, 3) if rs_20d is not None else None,
            "rs_60d":    round(rs_60d, 3) if rs_60d is not None else None,
            "vol_ratio": round(vol,    3) if vol    is not None else None,
            "signal":    signal,
        })

    candidates.sort(key=lambda x: x["rs_5d"] if x["rs_5d"] is not None else 999)

    return {
        "sector":        sector,
        "sector_ret_5d": round(sector_ret_5d * 100, 2),
        "rising_ratio":  round(rising_ratio, 2),
        "is_rising":     is_rising,
        "total":         len(candidates),
        "lagging_count": sum(1 for c in candidates if c["signal"] == "lagging"),
        "candidates":    candidates,
    }


def _sector_avg(code_list: list[str], days: int) -> float | None:
    vals = [_get_return(c, days) for c in code_list]
    vals = [v for v in vals if v is not None]
    return sum(vals) / len(vals) if vals else None


def _judge_signal(
    is_rising: bool,
    rs_5d:  float | None,
    rs_20d: float | None,
    rs_60d: float | None,
    vol:    float | None,
) -> str:
    if not is_rising or rs_5d is None:
        return "neutral"

    cond_5d  = rs_5d  < RS_5D_THRESHOLD
    cond_20d = rs_20d is not None and rs_20d < RS_20D_THRESHOLD
    cond_60d = rs_60d is None or rs_60d > RS_60D_MIN
    cond_vol = vol    is None or vol    >= VOL_RATIO_MIN

    if cond_5d and cond_20d and cond_60d and cond_vol:
        return "lagging"
    if cond_5d and cond_60d:
        return "watch"
    return "neutral"


def calc_all_signals() -> dict:
    from datetime import datetime
    result = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sectors": {}
    }
    for sector, codes in SECTORS.items():
        result["sectors"][sector] = calc_sector_signals(sector, codes)
    return result


def _empty_sector(sector: str) -> dict:
    return {
        "sector":        sector,
        "sector_ret_5d": None,
        "rising_ratio":  None,
        "is_rising":     False,
        "total":         0,
        "lagging_count": 0,
        "candidates":    [],
    }
