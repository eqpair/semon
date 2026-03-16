"""
sector_signal.py  —  RRG 기반 소외주 탐색

계산 방식:
  1. RS          = (종목 종가 / 테마 평균 종가) × 100
  2. RS_Ratio    = 100 + (RS - RS_MA10) / RS_STD10
  3. RS_Momentum = 100 + (ROC - ROC_MA10) / ROC_STD10
     ROC = (RS_Ratio / RS_Ratio[10일전] - 1) × 100

사분면:
  improving : RS_Ratio < 100, Momentum >= 100  ← 핵심 매수 타이밍
  lagging   : RS_Ratio < 100, Momentum <  100  ← 소외 진행 중
  leading   : RS_Ratio >= 100, Momentum >= 100  ← 강하고 가속
  weakening : RS_Ratio >= 100, Momentum <  100  ← 강하지만 둔화

최적 파라미터 (백테스트): MA=10, ROC=10
"""
import logging
from config import SECTORS

logger = logging.getLogger(__name__)

MA_PERIOD  = 10
ROC_PERIOD = 10
TAIL_DAYS  = 40   # 8주 궤적

ohlcv_store:    dict[str, dict]       = {}
current_price:  dict[str, float]      = {}
rrg_history:    dict[str, list[dict]] = {}


def update_ohlcv(data: dict[str, dict | None]):
    for code, val in data.items():
        if val:
            ohlcv_store[code] = val


def update_prices(prices: dict[str, float | None]):
    for code, price in prices.items():
        if price is not None:
            current_price[code] = price


def _ma(values: list[float], period: int) -> list[float | None]:
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1:i + 1]) / period
    return result


def _std(values: list[float], period: int) -> list[float | None]:
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        window = values[i - period + 1:i + 1]
        mean   = sum(window) / period
        result[i] = (sum((x - mean) ** 2 for x in window) / period) ** 0.5
    return result


def _calc_rs_ratio(closes: list[float], benchmark: list[float]) -> list[float | None]:
    n      = min(len(closes), len(benchmark))
    raw_rs = [(closes[i] / benchmark[i] * 100) if benchmark[i] > 0 else 0.0 for i in range(n)]
    ma     = _ma(raw_rs, MA_PERIOD)
    std    = _std(raw_rs, MA_PERIOD)
    return [
        100.0 + (raw_rs[i] - ma[i]) / std[i]
        if ma[i] is not None and std[i] and std[i] > 0 else None
        for i in range(n)
    ]


def _calc_rs_momentum(rs_ratio: list[float | None]) -> list[float | None]:
    n   = len(rs_ratio)
    roc = [0.0] * n
    for i in range(ROC_PERIOD, n):
        a, b = rs_ratio[i], rs_ratio[i - ROC_PERIOD]
        if a is not None and b and b != 0:
            roc[i] = (a / b - 1) * 100
    ma  = _ma(roc, ROC_PERIOD)
    std = _std(roc, ROC_PERIOD)
    return [
        100.0 + (roc[i] - ma[i]) / std[i]
        if ma[i] is not None and std[i] and std[i] > 0 else None
        for i in range(n)
    ]


def _quadrant(ratio: float | None, mom: float | None) -> str:
    if ratio is None or mom is None:
        return "neutral"
    if ratio >= 100 and mom >= 100: return "leading"
    if ratio >= 100 and mom <  100: return "weakening"
    if ratio <  100 and mom <  100: return "lagging"
    return "improving"


def _get_return(code: str, days: int) -> float | None:
    entry = ohlcv_store.get(code)
    if not entry:
        return None
    closes = entry["closes"]
    if len(closes) < days:
        return None
    now  = current_price.get(code) or closes[-1]
    past = closes[-days]
    return (now - past) / past if past != 0 else None


def _get_vol_ratio(code: str) -> float | None:
    entry = ohlcv_store.get(code)
    if not entry:
        return None
    vols = entry["volumes"]
    if len(vols) < 2:
        return None
    avg = sum(vols[-21:-1]) / len(vols[-21:-1]) if vols[-21:-1] else 0
    return vols[-1] / avg if avg > 0 else None


def _sector_avg(code_list: list[str], days: int) -> float | None:
    vals = [v for c in code_list if (v := _get_return(c, days)) is not None]
    return sum(vals) / len(vals) if vals else None


def calc_sector_signals(sector: str, codes: list[tuple[str, str]]) -> dict:
    code_list = [c for c, _ in codes]
    name_map  = {c: n for c, n in codes}

    min_req = MA_PERIOD + ROC_PERIOD + 5
    valid   = {
        c: ohlcv_store[c]["closes"]
        for c in code_list
        if c in ohlcv_store and len(ohlcv_store[c]["closes"]) >= min_req
    }

    if len(valid) < 2:
        return _empty_sector(sector)

    min_len   = min(len(v) for v in valid.values())
    aligned   = {c: v[-min_len:] for c, v in valid.items()}
    benchmark = [sum(aligned[c][i] for c in aligned) / len(aligned) for i in range(min_len)]

    returns_5d = {c: r for c in valid if (r := _get_return(c, 5)) is not None}
    if not returns_5d:
        return _empty_sector(sector)

    sector_ret_5d  = sum(returns_5d.values()) / len(returns_5d)
    rising_ratio   = sum(1 for r in returns_5d.values() if r > 0) / len(returns_5d)
    is_rising      = sector_ret_5d > 0 and rising_ratio > 0.5
    sector_ret_20d = _sector_avg(code_list, 20)
    sector_ret_60d = _sector_avg(code_list, 60)

    candidates = []
    for code in valid:
        closes    = aligned[code]
        now_price = current_price.get(code) or closes[-1]

        rs_ratio    = _calc_rs_ratio(closes, benchmark)
        rs_momentum = _calc_rs_momentum(rs_ratio)

        curr_ratio = next((v for v in reversed(rs_ratio)    if v is not None), None)
        curr_mom   = next((v for v in reversed(rs_momentum) if v is not None), None)
        quad       = _quadrant(curr_ratio, curr_mom)

        # 궤적 누적(처음 실행 시 과거 궤적 미리 채움)
        if code not in rrg_history or len(rrg_history[code]) == 0:
            # 과거 TAIL_DAYS일치 한번에 계산
            tail = []
            for offset in range(TAIL_DAYS, 0, -5):
                if offset >= min_len:
                    continue
                past_closes    = [aligned[c][-(offset)] for c in aligned]
                past_benchmark = [sum(past_closes) / len(past_closes)]
                # 해당 시점까지의 데이터로 RS-Ratio, RS-Momentum 계산
                hist_closes    = aligned[code][:-offset] if offset > 0 else aligned[code]
                hist_benchmark = [sum(aligned[c][i] for c in aligned) / len(aligned)
                                for i in range(len(hist_closes))]
                hist_ratio    = _calc_rs_ratio(hist_closes, hist_benchmark)
                hist_momentum = _calc_rs_momentum(hist_ratio)
                t_ratio = next((v for v in reversed(hist_ratio)    if v is not None), None)
                t_mom   = next((v for v in reversed(hist_momentum) if v is not None), None)
                if t_ratio is not None and t_mom is not None:
                    tail.append({"rs_ratio": round(t_ratio, 3), "rs_momentum": round(t_mom, 3)})
            rrg_history[code] = tail

        # 현재 값 append
        if curr_ratio is not None and curr_mom is not None:
            rrg_history[code].append({
                "rs_ratio":    round(curr_ratio, 3),
                "rs_momentum": round(curr_mom,   3),
            })
            rrg_history[code] = rrg_history[code][-TAIL_DAYS:]
    
        r1  = _get_return(code, 1)
        r5  = returns_5d.get(code)
        r20 = _get_return(code, 20)
        r60 = _get_return(code, 60)
        vol = _get_vol_ratio(code)

        rs_5d  = (r5  / sector_ret_5d)  if r5  is not None and sector_ret_5d  else None
        rs_20d = (r20 / sector_ret_20d) if r20 is not None and sector_ret_20d else None
        rs_60d = (r60 / sector_ret_60d) if r60 is not None and sector_ret_60d else None

        candidates.append({
            "code":        code,
            "name":        name_map[code],
            "price":       now_price,
            "ret_1d":      round(r1  * 100, 2) if r1  is not None else None,
            "ret_5d":      round(r5  * 100, 2) if r5  is not None else None,
            "rs_5d":       round(rs_5d,  3)    if rs_5d  is not None else None,
            "rs_20d":      round(rs_20d, 3)    if rs_20d is not None else None,
            "rs_60d":      round(rs_60d, 3)    if rs_60d is not None else None,
            "vol_ratio":   round(vol,    3)    if vol    is not None else None,
            "rs_ratio":    round(curr_ratio, 3) if curr_ratio is not None else None,
            "rs_momentum": round(curr_mom,   3) if curr_mom   is not None else None,
            "quadrant":    quad,
            "tail":        rrg_history.get(code, [])[-TAIL_DAYS:],
            "signal":      _rrg_signal(quad, vol),
        })

    def _sort_key(x):
        order = {"improving": 0, "lagging": 1, "weakening": 2, "leading": 3, "neutral": 4}
        return (order.get(x["quadrant"], 4), x["rs_ratio"] if x["rs_ratio"] is not None else 999)

    candidates.sort(key=_sort_key)

    return {
        "sector":          sector,
        "sector_ret_5d":   round(sector_ret_5d * 100, 2),
        "rising_ratio":    round(rising_ratio, 2),
        "is_rising":       is_rising,
        "total":           len(candidates),
        "improving_count": sum(1 for c in candidates if c["quadrant"] == "improving"),
        "lagging_count":   sum(1 for c in candidates if c["quadrant"] == "lagging"),
        "candidates":      candidates,
    }


def _rrg_signal(quadrant: str, vol: float | None) -> str:
    if quadrant == "improving":
        return "watch" if (vol is not None and vol < 0.5) else "improving"
    return quadrant if quadrant in ("lagging", "weakening", "leading") else "neutral"


def calc_all_signals() -> dict:
    from datetime import datetime
    result = {
        "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rrg_params":  {"ma_period": MA_PERIOD, "roc_period": ROC_PERIOD},
        "sectors":     {}
    }
    for sector, codes in SECTORS.items():
        result["sectors"][sector] = calc_sector_signals(sector, codes)
    return result


def _empty_sector(sector: str) -> dict:
    return {
        "sector": sector, "sector_ret_5d": None, "rising_ratio": None,
        "is_rising": False, "total": 0, "improving_count": 0,
        "lagging_count": 0, "candidates": [],
    }