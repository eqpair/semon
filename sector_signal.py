import logging
from collections import deque
from config import SECTORS

logger = logging.getLogger(__name__)

# 각 종목별 가격 이력 저장 (최대 60일치 = 60 * 6회/시간 * 6.5시간 = 약 2340개)
# 실제로는 장 중 10분 간격이므로 하루 약 39개, 60일 = 2340개
MAX_HISTORY = 2340

# {code: deque([price1, price2, ...])}
price_history: dict[str, deque] = {}


def update_prices(prices: dict[str, float | None]):
    """새로 fetch한 현재가를 이력에 추가"""
    for code, price in prices.items():
        if price is None:
            continue
        if code not in price_history:
            price_history[code] = deque(maxlen=MAX_HISTORY)
        price_history[code].append(price)


def get_return(code: str, period: int) -> float | None:
    """
    N개 전 대비 현재 수익률 반환
    period: 이력 개수 기준 (5일 = 5 * 39 = 195개)
    """
    hist = price_history.get(code)
    if not hist or len(hist) < period + 1:
        return None
    past = hist[-period - 1]
    current = hist[-1]
    if past == 0:
        return None
    return (current - past) / past


# 종목별 거래량 이력 저장 {code: [vol_20일전, ..., vol_오늘]}
volume_history: dict[str, list[float]] = {}


def update_volumes(volumes: dict[str, list[float] | None]):
    """거래량 이력 업데이트 (1시간마다 갱신)"""
    for code, vols in volumes.items():
        if vols:
            volume_history[code] = vols


def get_vol_ratio(code: str) -> float | None:
    """
    당일 거래량 / 20일 평균 거래량
    1.0 이상이면 평균 수준, 0.7 미만이면 거래 부진
    """
    vols = volume_history.get(code)
    if not vols or len(vols) < 2:
        return None

    today_vol = vols[-1]
    past_vols = vols[:-1]
    avg_vol = sum(past_vols) / len(past_vols)

    if avg_vol == 0:
        return None

    return today_vol / avg_vol


# 기간별 이력 개수 (10분 간격, 하루 39회 기준)
PERIOD_MAP = {
    "5d":  195,   # 5일
    "20d": 780,   # 20일
    "60d": 2340,  # 60일
}

# RS 판정 기준값
RS_5D_THRESHOLD  = 0.85
RS_20D_THRESHOLD = 0.90
RS_60D_MIN       = 0.75
VOL_RATIO_MIN    = 0.70


def calc_sector_signals(sector: str, codes: list[tuple[str, str]]) -> dict:
    """
    섹터 하나에 대한 신호 계산
    반환: {
        sector_ret_5d, rising_ratio, is_rising,
        candidates: [{code, name, rs_5d, rs_20d, rs_60d, vol_ratio, signal}]
    }
    """
    code_list = [c for c, _ in codes]
    name_map  = {c: n for c, n in codes}

    # 각 종목의 5일 수익률 계산
    returns_5d = {}
    for code in code_list:
        r = get_return(code, PERIOD_MAP["5d"])
        if r is not None:
            returns_5d[code] = r

    if not returns_5d:
        return _empty_sector(sector)

    # 섹터 조건 계산
    sector_ret_5d = sum(returns_5d.values()) / len(returns_5d)
    rising_count  = sum(1 for r in returns_5d.values() if r > 0)
    rising_ratio  = rising_count / len(returns_5d)
    is_rising     = sector_ret_5d > 0 and rising_ratio > 0.5

    candidates = []

    for code in code_list:
        r5  = returns_5d.get(code)
        r20 = get_return(code, PERIOD_MAP["20d"])
        r60 = get_return(code, PERIOD_MAP["60d"])
        vol = get_vol_ratio(code)

        if r5 is None:
            continue

        # RS 계산 (섹터 평균 대비)
        rs_5d  = (r5  / sector_ret_5d) if sector_ret_5d != 0 else None
        rs_20d = None
        rs_60d = None

        if r20 is not None:
            sector_ret_20d = _sector_avg_return(code_list, "20d")
            rs_20d = (r20 / sector_ret_20d) if sector_ret_20d else None

        if r60 is not None:
            sector_ret_60d = _sector_avg_return(code_list, "60d")
            rs_60d = (r60 / sector_ret_60d) if sector_ret_60d else None

        # 소외주 판정
        signal = _judge_signal(
            is_rising, rs_5d, rs_20d, rs_60d, vol
        )

        candidates.append({
            "code":      code,
            "name":      name_map[code],
            "price":     price_history[code][-1] if code in price_history and price_history[code] else None,
            "ret_5d":    round(r5 * 100, 2) if r5 is not None else None,
            "rs_5d":     round(rs_5d,  3) if rs_5d  is not None else None,
            "rs_20d":    round(rs_20d, 3) if rs_20d is not None else None,
            "rs_60d":    round(rs_60d, 3) if rs_60d is not None else None,
            "vol_ratio": round(vol,    3) if vol    is not None else None,
            "signal":    signal,
        })

    # 소외주 후보를 RS 5일 기준 오름차순 정렬 (가장 소외된 순)
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


def _sector_avg_return(code_list: list[str], period_key: str) -> float | None:
    """섹터 평균 수익률"""
    returns = []
    for code in code_list:
        r = get_return(code, PERIOD_MAP[period_key])
        if r is not None:
            returns.append(r)
    if not returns:
        return None
    return sum(returns) / len(returns)


def _judge_signal(
    is_rising: bool,
    rs_5d:  float | None,
    rs_20d: float | None,
    rs_60d: float | None,
    vol:    float | None,
) -> str:
    """
    소외주 판정
    - lagging : 매수 후보
    - watch   : 조건 일부 충족, 관찰 필요
    - neutral : 해당 없음
    """
    if not is_rising:
        return "neutral"

    if rs_5d is None:
        return "neutral"

    # 핵심 조건 모두 충족
    cond_5d  = rs_5d  < RS_5D_THRESHOLD
    cond_20d = rs_20d is not None and rs_20d < RS_20D_THRESHOLD
    cond_60d = rs_60d is None or rs_60d > RS_60D_MIN   # 60일 데이터 없으면 패스
    cond_vol = vol    is None or vol    >= VOL_RATIO_MIN  # 거래량 데이터 없으면 패스

    if cond_5d and cond_20d and cond_60d and cond_vol:
        return "lagging"

    # 5일만 소외 — 관찰 대상
    if cond_5d and cond_60d:
        return "watch"

    return "neutral"


def calc_all_signals() -> dict:
    """전체 섹터 신호 계산"""
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
