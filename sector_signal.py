"""
sector_signal.py  —  RRG 기반 소외주 탐색

계산 방식 (JdK RS-Ratio / RS-Momentum):
  1. raw_rs       = 종목 종가 / 섹터 평균 종가 × 100
  2. RS_Ratio     = EMA(raw_rs, 10) / EMA(raw_rs, 65) × 100
  3. RS_Momentum  = EMA(RS_Ratio, 10) / EMA(RS_Ratio, 65) × 100

사분면:
  improving : RS_Ratio < 100, Momentum >= 100  ← 핵심 매수 타이밍
  lagging   : RS_Ratio < 100, Momentum <  100  ← 소외 진행 중
  leading   : RS_Ratio >= 100, Momentum >= 100  ← 강하고 가속
  weakening : RS_Ratio >= 100, Momentum <  100  ← 강하지만 둔화

EMA 파라미터: short=10, long=65 (JdK 원본과 동일)
최소 데이터: 65일치 이상 필요
"""
import logging
from config import SECTORS

logger = logging.getLogger(__name__)

EMA_SHORT  = 10
EMA_LONG   = 65
TAIL_DAYS  = 16   # 8주 궤적 (주 2포인트 기준)
CHART_DAYS = 120  # 주가 차트용 6개월

ohlcv_store:        dict[str, dict]       = {}
current_price:      dict[str, float]      = {}
rrg_history:        dict[str, list[dict]] = {}
sector_rrg_history: dict[str, list[dict]] = {}  # 섹터 궤적


def update_ohlcv(data: dict[str, dict | None]):
    for code, val in data.items():
        if val:
            ohlcv_store[code] = val


def update_prices(prices: dict[str, float | None]):
    for code, price in prices.items():
        if price is not None:
            current_price[code] = price


# ── EMA ──────────────────────────────────────────────────────

def _ema(values: list[float], period: int) -> list[float]:
    """
    Wilder 방식이 아닌 표준 EMA
    k = 2 / (period + 1)
    초기값: 첫 period개의 단순평균
    """
    result = [0.0] * len(values)
    if len(values) < period:
        return result

    k = 2.0 / (period + 1)
    # 초기 SMA
    sma = sum(values[:period]) / period
    result[period - 1] = sma

    for i in range(period, len(values)):
        result[i] = values[i] * k + result[i - 1] * (1 - k)

    return result


# ── JdK RS-Ratio / RS-Momentum ───────────────────────────────

def _calc_rs_ratio(closes: list[float], benchmark: list[float]) -> list[float]:
    """
    RS_Ratio = EMA(raw_rs, 10) / EMA(raw_rs, 65) × 100
    65일 미만 구간은 0.0 반환
    """
    n      = min(len(closes), len(benchmark))
    raw_rs = [
        closes[i] / benchmark[i] * 100.0 if benchmark[i] > 0 else 100.0
        for i in range(n)
    ]
    ema_s = _ema(raw_rs, EMA_SHORT)
    ema_l = _ema(raw_rs, EMA_LONG)

    result = [0.0] * n
    for i in range(EMA_LONG - 1, n):
        result[i] = ema_s[i] / ema_l[i] * 100.0 if ema_l[i] != 0 else 100.0
    return result


def _calc_rs_momentum(rs_ratio: list[float]) -> list[float]:
    """
    RS_Momentum = EMA(RS_Ratio, 10) / EMA(RS_Ratio, 65) × 100
    """
    ema_s = _ema(rs_ratio, EMA_SHORT)
    ema_l = _ema(rs_ratio, EMA_LONG)

    n      = len(rs_ratio)
    result = [0.0] * n
    for i in range(EMA_LONG - 1, n):
        result[i] = ema_s[i] / ema_l[i] * 100.0 if ema_l[i] != 0 else 100.0
    return result


# ── 유틸 ─────────────────────────────────────────────────────

def _quadrant(ratio: float, mom: float) -> str:
    if ratio == 0.0 or mom == 0.0:
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

    if code in current_price and current_price[code] is not None:
        hist_closes = closes[:-1]
        now = current_price[code]
    else:
        hist_closes = closes
        now = closes[-1]

    if len(hist_closes) < days:
        return None

    past = hist_closes[-days]
    return (now - past) / past if past != 0 else None


def _get_vol_ratio(code: str) -> float | None:
    entry = ohlcv_store.get(code)
    if not entry:
        return None
    vols = entry["volumes"]
    if len(vols) < 2:
        return None
    avg = sum(vols[-22:-2]) / len(vols[-22:-2]) if vols[-22:-2] else 0
    return vols[-2] / avg if avg > 0 else None


def _sector_avg(code_list: list[str], days: int) -> float | None:
    vals = [v for c in code_list if (v := _get_return(c, days)) is not None]
    return sum(vals) / len(vals) if vals else None


def _get_closes_chart(code: str) -> list[float]:
    entry = ohlcv_store.get(code)
    if not entry:
        return []
    closes = entry["closes"]
    if code in current_price and current_price[code] is not None:
        hist = closes[:-1][-CHART_DAYS:]
        return [round(v, 0) for v in hist] + [round(current_price[code], 0)]
    return [round(v, 0) for v in closes[-CHART_DAYS:]]


# ── 섹터 신호 계산 ────────────────────────────────────────────

def calc_sector_signals(sector: str, codes: list[tuple[str, str]]) -> dict:
    code_list = [c for c, _ in codes]
    name_map  = {c: n for c, n in codes}

    # JdK는 EMA_LONG(65)일치가 필요
    min_req = EMA_LONG + EMA_SHORT + 5  # 80일
    valid = {
        c: (
            ohlcv_store[c]["closes"][:-1]
            if c in current_price and current_price[c] is not None
            else ohlcv_store[c]["closes"]
        )
        for c in code_list
        if c in ohlcv_store and len(ohlcv_store[c]["closes"]) >= min_req
    }

    if len(valid) < 2:
        return _empty_sector(sector)

    min_len   = min(len(v) for v in valid.values())
    aligned   = {c: v[-min_len:] for c, v in valid.items()}
    benchmark = [
        sum(aligned[c][i] for c in aligned) / len(aligned)
        for i in range(min_len)
    ]

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

        curr_ratio = rs_ratio[-1]
        curr_mom   = rs_momentum[-1]
        quad       = _quadrant(curr_ratio, curr_mom)

        # ── 궤적 누적 ──────────────────────────────────────────
        # 초기화: 과거 데이터로 tail 채우기 (5일 간격)
        if code not in rrg_history or len(rrg_history[code]) == 0:
            tail = []
            for offset in range(TAIL_DAYS * 5, 0, -5):
                if offset >= min_len - EMA_LONG:
                    continue
                h_closes    = aligned[code][:-offset]
                h_benchmark = [
                    sum(aligned[c][i] for c in aligned) / len(aligned)
                    for i in range(len(h_closes))
                ]
                h_ratio = _calc_rs_ratio(h_closes, h_benchmark)
                h_mom   = _calc_rs_momentum(h_ratio)
                t_r = h_ratio[-1]
                t_m = h_mom[-1]
                if t_r != 0.0 and t_m != 0.0:
                    tail.append({"rs_ratio": round(t_r, 3), "rs_momentum": round(t_m, 3)})
            rrg_history[code] = tail

        if curr_ratio != 0.0 and curr_mom != 0.0:
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

        closes_chart = _get_closes_chart(code)

        candidates.append({
            "code":         code,
            "name":         name_map[code],
            "price":        now_price,
            "ret_1d":       round(r1  * 100, 2) if r1  is not None else None,
            "ret_5d":       round(r5  * 100, 2) if r5  is not None else None,
            "rs_5d":        round(rs_5d,  3)    if rs_5d  is not None else None,
            "rs_20d":       round(rs_20d, 3)    if rs_20d is not None else None,
            "rs_60d":       round(rs_60d, 3)    if rs_60d is not None else None,
            "vol_ratio":    round(vol,    3)    if vol    is not None else None,
            "rs_ratio":     round(curr_ratio, 3),
            "rs_momentum":  round(curr_mom,   3),
            "quadrant":     quad,
            "tail":         rrg_history.get(code, [])[-TAIL_DAYS:],
            "signal":       _rrg_signal(quad, vol),
            "closes_chart": closes_chart,
        })

    def _sort_key(x):
        order = {"improving": 0, "lagging": 1, "weakening": 2, "leading": 3, "neutral": 4}
        return (order.get(x["quadrant"], 4), x["rs_ratio"])

    candidates.sort(key=_sort_key)

    return {
        "sector":          sector,
        "sector_ret_5d":   round(sector_ret_5d * 100, 2),
        "rising_ratio":    round(rising_ratio, 2),
        "is_rising":       is_rising,
        "total":           len(candidates),
        "improving_count": sum(1 for c in candidates if c["signal"] == "improving"),
        "watch_count":     sum(1 for c in candidates if c["signal"] == "watch"),
        "lagging_count":   sum(1 for c in candidates if c["quadrant"] == "lagging"),
        "candidates":      candidates,
    }


def _rrg_signal(quadrant: str, vol: float | None) -> str:
    if quadrant == "improving":
        return "watch" if (vol is not None and vol < 0.5) else "improving"
    return quadrant if quadrant in ("lagging", "weakening", "leading") else "neutral"


def calc_all_signals() -> dict:
    from datetime import datetime

    # ── 섹터 RRG 계산 ──────────────────────────────────────────
    # 1. 각 섹터의 평균 종가 시계열 계산
    min_req = EMA_LONG + EMA_SHORT + 5

    sector_closes: dict[str, list[float]] = {}
    for sector, codes in SECTORS.items():
        code_list = [c for c, _ in codes]
        valid_closes = []
        for c in code_list:
            if c not in ohlcv_store:
                continue
            closes = ohlcv_store[c]["closes"]
            if c in current_price and current_price[c] is not None:
                closes = closes[:-1]
            if len(closes) >= min_req:
                valid_closes.append(closes)
        if len(valid_closes) < 2:
            continue
        min_len = min(len(v) for v in valid_closes)
        # 섹터 평균 종가 시계열
        sector_closes[sector] = [
            sum(v[-min_len:][i] for v in valid_closes) / len(valid_closes)
            for i in range(min_len)
        ]

    # 2. 전체 벤치마크 = 모든 섹터 평균의 평균
    if sector_closes:
        min_len_all = min(len(v) for v in sector_closes.values())
        global_benchmark = [
            sum(sector_closes[s][-min_len_all:][i] for s in sector_closes) / len(sector_closes)
            for i in range(min_len_all)
        ]

        # 3. 섹터별 RS_Ratio / RS_Momentum 계산
        for sector, sc in sector_closes.items():
            aligned = sc[-min_len_all:]
            rs_ratio    = _calc_rs_ratio(aligned, global_benchmark)
            rs_momentum = _calc_rs_momentum(rs_ratio)

            curr_ratio = rs_ratio[-1]
            curr_mom   = rs_momentum[-1]

            # 궤적 초기화
            if sector not in sector_rrg_history or len(sector_rrg_history[sector]) == 0:
                tail = []
                for offset in range(TAIL_DAYS * 5, 0, -5):
                    if offset >= min_len_all - EMA_LONG:
                        continue
                    h = aligned[:-offset]
                    h_bm = global_benchmark[-min_len_all:][:-offset]
                    h_ratio = _calc_rs_ratio(h, h_bm)
                    h_mom   = _calc_rs_momentum(h_ratio)
                    t_r = h_ratio[-1]
                    t_m = h_mom[-1]
                    if t_r != 0.0 and t_m != 0.0:
                        tail.append({"rs_ratio": round(t_r, 3), "rs_momentum": round(t_m, 3)})
                sector_rrg_history[sector] = tail

            if curr_ratio != 0.0 and curr_mom != 0.0:
                sector_rrg_history[sector].append({
                    "rs_ratio":    round(curr_ratio, 3),
                    "rs_momentum": round(curr_mom,   3),
                })
                sector_rrg_history[sector] = sector_rrg_history[sector][-TAIL_DAYS:]

    # ── 종목 신호 계산 ──────────────────────────────────────────
    result = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rrg_params": {"ema_short": EMA_SHORT, "ema_long": EMA_LONG},
        "sectors":    {}
    }
    for sector, codes in SECTORS.items():
        sector_data = calc_sector_signals(sector, codes)
        # 섹터 RRG 데이터 주입
        if sector in sector_rrg_history and sector_rrg_history[sector]:
            last = sector_rrg_history[sector][-1]
            sector_data["rs_ratio"]    = last["rs_ratio"]
            sector_data["rs_momentum"] = last["rs_momentum"]
            sector_data["quadrant"]    = _quadrant(last["rs_ratio"], last["rs_momentum"])
            sector_data["tail"]        = sector_rrg_history[sector][-TAIL_DAYS:]
        else:
            sector_data["rs_ratio"]    = None
            sector_data["rs_momentum"] = None
            sector_data["quadrant"]    = "neutral"
            sector_data["tail"]        = []
        result["sectors"][sector] = sector_data

    return result


def _empty_sector(sector: str) -> dict:
    return {
        "sector": sector, "sector_ret_5d": None, "rising_ratio": None,
        "is_rising": False, "total": 0, "improving_count": 0,
        "lagging_count": 0, "watch_count": 0, "candidates": [],
    }