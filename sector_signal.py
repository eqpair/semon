"""
sector_signal.py  —  RRG 기반 소외주 탐색

계산 방식:
  1. rebased[i]  = closes[i] / closes[0] * 100   (첫날 = 100으로 정규화)
  2. benchmark   = mean(rebased) across all sector stocks  (동등가중 수익률 벤치마크)
  3. RS          = rebased[i] / benchmark[i] * 100
  4. RS_Ratio    = 100 + (RS - RS_MA10) / RS_STD10
  5. RS_Momentum = 100 + (ROC - ROC_MA10) / ROC_STD10
     ROC = (RS_Ratio / RS_Ratio[10일전] - 1) × 100

  ※ rebasing 이유: 주가 수준(삼성전자 6만원 vs 리노공업 20만원)이
     다른 종목들의 평균 종가를 벤치마크로 쓰면 고가 종목이 벤치마크를
     왜곡한다. 첫날 100 기준 수익률로 환산하면 모든 종목이 동등 가중된다.

사분면:
  improving : RS_Ratio < 100, Momentum >= 100  ← 핵심 매수 타이밍
  lagging   : RS_Ratio < 100, Momentum <  100  ← 소외 진행 중
  leading   : RS_Ratio >= 100, Momentum >= 100  ← 강하고 가속
  weakening : RS_Ratio >= 100, Momentum <  100  ← 강하지만 둔화

최적 파라미터 (백테스트): MA=10, ROC=10
"""
import logging
from config import SECTORS
from utils import now_kst

logger = logging.getLogger(__name__)

MA_PERIOD  = 10
ROC_PERIOD = 10
TAIL_DAYS  = 40   # 8주 궤적
CHART_DAYS = 120  # 주가 차트용 6개월

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


MIN_STD = 0.01  # RS_Ratio 폭발 방지용 최소 표준편차 임계값


def _rebase(closes: list[float]) -> list[float]:
    """첫날 종가를 100으로 정규화한 수익률 시계열 반환"""
    base = closes[0]
    if base == 0:
        return [0.0] * len(closes)
    return [c / base * 100.0 for c in closes]


def _make_benchmark(rebased_map: dict[str, list[float]]) -> list[float]:
    """
    섹터 내 전 종목의 rebased 시계열 동등가중 평균 → 벤치마크
    모든 rebased 시계열은 동일 길이(min_len으로 정렬된 상태)여야 한다.
    """
    codes = list(rebased_map.keys())
    n     = len(rebased_map[codes[0]])
    return [
        sum(rebased_map[c][i] for c in codes) / len(codes)
        for i in range(n)
    ]


def _calc_rs_ratio(rebased: list[float], benchmark: list[float]) -> list[float | None]:
    """
    rebased: 해당 종목의 첫날=100 정규화 시계열
    benchmark: 섹터 동등가중 rebased 평균
    RS = rebased / benchmark * 100
    RS_Ratio = 100 + (RS - MA) / STD  (STD < MIN_STD 이면 None)
    """
    n      = min(len(rebased), len(benchmark))
    raw_rs = [
        (rebased[i] / benchmark[i] * 100.0) if benchmark[i] > 0 else 0.0
        for i in range(n)
    ]
    ma  = _ma(raw_rs, MA_PERIOD)
    std = _std(raw_rs, MA_PERIOD)
    return [
        100.0 + (raw_rs[i] - ma[i]) / std[i]
        if ma[i] is not None and std[i] is not None and std[i] >= MIN_STD
        else None
        for i in range(n)
    ]


def _calc_rs_momentum(rs_ratio: list[float | None]) -> list[float | None]:
    """
    ROC = (RS_Ratio[i] / RS_Ratio[i-ROC_PERIOD] - 1) * 100
    RS_Momentum = 100 + (ROC - ROC_MA) / ROC_STD

    ※ 수정 이유:
       기존 코드는 ROC를 계산할 수 없는 초기 구간(rs_ratio가 None이거나
       ROC_PERIOD 이전)을 0.0으로 채웠다. 이 0들이 MA/STD 윈도우에 섞이면
       - STD가 실제보다 부풀려져 Momentum 값이 100 근처로 과도하게 눌리고
       - MA가 실제보다 낮아져 초기 Momentum이 위쪽으로 편향된다.

    수정: 유효한 ROC를 확보한 인덱스(roc_start)부터만 _ma/_std를 돌린 뒤
          그 결과를 원래 길이 n의 배열에 다시 매핑한다.
          roc_start 이전은 None으로 반환한다.
    """
    n = len(rs_ratio)

    # 1. 유효 ROC 인덱스와 값 수집
    roc_values: list[float] = []   # 유효 ROC 값들 (연속 시계열)
    roc_start:  int | None  = None  # 유효 ROC가 처음 등장하는 n-인덱스

    for i in range(ROC_PERIOD, n):
        a = rs_ratio[i]
        b = rs_ratio[i - ROC_PERIOD]
        if a is not None and b is not None and b != 0:
            if roc_start is None:
                roc_start = i
            roc_values.append((a / b - 1) * 100)
        else:
            # 유효 시작점 이후에 None 구간이 생기면 시계열을 끊는다
            if roc_start is not None:
                roc_values.append(0.0)  # 연속성 유지용 — 길이 맞춤

    if roc_start is None or len(roc_values) < ROC_PERIOD:
        return [None] * n  # 유효 ROC 자체가 부족하면 전부 None

    # 2. 유효 구간에서만 MA/STD 계산
    ma_vals  = _ma(roc_values,  ROC_PERIOD)
    std_vals = _std(roc_values, ROC_PERIOD)

    # 3. 결과를 원래 길이 n의 배열에 매핑
    result: list[float | None] = [None] * n
    for j, i in enumerate(range(roc_start, roc_start + len(roc_values))):
        if i >= n:
            break
        m_val = ma_vals[j]
        s_val = std_vals[j]
        r_val = roc_values[j]
        if m_val is not None and s_val is not None and s_val >= MIN_STD:
            result[i] = 100.0 + (r_val - m_val) / s_val
        # else: result[i] remains None

    return result


def _quadrant(ratio: float | None, mom: float | None) -> str:
    if ratio is None or mom is None:
        return "neutral"
    if ratio >= 100 and mom >= 100: return "leading"
    if ratio >= 100 and mom <  100: return "weakening"
    if ratio <  100 and mom <  100: return "lagging"
    return "improving"


# 변경 — 오늘 장중 데이터를 closes에서 제외하고 전일 종가 기준으로 계산
def _get_return(code: str, days: int) -> float | None:
    entry = ohlcv_store.get(code)
    if not entry:
        return None
    closes = entry["closes"]

    # 장중에 오늘 데이터가 closes[-1]에 포함될 수 있으므로
    # current_price가 있으면 closes[-1]은 오늘 임시값으로 보고 제외
    if code in current_price and current_price[code] is not None:
        hist_closes = closes[:-1]  # 오늘 제외 → 순수 과거 종가만
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
    # 현재가 있으면 오늘 임시 종가 제외하고 현재가로 교체
    if code in current_price and current_price[code] is not None:
        hist = closes[:-1][-CHART_DAYS:]
        return [round(v, 0) for v in hist] + [round(current_price[code], 0)]
    return [round(v, 0) for v in closes[-CHART_DAYS:]]


def calc_sector_signals(sector: str, codes: list[tuple[str, str]]) -> dict:
    code_list = [c for c, _ in codes]
    name_map  = {c: n for c, n in codes}

    min_req = MA_PERIOD + ROC_PERIOD + 5
    valid   = {
        c: ohlcv_store[c]["closes"][:-1] if c in current_price and current_price[c] is not None
        else ohlcv_store[c]["closes"]
        for c in code_list
        if c in ohlcv_store and len(ohlcv_store[c]["closes"]) >= min_req
    }

    if len(valid) < 2:
        return _empty_sector(sector)

    # ── 1. 길이 정렬 ─────────────────────────────────────────
    min_len = min(len(v) for v in valid.values())
    aligned = {c: v[-min_len:] for c, v in valid.items()}

    # ── 2. 전 종목 rebasing (첫날 = 100) ─────────────────────
    #  aligned 시작 시점 기준으로 rebase. offset 슬라이싱 시에도
    #  그 슬라이스의 [0]을 기준으로 재계산(_rebase 내부에서 처리).
    rebased_full = {c: _rebase(aligned[c]) for c in aligned}

    # ── 3. 동등가중 벤치마크 ──────────────────────────────────
    benchmark = _make_benchmark(rebased_full)

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
        now_price = current_price.get(code) or aligned[code][-1]

        rs_ratio    = _calc_rs_ratio(rebased_full[code], benchmark)
        rs_momentum = _calc_rs_momentum(rs_ratio)

        curr_ratio = next((v for v in reversed(rs_ratio)    if v is not None), None)
        curr_mom   = next((v for v in reversed(rs_momentum) if v is not None), None)
        quad       = _quadrant(curr_ratio, curr_mom)

        # ── 궤적(tail) 누적 ───────────────────────────────────
        # 핵심: offset 슬라이싱 시 전 종목을 동시에 잘라야
        #       벤치마크 길이와 종목 rebased 시계열이 일치한다.
        if code not in rrg_history or len(rrg_history[code]) == 0:
            tail = []
            for offset in range(TAIL_DAYS, 0, -5):
                if offset >= min_len:
                    continue
                # 전 종목을 동일하게 offset만큼 자름 → 벤치마크도 자동 일치
                hist_aligned   = {c: aligned[c][:-offset] for c in aligned}
                hist_rebased   = {c: _rebase(hist_aligned[c]) for c in hist_aligned}
                hist_benchmark = _make_benchmark(hist_rebased)
                hist_ratio    = _calc_rs_ratio(hist_rebased[code], hist_benchmark)
                hist_momentum = _calc_rs_momentum(hist_ratio)
                t_ratio = next((v for v in reversed(hist_ratio)    if v is not None), None)
                t_mom   = next((v for v in reversed(hist_momentum) if v is not None), None)
                if t_ratio is not None and t_mom is not None:
                    tail.append({"rs_ratio": round(t_ratio, 3), "rs_momentum": round(t_mom, 3)})
            rrg_history[code] = tail

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

        # 주가 차트용 데이터 (6개월, 마지막 20일 강조는 프론트에서)
        closes_chart = _get_closes_chart(code)

        candidates.append({
            "code":          code,
            "name":          name_map[code],
            "price":         now_price,
            "ret_1d":        round(r1  * 100, 2) if r1  is not None else None,
            "ret_5d":        round(r5  * 100, 2) if r5  is not None else None,
            "rs_5d":         round(rs_5d,  3)    if rs_5d  is not None else None,
            "rs_20d":        round(rs_20d, 3)    if rs_20d is not None else None,
            "rs_60d":        round(rs_60d, 3)    if rs_60d is not None else None,
            "vol_ratio":     round(vol,    3)    if vol    is not None else None,
            "rs_ratio":      round(curr_ratio, 3) if curr_ratio is not None else None,
            "rs_momentum":   round(curr_mom,   3) if curr_mom   is not None else None,
            "quadrant":      quad,
            "tail":          rrg_history.get(code, [])[-TAIL_DAYS:],
            "signal":        _rrg_signal(quad, vol),
            "closes_chart":  closes_chart,   # 6개월 주가 (마지막 20일은 프론트에서 강조)
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
    # ── rrg_history 정리: config에서 사라진 종목 코드 제거 ────
    # SECTORS 변경 시 더 이상 사용하지 않는 코드가 메모리에
    # 계속 쌓이는 것을 방지한다.
    active_codes = {code for codes in SECTORS.values() for code, _ in codes}
    stale = [c for c in list(rrg_history.keys()) if c not in active_codes]
    for c in stale:
        del rrg_history[c]
    if stale:
        logger.info(f"rrg_history 정리: {len(stale)}개 코드 삭제 {stale[:5]}{'...' if len(stale)>5 else ''}")

    result = {
        "updated_at":  now_kst().strftime("%Y-%m-%d %H:%M:%S"),
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