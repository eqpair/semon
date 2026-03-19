"""
sector_signal.py  —  RRG 기반 소외주 탐색

계산 방식 (블룸버그 RRG 방식):
  1. rebased[i]  = closes[i] / closes[0] * 100        (첫날 = 100 정규화)
  2. benchmark   = 섹터 내 전 종목 rebased 동등가중 평균
  3. RS          = rebased / benchmark × 100           (상대강도 원시값)
  4. RS_Ratio    = 100 × (RS_MA10 / RS_MA40)
                   단기(10일) MA가 장기(40일) MA 대비 위에 있으면 > 100
  5. RS_Momentum = 100 × (RS_Ratio_MA10 / RS_Ratio_MA40)
                   RS_Ratio 자체의 단기/장기 MA 비율

  ※ Z-score 방식 대비 장점:
     - STD 분모가 없으므로 횡보 구간에서 값이 폭발하지 않음
     - 장기 MA가 자연스러운 스무딩 역할 → 궤적이 부드러워짐
     - 100 기준선이 명확한 의미 (단기=장기 추세이면 정확히 100)
     - 블룸버그·스톡차트 등 실전 검증된 수십 년의 사용 이력

사분면:
  improving : RS_Ratio < 100, Momentum >= 100  ← 핵심 매수 타이밍
  lagging   : RS_Ratio < 100, Momentum <  100  ← 소외 진행 중
  leading   : RS_Ratio >= 100, Momentum >= 100  ← 강하고 가속
  weakening : RS_Ratio >= 100, Momentum <  100  ← 강하지만 둔화

파라미터: MA_SHORT=10, MA_LONG=40 (블룸버그 기본값)
최소 필요 데이터: 80일 (RS_Momentum 계산을 위해 RS_Ratio_MA40 필요)
"""
import logging
from config import SECTORS
from utils import now_kst

logger = logging.getLogger(__name__)

MA_SHORT   = 10   # 단기 이동평균 (블룸버그 기본값)
MA_LONG    = 40   # 장기 이동평균 (블룸버그 기본값)
TAIL_DAYS  = 40   # 8주 궤적
CHART_DAYS = 120  # 주가 차트용 6개월

ohlcv_store:    dict[str, dict]       = {}
current_price:  dict[str, float]      = {}
rrg_history:    dict[str, list[dict]] = {}
market_cap_store: dict[str, int]      = {}  # { code: 시총(억원) }


def load_market_caps_into_store(caps: dict[str, int]) -> None:
    """main.py에서 시총 데이터를 주입할 때 호출"""
    market_cap_store.clear()
    market_cap_store.update({k: v for k, v in caps.items() if v > 0})

# 수식 버전 — 파라미터나 수식이 바뀌면 이 값을 올려서
# 재시작 시 rrg_history를 자동으로 무효화하고 소급 재계산한다.
RRG_VERSION = f"bloomberg_v1_s{MA_SHORT}_l{MA_LONG}"


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


def _rebase(closes: list[float]) -> list[float]:
    """첫날 종가를 100으로 정규화한 수익률 시계열 반환"""
    base = closes[0]
    if base == 0:
        return [0.0] * len(closes)
    return [c / base * 100.0 for c in closes]


def _make_benchmark(rebased_map: dict[str, list[float]]) -> list[float]:
    """
    섹터 벤치마크 = 시총 가중 rebased 평균

    시총 데이터가 있는 종목은 시총 비례 가중치를 적용한다.
    시총 데이터가 없는 종목은 동등가중으로 폴백한다.
    삼성전자처럼 시총이 압도적인 종목이 벤치마크를 지배하도록 허용하여
    실제 시장 구조를 반영한다.
    """
    codes = list(rebased_map.keys())
    n     = len(rebased_map[codes[0]])

    # 시총 가중치 계산
    caps   = {c: market_cap_store.get(c, 0) for c in codes}
    total  = sum(caps.values())

    if total > 0:
        # 시총 데이터 있는 종목만 가중 / 없는 종목은 최소 가중치(1억원 가정)
        adj    = {c: caps[c] if caps[c] > 0 else 1 for c in codes}
        adj_total = sum(adj.values())
        weights = {c: adj[c] / adj_total for c in codes}
    else:
        # 시총 데이터 전혀 없으면 동등가중
        weights = {c: 1.0 / len(codes) for c in codes}

    return [
        sum(rebased_map[c][i] * weights[c] for c in codes)
        for i in range(n)
    ]


def _calc_rs_ratio(rebased: list[float], benchmark: list[float]) -> list[float | None]:
    """
    RS_Ratio = 100 × (RS_MA_SHORT / RS_MA_LONG)

    RS_MA_SHORT가 RS_MA_LONG 위에 있으면 100 초과 (상대적 강세)
    RS_MA_SHORT가 RS_MA_LONG 아래에 있으면 100 미만 (상대적 약세)

    STD 정규화 없음 → 횡보 구간에서 값 폭발 없음, 궤적 부드러움
    """
    n      = min(len(rebased), len(benchmark))
    raw_rs = [
        (rebased[i] / benchmark[i] * 100.0) if benchmark[i] > 0 else 0.0
        for i in range(n)
    ]
    ma_short = _ma(raw_rs, MA_SHORT)
    ma_long  = _ma(raw_rs, MA_LONG)
    return [
        100.0 * ma_short[i] / ma_long[i]
        if ma_short[i] is not None and ma_long[i] is not None and ma_long[i] != 0
        else None
        for i in range(n)
    ]


def _calc_rs_momentum(rs_ratio: list[float | None]) -> list[float | None]:
    """
    RS_Momentum = 100 × (RS_Ratio_MA_SHORT / RS_Ratio_MA_LONG)

    RS_Ratio 자체의 단기/장기 MA 비율.
    RS_Ratio가 가속하고 있으면(단기 MA > 장기 MA) 100 초과.
    RS_Ratio가 감속하고 있으면(단기 MA < 장기 MA) 100 미만.

    None 값은 건너뛰고 유효한 값만으로 MA를 계산한다.
    """
    n = len(rs_ratio)

    # None 제외한 유효값만 추출 (인덱스 보존)
    valid_indices = [i for i, v in enumerate(rs_ratio) if v is not None]
    valid_values  = [rs_ratio[i] for i in valid_indices]

    if len(valid_values) < MA_LONG:
        return [None] * n

    ma_short = _ma(valid_values, MA_SHORT)
    ma_long  = _ma(valid_values, MA_LONG)

    # 결과를 원래 n 길이 배열에 매핑
    result: list[float | None] = [None] * n
    for j, orig_i in enumerate(valid_indices):
        s = ma_short[j]
        l = ma_long[j]
        if s is not None and l is not None and l != 0:
            result[orig_i] = 100.0 * s / l

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

    min_req = MA_LONG * 2 + 5  # RS_Momentum 계산에 최소 MA_LONG*2 필요
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

    # 섹터 RRG용 — 이 섹터의 동등가중 rebased 시계열을 캐시에 저장
    # (벤치마크 자체가 섹터의 동등가중 수익률 시계열이다)
    _update_sector_rebased_cache(sector, rebased_full)

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
        # 수식 버전 체크: 파라미터가 바뀌었으면 기존 history 무효화
        stored = rrg_history.get(code)
        if stored and stored[0].get("_v") != RRG_VERSION:
            stored = None
            rrg_history.pop(code, None)

        if not stored:
            # 1일 단위로 소급 계산 → 재시작 직후에도 TAIL_DAYS 포인트 전부 채움
            tail = []
            for offset in range(TAIL_DAYS, 0, -1):
                if offset >= min_len:
                    continue
                hist_aligned   = {c: aligned[c][:-offset] for c in aligned}
                hist_rebased   = {c: _rebase(hist_aligned[c]) for c in hist_aligned}
                hist_benchmark = _make_benchmark(hist_rebased)
                hist_ratio     = _calc_rs_ratio(hist_rebased[code], hist_benchmark)
                hist_momentum  = _calc_rs_momentum(hist_ratio)
                t_ratio = next((v for v in reversed(hist_ratio)    if v is not None), None)
                t_mom   = next((v for v in reversed(hist_momentum) if v is not None), None)
                if t_ratio is not None and t_mom is not None:
                    tail.append({
                        "rs_ratio":    round(t_ratio, 3),
                        "rs_momentum": round(t_mom,   3),
                        "_v":          RRG_VERSION,   # 버전 태그
                    })
            # 버전 태그를 마지막 항목에도 유지 (빈 tail이면 더미 삽입)
            rrg_history[code] = tail if tail else [{"_v": RRG_VERSION}]

        if curr_ratio is not None and curr_mom is not None:
            rrg_history[code].append({
                "rs_ratio":    round(curr_ratio, 3),
                "rs_momentum": round(curr_mom,   3),
                "_v":          RRG_VERSION,
            })
            rrg_history[code] = rrg_history[code][-TAIL_DAYS:]

        # 프론트엔드 출력 시 내부 버전 키(_v) 제거
        clean_tail = [
            {k: v for k, v in pt.items() if k != "_v"}
            for pt in rrg_history.get(code, [])
            if pt.get("rs_ratio") is not None
        ]

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
            "tail":          clean_tail[-TAIL_DAYS:],
            "signal":        _improving_grade(quad, clean_tail, vol, curr_mom),
            "combo_score":   0,   # calc_all_signals에서 섹터 RRG 완료 후 채워짐
            "closes_chart":  closes_chart,
        })

    def _sort_key(x):
        # prime > confirm > watch > improving(구분 없음) > 나머지
        sig_order = {"prime": 0, "confirm": 1, "watch": 2,
                     "improving": 3, "lagging": 4, "weakening": 5, "leading": 6, "neutral": 7}
        return (sig_order.get(x["signal"], 7), x["rs_ratio"] if x["rs_ratio"] is not None else 999)

    candidates.sort(key=_sort_key)

    return {
        "sector":          sector,
        "sector_ret_5d":   round(sector_ret_5d * 100, 2),
        "rising_ratio":    round(rising_ratio, 2),
        "is_rising":       is_rising,
        "total":           len(candidates),
        "prime_count":     sum(1 for c in candidates if c["signal"] == "prime"),
        "confirm_count":   sum(1 for c in candidates if c["signal"] == "confirm"),
        "improving_count": sum(1 for c in candidates if c["signal"] in ("prime", "confirm")),
        "watch_count":     sum(1 for c in candidates if c["signal"] == "watch"),
        "lagging_count":   sum(1 for c in candidates if c["quadrant"] == "lagging"),
        "candidates":      candidates,
    }


def _improving_grade(quadrant: str, tail: list[dict], vol: float | None,
                     curr_mom: float | None) -> str:
    """
    improving 사분면 종목의 품질을 3단계로 분류한다.

    PRIME   : 직전 tail에서 lagging → improving 전환이 확인되고
              RS_Momentum이 상승 중 (가장 이른 매수 타이밍)
    CONFIRM : improving에 10일(2주) 이상 머물며 안정적으로 상승 중
    WATCH   : improving이지만 전환 확인 불충분 또는 거래량 미확인

    판정 기준:
      - tail[-N:]의 사분면 이력에서 직전 lagging 구간을 탐지
      - RS_Momentum 추세: tail 최근 3개 값이 상승하면 가속 확인
    """
    if quadrant != "improving":
        return quadrant if quadrant in ("lagging", "weakening", "leading") else "neutral"

    # 거래량 미확인이면 무조건 WATCH
    if vol is not None and vol < 0.5:
        return "watch"

    if not tail or len(tail) < 2:
        return "watch"

    # tail의 사분면 이력 추출
    def q(pt):
        r, m = pt.get("rs_ratio"), pt.get("rs_momentum")
        if r is None or m is None: return "neutral"
        if r >= 100 and m >= 100: return "leading"
        if r >= 100 and m < 100:  return "weakening"
        if r < 100  and m < 100:  return "lagging"
        return "improving"

    tail_quads = [q(pt) for pt in tail]

    # 직전 lagging 구간 탐지: 최근 10개 안에 lagging이 있었는가
    recent = tail_quads[-10:]
    had_lagging = "lagging" in recent

    # RS_Momentum 가속 여부: 최근 3개 값이 단조 증가
    moms = [pt.get("rs_momentum") for pt in tail[-3:] if pt.get("rs_momentum") is not None]
    mom_rising = len(moms) >= 2 and all(moms[i] < moms[i+1] for i in range(len(moms)-1))

    # improving 유지 기간: tail 뒤에서부터 연속 improving 카운트
    improving_streak = 0
    for pt in reversed(tail):
        if q(pt) == "improving":
            improving_streak += 1
        else:
            break

    if had_lagging and mom_rising:
        return "prime"           # 전환 직후 + 가속 확인
    elif improving_streak >= 2:
        return "confirm"         # 2회 이상 연속 improving
    else:
        return "watch"


def _combo_score(stock_signal: str, sector_quadrant: str,
                 curr_mom: float | None, tail: list[dict]) -> int:
    """
    섹터 상황과 종목 신호를 결합한 복합 점수 (0~3).

    +1  섹터가 소외권(lagging / improving) — 아직 시장 대비 안 오른 섹터
    +1  종목이 prime 또는 confirm improving
    +1  종목 RS_Momentum이 상승 가속 중

    3점 → 핵심 매수 후보 (소외 섹터 안에서 먼저 움직이는 종목)
    2점 → 관심 후보
    1점 이하 → 일반
    """
    score = 0

    # 섹터 소외권 여부
    if sector_quadrant in ("lagging", "improving"):
        score += 1

    # 종목 improving 품질
    if stock_signal in ("prime", "confirm"):
        score += 1

    # 모멘텀 가속
    moms = [pt.get("rs_momentum") for pt in tail[-3:] if pt.get("rs_momentum") is not None]
    if len(moms) >= 2 and all(moms[i] < moms[i+1] for i in range(len(moms)-1)):
        if curr_mom is not None and curr_mom >= 100:
            score += 1

    return score


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
        "rrg_params":  {"ma_short": MA_SHORT, "ma_long": MA_LONG},
        "sectors":     {}
    }
    for sector, codes in SECTORS.items():
        result["sectors"][sector] = calc_sector_signals(sector, codes)

    # 섹터 레벨 RRG — 전체 섹터 계산 완료 후 실행
    result["sector_rrg"] = calc_sector_rrg(result["sectors"])

    # ── combo_score 사후 계산 ─────────────────────────────────
    # 섹터 RRG가 완성된 뒤에야 섹터 사분면을 알 수 있으므로
    # 여기서 각 종목의 combo_score를 채운다.
    for sector_name, sec_data in result["sectors"].items():
        sector_quad = result["sector_rrg"].get(sector_name, {}).get("quadrant", "neutral")
        for c in sec_data.get("candidates", []):
            c["combo_score"] = _combo_score(
                c["signal"], sector_quad, c.get("rs_momentum"), c.get("tail", [])
            )

    return result


def _empty_sector(sector: str) -> dict:
    return {
        "sector": sector, "sector_ret_5d": None, "rising_ratio": None,
        "is_rising": False, "total": 0, "improving_count": 0,
        "lagging_count": 0, "candidates": [],
    }

# ══════════════════════════════════════════════════════════════
# 섹터 레벨 RRG
# ══════════════════════════════════════════════════════════════
# 각 섹터를 하나의 자산으로 취급.
# 섹터의 동등가중 rebased 시계열을 구한 뒤,
# 전체 섹터들의 평균을 시장 벤치마크로 삼아 RS_Ratio / RS_Momentum 계산.
#
# 종목 RRG와 완전히 동일한 수식을 사용하므로
# _calc_rs_ratio / _calc_rs_momentum 을 그대로 재사용한다.
# ──────────────────────────────────────────────────────────────

# 섹터 RRG용 rrg_history 키 접두사
_SECTOR_KEY_PREFIX = "sector:"

# 섹터별 동등가중 rebased 시계열 캐시 (calc_sector_signals에서 채운다)
# { sector_name: [rebased_avg_0, rebased_avg_1, ...] }
_sector_rebased_cache: dict[str, list[float]] = {}


def _update_sector_rebased_cache(sector: str, rebased_map: dict[str, list[float]]):
    """
    calc_sector_signals 내부에서 rebased_full이 만들어진 시점에 호출.
    섹터의 동등가중 rebased 평균(= 내부 벤치마크 자체)을 캐시에 저장한다.
    """
    _sector_rebased_cache[sector] = _make_benchmark(rebased_map)


def calc_sector_rrg(sector_results: dict) -> dict:
    """
    전체 섹터 RRG 계산.

    sector_results: calc_all_signals()가 만든 result["sectors"] 전체.
    반환: { sector_name: { rs_ratio, rs_momentum, quadrant, tail } }

    흐름:
      1. _sector_rebased_cache에서 섹터별 동등가중 시계열 수집
      2. 가장 짧은 섹터에 맞춰 길이 정렬 (시장 벤치마크 일관성)
      3. 전체 섹터 평균 → 시장 벤치마크
      4. 섹터별 RS_Ratio / RS_Momentum 계산
      5. tail 누적 (종목 tail과 동일 방식)
    """
    # 유효한 섹터만 (캐시에 있고 길이 충분한 것)
    min_req = MA_LONG * 2 + 5  # RS_Momentum 계산에 최소 MA_LONG*2 필요
    valid = {
        s: v for s, v in _sector_rebased_cache.items()
        if len(v) >= min_req
    }

    if len(valid) < 2:
        return {}

    # ── 1. 길이 정렬 ─────────────────────────────────────────
    min_len  = min(len(v) for v in valid.values())
    aligned  = {s: v[-min_len:] for s, v in valid.items()}

    # ── 2. 이미 rebased 평균이므로 rebase 불필요
    #       단, 시작점이 다를 수 있으므로 각 섹터 시계열을
    #       aligned[0] 기준으로 다시 rebase한다
    re_rebased = {s: _rebase(v) for s, v in aligned.items()}

    # ── 3. 시장 벤치마크 = 전체 섹터 동등가중 평균 ────────────
    market_bm = _make_benchmark(re_rebased)

    result = {}
    for sector in valid:
        key = _SECTOR_KEY_PREFIX + sector

        rs_ratio    = _calc_rs_ratio(re_rebased[sector], market_bm)
        rs_momentum = _calc_rs_momentum(rs_ratio)

        curr_ratio = next((v for v in reversed(rs_ratio)    if v is not None), None)
        curr_mom   = next((v for v in reversed(rs_momentum) if v is not None), None)
        quad       = _quadrant(curr_ratio, curr_mom)

        # ── tail 누적 (종목 tail과 동일 방식) ─────────────────
        stored_key = rrg_history.get(key)
        if stored_key and stored_key[0].get("_v") != RRG_VERSION:
            stored_key = None
            rrg_history.pop(key, None)

        if not stored_key:
            tail = []
            for offset in range(TAIL_DAYS, 0, -1):
                if offset >= min_len:
                    continue
                hist_aligned   = {s: aligned[s][:-offset] for s in aligned}
                hist_rebased   = {s: _rebase(hist_aligned[s]) for s in hist_aligned}
                hist_market_bm = _make_benchmark(hist_rebased)
                hist_ratio     = _calc_rs_ratio(hist_rebased[sector], hist_market_bm)
                hist_momentum  = _calc_rs_momentum(hist_ratio)
                t_ratio = next((v for v in reversed(hist_ratio)    if v is not None), None)
                t_mom   = next((v for v in reversed(hist_momentum) if v is not None), None)
                if t_ratio is not None and t_mom is not None:
                    tail.append({
                        "rs_ratio":    round(t_ratio, 3),
                        "rs_momentum": round(t_mom,   3),
                        "_v":          RRG_VERSION,
                    })
            rrg_history[key] = tail if tail else [{"_v": RRG_VERSION}]

        if curr_ratio is not None and curr_mom is not None:
            rrg_history[key].append({
                "rs_ratio":    round(curr_ratio, 3),
                "rs_momentum": round(curr_mom,   3),
                "_v":          RRG_VERSION,
            })
            rrg_history[key] = rrg_history[key][-TAIL_DAYS:]

        clean_tail = [
            {k: v for k, v in pt.items() if k != "_v"}
            for pt in rrg_history.get(key, [])
            if pt.get("rs_ratio") is not None
        ]

        # sector_results에서 5D 수익률 가져오기
        sec_data = sector_results.get(sector, {})

        result[sector] = {
            "rs_ratio":        round(curr_ratio, 3) if curr_ratio is not None else None,
            "rs_momentum":     round(curr_mom,   3) if curr_mom   is not None else None,
            "quadrant":        quad,
            "tail":            clean_tail[-TAIL_DAYS:],
            "sector_ret_5d":   sec_data.get("sector_ret_5d"),
            "improving_count": sec_data.get("improving_count", 0),
            "watch_count":     sec_data.get("watch_count", 0),
            "total":           sec_data.get("total", 0),
        }

    return result