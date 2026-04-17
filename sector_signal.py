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
import json
import logging
import os
from pathlib import Path
from config import SECTORS
from utils import now_kst
from datetime import timedelta

logger = logging.getLogger(__name__)

MA_SHORT   = 10   # 단기 이동평균 (블룸버그 기본값)
MA_LONG    = 40   # 장기 이동평균 (블룸버그 기본값)
TAIL_DAYS  = 40   # 8주 궤적
CHART_DAYS = 120  # 주가 차트용 6개월

RRG_HISTORY_PATH = "/home/eq/semon/data/rrg_history.json"

ohlcv_store:    dict[str, dict]       = {}
current_price:  dict[str, float]      = {}
current_volume: dict[str, float]      = {}  # 장중 누적 거래량
rrg_history:    dict[str, list[dict]] = {}
market_cap_store: dict[str, int]      = {}  # { code: 시총(억원) }


def load_market_caps_into_store(caps: dict[str, int]) -> None:
    """main.py에서 시총 데이터를 주입할 때 호출"""
    market_cap_store.clear()
    market_cap_store.update({k: v for k, v in caps.items() if v > 0})

# 수식 버전 — 파라미터나 수식이 바뀌면 이 값을 올려서
# 재시작 시 rrg_history를 자동으로 무효화하고 소급 재계산한다.
#RRG_VERSION = f"bloomberg_v1_s{MA_SHORT}_l{MA_LONG}"
#RRG_VERSION = f"bloomberg_v2_s{MA_SHORT}_l{MA_LONG}_cap20"
RRG_VERSION = f"bloomberg_v2_s{MA_SHORT}_l{MA_LONG}_daily"

# 섹터 RRG용 rrg_history 키 접두사
_SECTOR_KEY_PREFIX = "sector:"

# 섹터별 동등가중 rebased 시계열 캐시 (calc_sector_signals에서 채운다)
_sector_rebased_cache: dict[str, list[float]] = {}


# ── rrg_history 영속화 ────────────────────────────────────────

def save_rrg_history(path: str = RRG_HISTORY_PATH) -> None:
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        tmp = path + ".tmp"
        clean = {
            code: [{k: v for k, v in pt.items() if k != "_v"} for pt in tail]
            for code, tail in rrg_history.items()
        }
        payload = {"__version__": RRG_VERSION, "data": clean}
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, path)
        logger.debug(f"rrg_history 저장: {len(rrg_history)}개 코드")
    except Exception as e:
        logger.warning(f"rrg_history 저장 실패: {e}")


def load_rrg_history(path: str = RRG_HISTORY_PATH) -> None:
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)

        if "__version__" not in raw:
            logger.info("rrg_history 구형 포맷 감지 — 소급 재계산합니다")
            return

        if raw.get("__version__") != RRG_VERSION:
            logger.info(f"rrg_history 버전 불일치 ({raw.get('__version__')} → {RRG_VERSION}) — 소급 재계산합니다")
            return

        data = raw.get("data", {})
        loaded = 0
        for code, tail in data.items():
            if tail:
                rrg_history[code] = tail
                loaded += 1
        logger.info(f"rrg_history 복원: {loaded}개 코드")
    except FileNotFoundError:
        logger.info("rrg_history 파일 없음 — 소급 계산으로 채워집니다")
    except Exception as e:
        logger.warning(f"rrg_history 로드 실패: {e}")


def update_ohlcv(data: dict[str, dict | None]):
    for code, val in data.items():
        if val:
            ohlcv_store[code] = val


def update_prices(prices: dict[str, tuple]):
    for code, (price, volume) in prices.items():
        if price is not None:
            current_price[code] = price
        if volume is not None:
            current_volume[code] = volume


def _ma(values: list[float], period: int) -> list[float | None]:
    result = [None] * len(values)
    for i in range(period - 1, len(values)):
        result[i] = sum(values[i - period + 1:i + 1]) / period
    return result


def _rebase(closes: list[float]) -> list[float]:
    base = closes[0]
    if base == 0:
        return [0.0] * len(closes)
    return [c / base * 100.0 for c in closes]


def _make_benchmark(rebased_map: dict[str, list[float]]) -> list[float]:
    CAP_WEIGHT = 0.20

    codes  = list(rebased_map.keys())
    n_code = len(codes)
    n_data = len(rebased_map[codes[0]])

    caps  = {c: market_cap_store.get(c, 0) for c in codes}
    total = sum(caps.values())

    if total > 0:
        adj       = {c: caps[c] if caps[c] > 0 else 1 for c in codes}
        adj_total = sum(adj.values())
        weights   = {c: adj[c] / adj_total for c in codes}

        for _ in range(10):
            capped  = {c: min(w, CAP_WEIGHT) for c, w in weights.items()}
            excess  = 1.0 - sum(capped.values())

            if excess < 1e-9:
                weights = capped
                break

            free     = {c: w for c, w in capped.items() if w < CAP_WEIGHT}
            free_sum = sum(free.values())
            if free_sum < 1e-9:
                weights = {c: 1.0 / n_code for c in codes}
                break

            for c in codes:
                if capped[c] < CAP_WEIGHT:
                    capped[c] += excess * (capped[c] / free_sum)
            weights = capped
    else:
        weights = {c: 1.0 / n_code for c in codes}

    return [
        sum(rebased_map[c][i] * weights[c] for c in codes)
        for i in range(n_data)
    ]


def _calc_rs_ratio(rebased: list[float], benchmark: list[float]) -> list[float | None]:
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
    n = len(rs_ratio)

    valid_indices = [i for i, v in enumerate(rs_ratio) if v is not None]
    valid_values  = [rs_ratio[i] for i in valid_indices]

    if len(valid_values) < MA_LONG:
        return [None] * n

    ma_short = _ma(valid_values, MA_SHORT)
    ma_long  = _ma(valid_values, MA_LONG)

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
    if avg <= 0:
        return None
    # 장중이면 실시간 누적 거래량 사용, 없으면 전일 기준
    today_vol = current_volume.get(code)
    return (today_vol / avg) if today_vol else (vols[-2] / avg)


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


def _short_rs_grade(rs_5d, rs_20d, quadrant, gap_1d=None, ret_5d=None):
    """
    단기 상대강도 등급

    breakout : 당일 섹터 대비 3%p 이상 이탈 (오늘 튄 것)
               OR Lagging/Improving인데 5일 rs_5d 2배 이상 + 절대 상승
    rising   : 단기 섹터 대비 우세
    neutral  : 섹터 수준
    weak     : 단기 섹터 대비 열세
    """
    if rs_5d is None:
        return "neutral"

    # 당일 디커플링 감지 — 섹터 대비 3%p 이상 이탈
    if gap_1d is not None and gap_1d >= 0.03:
        return "breakout"

    # 5일 누적 디커플링 — 절대 상승 + 섹터 대비 2배
    if quadrant in ("lagging", "improving") and rs_5d >= 2.0 \
            and ret_5d is not None and ret_5d > 0:
        return "breakout"

    elif rs_5d >= 1.3:
        return "rising"
    elif rs_5d >= 0.7:
        return "neutral"
    else:
        return "weak"


def _update_sector_rebased_cache(sector: str, rebased_map: dict[str, list[float]]):
    _sector_rebased_cache[sector] = _make_benchmark(rebased_map)


def calc_sector_signals(sector: str, codes: list[tuple[str, str]]) -> dict:
    code_list = [c for c, _ in codes]
    name_map  = {c: n for c, n in codes}

    min_req = MA_LONG * 2 + 5
    valid   = {
        c: ohlcv_store[c]["closes"][:-1] if c in current_price and current_price[c] is not None
        else ohlcv_store[c]["closes"]
        for c in code_list
        if c in ohlcv_store and len(ohlcv_store[c]["closes"]) >= min_req
    }

    if len(valid) < 2:
        return _empty_sector(sector)

    # ── 1. 길이 정렬
    min_len = min(len(v) for v in valid.values())
    aligned = {c: v[-min_len:] for c, v in valid.items()}

    # ── 2. 전 종목 rebasing
    rebased_full = {c: _rebase(aligned[c]) for c in aligned}

    # ── 3. 벤치마크
    benchmark = _make_benchmark(rebased_full)

    # 섹터 RRG용 캐시
    _update_sector_rebased_cache(sector, rebased_full)

    returns_5d = {c: r for c in valid if (r := _get_return(c, 5)) is not None}
    if not returns_5d:
        return _empty_sector(sector)

    sector_ret_5d  = sum(returns_5d.values()) / len(returns_5d)

    # 당일 섹터 평균 수익률
    returns_1d    = {c: r for c in valid if (r := _get_return(c, 1)) is not None}
    sector_ret_1d = sum(returns_1d.values()) / len(returns_1d) if returns_1d else None

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

        # ── 궤적(tail) 누적 — 하루 1포인트
        stored = rrg_history.get(code)

        if not stored:
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
                        "date":        (now_kst() - timedelta(days=offset)).strftime("%Y-%m-%d"),
                        "rs_ratio":    round(t_ratio, 3),
                        "rs_momentum": round(t_mom,   3),
                    })
            rrg_history[code] = tail if tail else []

        if curr_ratio is not None and curr_mom is not None:
            today = now_kst().strftime("%Y-%m-%d")
            last = rrg_history[code][-1] if rrg_history[code] else {}
            if last.get("date") != today:
                rrg_history[code].append({
                    "date":        today,
                    "rs_ratio":    round(curr_ratio, 3),
                    "rs_momentum": round(curr_mom,   3),
                })
                rrg_history[code] = rrg_history[code][-TAIL_DAYS:]

        # date 제거 후 프론트엔드 출력
        clean_tail = [
            {"rs_ratio": pt["rs_ratio"], "rs_momentum": pt["rs_momentum"]}
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

        # 당일 디커플링 — 종목 당일 수익률 - 섹터 당일 평균
        gap_1d = (r1 - sector_ret_1d) if r1 is not None and sector_ret_1d is not None else None

        closes_chart = _get_closes_chart(code)

        candidates.append({
            "code":           code,
            "name":           name_map[code],
            "price":          now_price,
            "ret_1d":         round(r1  * 100, 2) if r1  is not None else None,
            "ret_5d":         round(r5  * 100, 2) if r5  is not None else None,
            "ret_20d":        round(r20 * 100, 2) if r20 is not None else None,
            "ret_60d":        round(r60 * 100, 2) if r60 is not None else None,
            "gap_1d":         round(gap_1d * 100, 2) if gap_1d is not None else None,
            "rs_5d":          round(rs_5d,  3)    if rs_5d  is not None else None,
            "rs_20d":         round(rs_20d, 3)    if rs_20d is not None else None,
            "rs_60d":         round(rs_60d, 3)    if rs_60d is not None else None,
            "vol_ratio":      round(vol,    3)    if vol    is not None else None,
            "market_cap":     market_cap_store.get(code, 0),
            "rs_ratio":       round(curr_ratio, 3) if curr_ratio is not None else None,
            "rs_momentum":    round(curr_mom,   3) if curr_mom   is not None else None,
            "quadrant":       quad,
            "tail":           clean_tail[-TAIL_DAYS:],
            "signal":         _improving_grade(quad, clean_tail, vol, curr_mom),
            "short_rs_grade": _short_rs_grade(rs_5d, rs_20d, quad, gap_1d, r5),
            "combo_score":    0,
            "closes_chart":   closes_chart,
        })

    def _sort_key(x):
        sig_order = {"prime": 0, "confirm": 1, "watch": 2,
                     "improving": 3, "lagging": 4, "weakening": 5, "leading": 6, "neutral": 7}
        return (sig_order.get(x["signal"], 7), x["rs_ratio"] if x["rs_ratio"] is not None else 999)

    candidates.sort(key=_sort_key)

    return {
        "sector":          sector,
        "sector_ret_5d":   round(sector_ret_5d * 100, 2),
        "sector_ret_1d":   round(sector_ret_1d * 100, 2) if sector_ret_1d is not None else None,
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
    if quadrant != "improving":
        return quadrant if quadrant in ("lagging", "weakening", "leading") else "neutral"

    if vol is not None and vol < 0.5:
        return "watch"

    if not tail or len(tail) < 2:
        return "watch"

    def q(pt):
        r, m = pt.get("rs_ratio"), pt.get("rs_momentum")
        if r is None or m is None: return "neutral"
        if r >= 100 and m >= 100: return "leading"
        if r >= 100 and m < 100:  return "weakening"
        if r < 100  and m < 100:  return "lagging"
        return "improving"

    tail_quads = [q(pt) for pt in tail]

    recent = tail_quads[-10:]
    had_lagging = "lagging" in recent

    moms = [pt.get("rs_momentum") for pt in tail[-3:] if pt.get("rs_momentum") is not None]
    mom_rising = len(moms) >= 2 and all(moms[i] < moms[i+1] for i in range(len(moms)-1))

    improving_streak = 0
    for pt in reversed(tail):
        if q(pt) == "improving":
            improving_streak += 1
        else:
            break

    if had_lagging and mom_rising:
        return "prime"
    elif improving_streak >= 2:
        return "confirm"
    else:
        return "watch"


def _combo_score(stock_signal: str, sector_quadrant: str,
                 curr_mom: float | None, tail: list[dict]) -> int:
    score = 0

    if sector_quadrant in ("lagging", "improving"):
        score += 1

    if stock_signal in ("prime", "confirm"):
        score += 1

    moms = [pt.get("rs_momentum") for pt in tail[-3:] if pt.get("rs_momentum") is not None]
    if len(moms) >= 2 and all(moms[i] < moms[i+1] for i in range(len(moms)-1)):
        if curr_mom is not None and curr_mom >= 100:
            score += 1

    return score


def calc_all_signals() -> dict:
    # rrg_history 정리 — 섹터 키(sector:)는 삭제 대상 제외
    active_codes = {code for codes in SECTORS.values() for code, _ in codes}
    stale = [c for c in list(rrg_history.keys())
             if c not in active_codes and not c.startswith(_SECTOR_KEY_PREFIX)]
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

    result["sector_rrg"] = calc_sector_rrg(result["sectors"])

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


def calc_sector_rrg(sector_results: dict) -> dict:
    min_req = MA_LONG * 2 + 5
    valid = {
        s: v for s, v in _sector_rebased_cache.items()
        if len(v) >= min_req
    }

    if len(valid) < 2:
        return {}

    min_len  = min(len(v) for v in valid.values())
    aligned  = {s: v[-min_len:] for s, v in valid.items()}
    re_rebased = {s: _rebase(v) for s, v in aligned.items()}
    market_bm = _make_benchmark(re_rebased)

    result = {}
    for sector in valid:
        key = _SECTOR_KEY_PREFIX + sector

        rs_ratio    = _calc_rs_ratio(re_rebased[sector], market_bm)
        rs_momentum = _calc_rs_momentum(rs_ratio)

        curr_ratio = next((v for v in reversed(rs_ratio)    if v is not None), None)
        curr_mom   = next((v for v in reversed(rs_momentum) if v is not None), None)
        quad       = _quadrant(curr_ratio, curr_mom)

        # ── tail 누적 — 하루 1포인트
        stored_key = rrg_history.get(key)

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
                        "date":        (now_kst() - timedelta(days=offset)).strftime("%Y-%m-%d"),
                        "rs_ratio":    round(t_ratio, 3),
                        "rs_momentum": round(t_mom,   3),
                    })
            rrg_history[key] = tail if tail else []

        if curr_ratio is not None and curr_mom is not None:
            today = now_kst().strftime("%Y-%m-%d")
            last = rrg_history[key][-1] if rrg_history[key] else {}
            if last.get("date") != today:
                rrg_history[key].append({
                    "date":        today,
                    "rs_ratio":    round(curr_ratio, 3),
                    "rs_momentum": round(curr_mom,   3),
                })
                rrg_history[key] = rrg_history[key][-TAIL_DAYS:]

        # date 제거 후 프론트엔드 출력
        clean_tail = [
            {"rs_ratio": pt["rs_ratio"], "rs_momentum": pt["rs_momentum"]}
            for pt in rrg_history.get(key, [])
            if pt.get("rs_ratio") is not None
        ]

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