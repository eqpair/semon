"""
rrg_backtest.py
---------------
목적: 16가지 RRG 파라미터 조합 중 최적값 탐색
      - RS-Ratio MA 기간: 10, 14, 21, 28일
      - RS-Momentum ROC 기간: 5, 10, 14, 21일
      - Improving 진입 후 5/10/20거래일 수익률 측정

실행: python3 rrg_backtest.py
결과: rrg_backtest_result.csv + 콘솔 요약 출력
"""

import asyncio
import aiohttp
import warnings
import itertools
import csv
from collections import defaultdict
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ── 설정 ──────────────────────────────────────────────────────

COUNT = 500  # 약 2년치

# 테스트할 파라미터 조합
MA_PERIODS  = [10, 14, 21, 28]   # RS-Ratio 이동평균 기간
ROC_PERIODS = [5, 10, 14, 21]    # RS-Momentum ROC 기간

# 수익률 측정 기간
HOLD_DAYS = [5, 10, 20]

SISE_URL = (
    "https://fchart.stock.naver.com/sise.nhn"
    "?symbol={code}&timeframe=day&count={count}&requestType=0"
)

# ── 섹터 정의 (config.py에서 가져옴) ─────────────────────────
try:
    import sys
    sys.path.insert(0, '/home/eq/semon')
    from config import SECTORS
    print(f"config.py 로드 완료: {len(SECTORS)}개 테마")
except ImportError:
    print("config.py를 찾을 수 없어 샘플 섹터로 테스트합니다.")
    SECTORS = {
        "반도체_제조": [
            ("005930", "삼성전자"), ("000660", "SK하이닉스"),
            ("000990", "DB하이텍"), ("108320", "LX세미콘"),
        ],
        "반도체_장비": [
            ("042700", "한미반도체"), ("240810", "원익IPS"),
            ("039030", "이오테크닉스"), ("036930", "주성엔지니어링"),
            ("084370", "유진테크"),
        ],
        "자동차_완성차": [
            ("005380", "현대차"), ("000270", "기아"),
        ],
    }

# ── 데이터 수집 ───────────────────────────────────────────────

async def fetch_closes(session: aiohttp.ClientSession, code: str) -> list[float]:
    url = SISE_URL.format(code=code, count=COUNT)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return []
            text = await resp.text(encoding="euc-kr")
        soup = BeautifulSoup(text, "html.parser")
        closes = []
        for item in soup.find_all("item"):
            parts = item.get("data", "").split("|")
            if len(parts) >= 5:
                try:
                    closes.append(float(parts[4]))
                except ValueError:
                    continue
        return closes
    except Exception:
        return []


async def fetch_all_closes(code_list: list[str]) -> dict[str, list[float]]:
    results = {}
    chunk_size = 20
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk = code_list[i:i + chunk_size]
            tasks = [fetch_closes(session, code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, closes in zip(chunk, responses):
                if closes:
                    results[code] = closes
            if i + chunk_size < len(code_list):
                await asyncio.sleep(0.5)
            done = min(i + chunk_size, len(code_list))
            print(f"\r데이터 수집 중: {done}/{len(code_list)}개", end="", flush=True)
    print()
    return results

# ── RRG 계산 ──────────────────────────────────────────────────

def moving_average(data: list[float], period: int) -> list[float | None]:
    result = [None] * len(data)
    for i in range(period - 1, len(data)):
        result[i] = sum(data[i - period + 1:i + 1]) / period
    return result


def std_dev(data: list[float], period: int) -> list[float | None]:
    result = [None] * len(data)
    for i in range(period - 1, len(data)):
        window = data[i - period + 1:i + 1]
        mean = sum(window) / period
        variance = sum((x - mean) ** 2 for x in window) / period
        result[i] = variance ** 0.5
    return result


def calc_rs_ratio(closes: list[float], benchmark: list[float], ma_period: int) -> list[float | None]:
    """RS-Ratio 계산"""
    n = min(len(closes), len(benchmark))
    # Raw RS
    rs = []
    for i in range(n):
        if benchmark[i] and benchmark[i] != 0:
            rs.append((closes[i] / benchmark[i]) * 100)
        else:
            rs.append(None)

    # None 제거 후 계산
    rs_clean = [v if v is not None else 0 for v in rs]
    rs_ma  = moving_average(rs_clean, ma_period)
    rs_std = std_dev(rs_clean, ma_period)

    ratio = [None] * n
    for i in range(n):
        if rs_ma[i] is not None and rs_std[i] and rs_std[i] > 0:
            ratio[i] = 100 + ((rs_clean[i] - rs_ma[i]) / rs_std[i])
    return ratio


def calc_rs_momentum(rs_ratio: list[float | None], roc_period: int) -> list[float | None]:
    """RS-Momentum 계산"""
    n = len(rs_ratio)
    roc = [None] * n
    for i in range(roc_period, n):
        if rs_ratio[i] is not None and rs_ratio[i - roc_period] is not None and rs_ratio[i - roc_period] != 0:
            roc[i] = (rs_ratio[i] / rs_ratio[i - roc_period] - 1) * 100

    roc_clean = [v if v is not None else 0 for v in roc]
    roc_ma  = moving_average(roc_clean, roc_period)
    roc_std = std_dev(roc_clean, roc_period)

    momentum = [None] * n
    for i in range(n):
        if roc_ma[i] is not None and roc_std[i] and roc_std[i] > 0:
            momentum[i] = 100 + ((roc_clean[i] - roc_ma[i]) / roc_std[i])
    return momentum


def get_quadrant(ratio: float | None, momentum: float | None) -> str | None:
    if ratio is None or momentum is None:
        return None
    if ratio >= 100 and momentum >= 100:
        return "leading"
    elif ratio >= 100 and momentum < 100:
        return "weakening"
    elif ratio < 100 and momentum < 100:
        return "lagging"
    else:
        return "improving"


def find_improving_entries(
    ratio: list[float | None],
    momentum: list[float | None]
) -> list[int]:
    """Lagging → Improving 전환 인덱스 반환"""
    entries = []
    prev_q = None
    for i in range(len(ratio)):
        curr_q = get_quadrant(ratio[i], momentum[i])
        if prev_q == "lagging" and curr_q == "improving":
            entries.append(i)
        if curr_q is not None:
            prev_q = curr_q
    return entries

# ── 백테스트 ──────────────────────────────────────────────────

def backtest_sector(
    sector: str,
    codes: list[tuple[str, str]],
    closes_db: dict[str, list[float]],
    ma_period: int,
    roc_period: int,
) -> list[dict]:
    """
    하나의 섹터, 하나의 파라미터 조합에 대해 백테스트
    반환: [{code, entry_idx, ret_5, ret_10, ret_20}, ...]
    """
    code_list = [c for c, _ in codes]

    # 유효한 종목만
    valid = {c: closes_db[c] for c in code_list if c in closes_db and len(closes_db[c]) > ma_period * 2 + roc_period + 20}
    if len(valid) < 3:
        return []

    # 최소 길이 맞추기
    min_len = min(len(v) for v in valid.values())

    # 섹터 평균 종가 (벤치마크)
    benchmark = []
    for i in range(min_len):
        avg = sum(valid[c][-(min_len - i)] for c in valid) / len(valid)
        benchmark.append(avg)

    results = []
    for code in valid:
        closes = valid[code][-min_len:]  # 길이 맞추기

        ratio    = calc_rs_ratio(closes, benchmark, ma_period)
        momentum = calc_rs_momentum(ratio, roc_period)
        entries  = find_improving_entries(ratio, momentum)

        for idx in entries:
            entry_price = closes[idx]
            ret = {}
            for hold in HOLD_DAYS:
                future_idx = idx + hold
                if future_idx < len(closes) and entry_price > 0:
                    ret[hold] = (closes[future_idx] - entry_price) / entry_price * 100
                else:
                    ret[hold] = None

            # 테마 평균 수익률 (알파 계산용)
            alpha = {}
            for hold in HOLD_DAYS:
                future_idx = idx + hold
                if future_idx < len(benchmark) and benchmark[idx] > 0:
                    bm_ret = (benchmark[future_idx] - benchmark[idx]) / benchmark[idx] * 100
                    if ret[hold] is not None:
                        alpha[hold] = ret[hold] - bm_ret
                    else:
                        alpha[hold] = None
                else:
                    alpha[hold] = None

            results.append({
                "sector":   sector,
                "code":     code,
                "entry_idx": idx,
                "ret_5":    ret.get(5),
                "ret_10":   ret.get(10),
                "ret_20":   ret.get(20),
                "alpha_5":  alpha.get(5),
                "alpha_10": alpha.get(10),
                "alpha_20": alpha.get(20),
            })

    return results


def summarize(results: list[dict]) -> dict:
    """파라미터 조합의 전체 성과 요약"""
    if not results:
        return {}

    summary = {}
    for hold in HOLD_DAYS:
        key = f"ret_{hold}"
        akey = f"alpha_{hold}"
        vals   = [r[key]  for r in results if r[key]  is not None]
        avals  = [r[akey] for r in results if r[akey] is not None]
        if vals:
            summary[f"신호수"]            = len(results)
            summary[f"승률_{hold}d"]      = round(sum(1 for v in vals if v > 0) / len(vals) * 100, 1)
            summary[f"평균수익_{hold}d"]  = round(sum(vals) / len(vals), 2)
            summary[f"평균알파_{hold}d"]  = round(sum(avals) / len(avals), 2) if avals else None
    return summary

# ── 메인 ──────────────────────────────────────────────────────

async def main():
    # 전체 종목 코드
    all_codes = list({code for codes in SECTORS.values() for code, _ in codes})
    print(f"총 {len(all_codes)}개 종목, {COUNT}일치 데이터 수집 시작...")

    closes_db = await fetch_all_closes(all_codes)
    print(f"수집 완료: {len(closes_db)}/{len(all_codes)}개 성공\n")

    # 파라미터 조합 테스트
    combos = list(itertools.product(MA_PERIODS, ROC_PERIODS))
    all_results = []

    print(f"파라미터 조합 {len(combos)}가지 백테스트 시작...\n")

    combo_summaries = []

    for ma, roc in combos:
        combo_results = []
        for sector, codes in SECTORS.items():
            r = backtest_sector(sector, codes, closes_db, ma, roc)
            combo_results.extend(r)

        summary = summarize(combo_results)
        if summary:
            row = {"MA기간": ma, "ROC기간": roc, **summary}
            combo_summaries.append(row)
            all_results.extend([{**r, "MA기간": ma, "ROC기간": roc} for r in combo_results])
            print(
                f"MA={ma:2d}, ROC={roc:2d} | "
                f"신호:{summary.get('신호수', 0):4d}개 | "
                f"승률(20d):{summary.get('승률_20d', '-'):5.1f}% | "
                f"평균수익(20d):{summary.get('평균수익_20d', '-'):6.2f}% | "
                f"알파(20d):{summary.get('평균알파_20d', '-'):6.2f}%"
            )

    # 최적 파라미터 출력
    print("\n" + "="*60)
    print("★ 최적 파라미터 (20일 알파 기준)")
    print("="*60)

    best = sorted(
        [s for s in combo_summaries if s.get("평균알파_20d") is not None],
        key=lambda x: x["평균알파_20d"],
        reverse=True
    )

    for i, b in enumerate(best[:5]):
        print(
            f"  {i+1}위: MA={b['MA기간']:2d}일, ROC={b['ROC기간']:2d}일 | "
            f"승률(20d)={b.get('승률_20d','-')}% | "
            f"평균수익(20d)={b.get('평균수익_20d','-')}% | "
            f"알파(20d)={b.get('평균알파_20d','-')}%"
        )

    print("\n★ 최적 파라미터 (5일 알파 기준 — 단기 스윙)")
    best5 = sorted(
        [s for s in combo_summaries if s.get("평균알파_5d") is not None],
        key=lambda x: x["평균알파_5d"],
        reverse=True
    )
    for i, b in enumerate(best5[:5]):
        print(
            f"  {i+1}위: MA={b['MA기간']:2d}일, ROC={b['ROC기간']:2d}일 | "
            f"승률(5d)={b.get('승률_5d','-')}% | "
            f"평균수익(5d)={b.get('평균수익_5d','-')}% | "
            f"알파(5d)={b.get('평균알파_5d','-')}%"
        )

    # CSV 저장
    if combo_summaries:
        with open("rrg_backtest_result.csv", "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=combo_summaries[0].keys())
            writer.writeheader()
            writer.writerows(combo_summaries)
        print("\n결과 저장: rrg_backtest_result.csv")


if __name__ == "__main__":
    asyncio.run(main())