"""
backtest_early.py — 선행 신호(A거래량·B궤적·C가속도) 백테스트

목적: 기존 breakout(후행) 대체할 선행 신호의 선행성 검증.
      "신호 켜진 날 이후 N일 수익률이 무작위보다 높은가?"

방법 (look-ahead 방지):
  각 과거 시점 t에서 t까지의 데이터로만 지표 계산 → 신호 판정
  → t 이후 N일(3·5·10) 실제 수익률 측정 → 신호일 vs 전체 비교

지표 (sector_signal.py 공식 재현):
  RS_Ratio    = 100 × MA10(RS) / MA40(RS),  RS = rebased/benchmark×100
  RS_Momentum = 100 × RS_Ratio / MA40(RS_Ratio)
  benchmark   = KOSPI+KOSDAQ 85:15 rebased

  A 거래량선행: vol_ratio≥1.5 AND |ret_1d|<2%
  C 가속도:     RS_Mom 3점 연속상승 AND 2차미분>0
  B 궤적전환:   RS_Ratio 3점 우상향 AND <100

실행:
  python3 backtest_early.py
"""
import asyncio
import json
import logging
import numpy as np
import pandas as pd
import aiohttp
from bs4 import BeautifulSoup
from collections import defaultdict
import warnings
from bs4 import XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MA_SHORT, MA_LONG = 10, 40
MIN_HISTORY = MA_LONG * 2 + 5   # RS_Momentum 계산 최소
HORIZONS = [3, 5, 10]
SISE = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=500&requestType=0"
MW_KOSPI, MW_KOSDAQ = 0.85, 0.15


def _ma(v, p):
    out = [None] * len(v)
    for i in range(p - 1, len(v)):
        window = v[i - p + 1:i + 1]
        if all(x is not None for x in window):
            out[i] = sum(window) / p
    return out


def _rebase(c):
    return [x / c[0] * 100 for x in c] if c[0] else [0] * len(c)


async def _fetch(session, code):
    try:
        async with session.get(SISE.format(code=code), timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return code, None
            text = await r.text(encoding="euc-kr")
        soup = BeautifulSoup(text, "html.parser")
        dates, closes, vols = [], [], []
        for it in soup.find_all("item"):
            p = it.get("data", "").split("|")
            if len(p) >= 6:
                try:
                    dates.append(p[0]); closes.append(float(p[4])); vols.append(float(p[5]))
                except ValueError:
                    continue
        return code, ({"dates": dates, "closes": closes, "vols": vols} if closes else None)
    except Exception:
        return code, None


async def fetch_all(codes):
    res = {}
    async with aiohttp.ClientSession() as s:
        for i in range(0, len(codes), 10):
            for code, d in await asyncio.gather(*[_fetch(s, c) for c in codes[i:i+10]]):
                res[code] = d
            if i + 10 < len(codes):
                await asyncio.sleep(1.0)
    return res


def calc_rrg_series(closes, benchmark):
    """전체 종가에 대해 매 시점 RS_Ratio, RS_Momentum 시계열 반환 (look-ahead 없음:
    각 값은 그 시점까지의 데이터만 사용)."""
    n = len(closes)
    reb = _rebase(closes)
    rs = [reb[i] / benchmark[i] * 100 if benchmark[i] else 0 for i in range(n)]
    ma_s = _ma(rs, MA_SHORT)
    ma_l = _ma(rs, MA_LONG)
    rs_ratio = [100 * ma_s[i] / ma_l[i] if ma_s[i] and ma_l[i] else None for i in range(n)]
    # RS_Momentum = 100 × rs_ratio / MA40(rs_ratio)
    valid_idx = [i for i, v in enumerate(rs_ratio) if v is not None]
    valid_val = [rs_ratio[i] for i in valid_idx]
    ma_rr = _ma(valid_val, MA_LONG)
    rs_mom = [None] * n
    for j, oi in enumerate(valid_idx):
        if ma_rr[j]:
            rs_mom[oi] = 100 * valid_val[j] / ma_rr[j]
    return rs_ratio, rs_mom


def main():
    name_of = {}
    for sec, lst in config.SECTORS.items():
        if sec == "미분류":
            continue
        for code, name in lst:
            name_of[code] = name
    codes = list(name_of.keys())
    logger.info(f"종목 {len(codes)}개 + 지수 수집")

    data = asyncio.run(fetch_all(codes))
    idx = asyncio.run(fetch_all(["KOSPI", "KOSDAQ"]))
    kospi, kosdaq = idx.get("KOSPI"), idx.get("KOSDAQ")
    if not kospi or not kosdaq:
        logger.error("지수 수집 실패")
        return

    # 시장 벤치마크: 공통 최소 길이로 정렬 후 rebase 합성
    # (날짜 매칭은 단순화 — 종목마다 길이 다르면 뒤에서 맞춤)
    records = []   # (signal_flags, future_returns)
    A_hits, C_hits, B_hits = [], [], []

    for code in codes:
        d = data.get(code)
        if not d or len(d["closes"]) < MIN_HISTORY + max(HORIZONS) + 5:
            continue
        closes = d["closes"]; vols = d["vols"]
        L = min(len(closes), len(kospi["closes"]), len(kosdaq["closes"]))
        closes, vols = closes[-L:], vols[-L:]
        kp = _rebase(kospi["closes"][-L:])
        kq = _rebase(kosdaq["closes"][-L:])
        bench = [kp[i] * MW_KOSPI + kq[i] * MW_KOSDAQ for i in range(L)]

        rs_ratio, rs_mom = calc_rrg_series(closes, bench)

        # 매 시점 t에서 신호 판정 (t 이후 horizon 있는 구간만)
        for t in range(MIN_HISTORY, L - max(HORIZONS)):
            # ret_1d, vol_ratio (t 시점)
            ret_1d = (closes[t] - closes[t-1]) / closes[t-1] if closes[t-1] else 0
            avg_vol = np.mean(vols[max(0,t-22):t-2]) if t >= 22 else np.mean(vols[:t]) if t > 0 else 0
            vol_ratio = vols[t] / avg_vol if avg_vol > 0 else 0

            # A: 거래량 선행
            A = (vol_ratio >= 1.5 and abs(ret_1d) < 0.02)
            # C: 가속도 (RS_Mom 3점 연속상승 + 2차미분>0)
            m = [rs_mom[t-2], rs_mom[t-1], rs_mom[t]]
            C = (all(x is not None for x in m)
                 and m[2] > m[1] > m[0]
                 and (m[2]-m[1]) > (m[1]-m[0]))
            # B: 궤적전환 (RS_Ratio 3점 우상향 + <100)
            rr = [rs_ratio[t-2], rs_ratio[t-1], rs_ratio[t]]
            B = (all(x is not None for x in rr)
                 and rr[2] > rr[1] > rr[0]
                 and rr[2] < 100)

            # 사분면 (t 시점)
            cr = rs_ratio[t]; cm = rs_mom[t]
            if cr is None or cm is None:
                continue
            if cr >= 100 and cm >= 100: quad = "leading"
            elif cr >= 100 and cm < 100: quad = "weakening"
            elif cr < 100 and cm < 100: quad = "lagging"
            else: quad = "improving"
            is_laggard = quad in ("lagging", "improving")  # 소외구간

            # 기존 breakout 로직 재현 (5일 rs_5d 방식)
            # ret_5d, sector 대비는 단순화 — 종목 5일수익률 자체로 근사
            ret_5d = (closes[t] - closes[t-5]) / closes[t-5] if t >= 5 and closes[t-5] else 0
            # 기존 breakout: lagging/improving + 5일 강세 + 절대상승 (섹터대비는 생략, 절대기준)
            old_breakout = (is_laggard and ret_5d > 0.05)  # 5일 5%+ 상승 (섹터대비 근사)

            # 미래 수익률
            futures = {h: (closes[t+h] - closes[t]) / closes[t] if closes[t] else 0
                       for h in HORIZONS}

            records.append({"A": A, "B": B, "C": C,
                            "laggard": is_laggard, "old_bo": old_breakout,
                            **{f"r{h}": futures[h] for h in HORIZONS}})

    df = pd.DataFrame(records)
    logger.info(f"총 관측: {len(df)}개 시점")

    # ── 기준선(전체 평균) ──
    print("\n" + "=" * 60)
    print(f"백테스트 결과 (총 {len(df)}개 시점)")
    print("=" * 60)
    print("\n[기준선] 전체 시점 평균 수익률 (무작위 진입)")
    for h in HORIZONS:
        base = df[f"r{h}"].mean()
        winr = (df[f"r{h}"] > 0).mean()
        print(f"  {h}일 후: 평균 {base*100:+.2f}% | 상승비율 {winr*100:.1f}%")

    # ── 각 지표 단독 선행성 ──
    for sig in ["A", "C", "B"]:
        sub = df[df[sig]]
        print(f"\n[{sig} 단독] 신호 {len(sub)}건 ({len(sub)/len(df)*100:.1f}%)")
        for h in HORIZONS:
            m = sub[f"r{h}"].mean(); w = (sub[f"r{h}"] > 0).mean()
            base = df[f"r{h}"].mean()
            edge = (m - base) * 100
            print(f"  {h}일 후: 평균 {m*100:+.2f}% (기준比 {edge:+.2f}%p) | 상승 {w*100:.1f}%")

    # ── 조합: 가중점수 스윕 ──
    # A=0.5, C=0.3, B=0.2 가중, 임계값 스윕
    df["score"] = df["A"]*0.5 + df["C"]*0.3 + df["B"]*0.2
    print("\n" + "=" * 60)
    print("[가중조합] score = A×0.5 + C×0.3 + B×0.2, 임계값 스윕")
    print("=" * 60)
    for thr in [0.2, 0.3, 0.5, 0.7, 0.8]:
        sub = df[df["score"] >= thr]
        if len(sub) < 20:
            print(f"  임계 {thr}: 신호 {len(sub)}건 (너무 적음)")
            continue
        print(f"\n  임계 {thr}: 신호 {len(sub)}건 ({len(sub)/len(df)*100:.1f}%)")
        for h in HORIZONS:
            m = sub[f"r{h}"].mean(); w = (sub[f"r{h}"] > 0).mean()
            base = df[f"r{h}"].mean()
            print(f"    {h}일: 평균 {m*100:+.2f}% (기준比 {(m-base)*100:+.2f}%p) | 상승 {w*100:.1f}%")

    # ── 소외구간 한정 분석 ──
    print("\n" + "=" * 60)
    print("[소외구간(lagging/improving) 한정] 각 신호 선행성")
    print("=" * 60)
    lag = df[df["laggard"]]
    print(f"\n소외구간 시점: {len(lag)}개 ({len(lag)/len(df)*100:.1f}%)")
    print("\n[소외구간 기준선]")
    for h in HORIZONS:
        print(f"  {h}일: 평균 {lag[f'r{h}'].mean()*100:+.2f}% | 상승 {(lag[f'r{h}']>0).mean()*100:.1f}%")
    for sig in ["A", "C", "B"]:
        sub = lag[lag[sig]]
        if len(sub) < 20: continue
        print(f"\n[소외+{sig}] 신호 {len(sub)}건")
        for h in HORIZONS:
            m=sub[f'r{h}'].mean(); w=(sub[f'r{h}']>0).mean(); base=lag[f'r{h}'].mean()
            print(f"  {h}일: 평균 {m*100:+.2f}% (소외比 {(m-base)*100:+.2f}%p) | 상승 {w*100:.1f}%")
    # 소외 + A&C 동시
    both = lag[lag["A"] & lag["C"]]
    if len(both) >= 20:
        print(f"\n[소외+A&C 동시] 신호 {len(both)}건")
        for h in HORIZONS:
            m=both[f'r{h}'].mean(); w=(both[f'r{h}']>0).mean(); base=lag[f'r{h}'].mean()
            print(f"  {h}일: 평균 {m*100:+.2f}% (소외比 {(m-base)*100:+.2f}%p) | 상승 {w*100:.1f}%")

    # ── 기존 breakout 검증 ──
    print("\n" + "=" * 60)
    print("[기존 breakout] (소외구간 + 5일 5%+ 상승, 섹터대비 근사)")
    print("=" * 60)
    obo = df[df["old_bo"]]
    print(f"신호 {len(obo)}건 ({len(obo)/len(df)*100:.1f}%)")
    for h in HORIZONS:
        m=obo[f'r{h}'].mean(); w=(obo[f'r{h}']>0).mean(); base=df[f'r{h}'].mean()
        print(f"  {h}일: 평균 {m*100:+.2f}% (기준比 {(m-base)*100:+.2f}%p) | 상승 {w*100:.1f}%")

    df.to_parquet("backtest_v2_records.parquet")
    print("\n저장: backtest_v2_records.parquet")


if __name__ == "__main__":
    main()