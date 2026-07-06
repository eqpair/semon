"""
backtest_factors.py — 전수 팩터 백테스트

가진 모든 팩터를 이진 신호로 변환 → 단독/2조합/3조합 전수 →
보유기간(3·5·10·20일) × 판정기준(엣지·상승확률·샤프) 전부 측정.

목적: "어느 팩터(조합)가 실제로 종목을 잘 뽑는가"를 데이터로 확정.

look-ahead 없음: 시점 t 신호는 t까지 데이터만.

산출:
  factor_records.parquet    — 시점별 팩터+미래수익 (재분석용)
  factor_ranking.txt        — 단독/조합 랭킹 (기준별)

실행:
  python3 backtest_factors.py
"""
import asyncio
import json
import logging
import numpy as np
import pandas as pd
import aiohttp
from itertools import combinations
from bs4 import BeautifulSoup
import warnings
from bs4 import XMLParsedAsHTMLWarning
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MA_SHORT, MA_LONG = 10, 40
MIN_HISTORY = MA_LONG * 2 + 5
HORIZONS = [3, 5, 10, 20]
MW_KOSPI, MW_KOSDAQ = 0.85, 0.15
SISE = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=500&requestType=0"
MIN_SIGNAL = 200   # 조합 최소 신호수 (이하면 통계 무의미)


def _ma(v, p):
    out = [None] * len(v)
    for i in range(p - 1, len(v)):
        w = v[i-p+1:i+1]
        if all(x is not None for x in w):
            out[i] = sum(w) / p
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
        closes, vols = [], []
        for it in soup.find_all("item"):
            p = it.get("data", "").split("|")
            if len(p) >= 6:
                try:
                    closes.append(float(p[4])); vols.append(float(p[5]))
                except ValueError:
                    continue
        return code, ({"closes": closes, "vols": vols} if closes else None)
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


def calc_rrg(closes, bench):
    n = len(closes)
    reb = _rebase(closes)
    rs = [reb[i]/bench[i]*100 if bench[i] else 0 for i in range(n)]
    ma_s, ma_l = _ma(rs, MA_SHORT), _ma(rs, MA_LONG)
    rr = [100*ma_s[i]/ma_l[i] if ma_s[i] and ma_l[i] else None for i in range(n)]
    vi = [i for i, v in enumerate(rr) if v is not None]
    vv = [rr[i] for i in vi]
    ma_rr = _ma(vv, MA_LONG)
    rm = [None]*n
    for j, oi in enumerate(vi):
        if ma_rr[j]:
            rm[oi] = 100*vv[j]/ma_rr[j]
    return rr, rm


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
        logger.error("지수 수집 실패"); return

    recs = []
    for code in codes:
        d = data.get(code)
        if not d or len(d["closes"]) < MIN_HISTORY + max(HORIZONS) + 5:
            continue
        closes, vols = d["closes"], d["vols"]
        L = min(len(closes), len(kospi["closes"]), len(kosdaq["closes"]))
        closes, vols = closes[-L:], vols[-L:]
        kp, kq = _rebase(kospi["closes"][-L:]), _rebase(kosdaq["closes"][-L:])
        bench = [kp[i]*MW_KOSPI + kq[i]*MW_KOSDAQ for i in range(L)]
        rr, rm = calc_rrg(closes, bench)

        for t in range(MIN_HISTORY, L - max(HORIZONS)):
            cr, cm = rr[t], rm[t]
            if cr is None or cm is None:
                continue
            ret_1d = (closes[t]-closes[t-1])/closes[t-1] if closes[t-1] else 0
            ret_5d = (closes[t]-closes[t-5])/closes[t-5] if closes[t-5] else 0
            avg_vol = np.mean(vols[max(0,t-22):t-2]) if t >= 22 else (np.mean(vols[:t]) if t > 0 else 0)
            vol_ratio = vols[t]/avg_vol if avg_vol > 0 else 0

            # ── 사분면 ──
            if cr >= 100 and cm >= 100: quad = "leading"
            elif cr >= 100 and cm < 100: quad = "weakening"
            elif cr < 100 and cm < 100: quad = "lagging"
            else: quad = "improving"

            # ── 팩터 (이진) ──
            m3 = [rm[t-2], rm[t-1], rm[t]]
            r3 = [rr[t-2], rr[t-1], rr[t]]
            mom_ok = all(x is not None for x in m3)
            rr_ok = all(x is not None for x in r3)

            f = {
                "improving": quad == "improving",
                "leading":   quad == "leading",
                "lagging":   quad == "lagging",
                "weakening": quad == "weakening",
                "laggard":   quad in ("lagging", "improving"),
                "A_vol_lead": vol_ratio >= 1.5 and abs(ret_1d) < 0.02,
                "vol_high":   vol_ratio >= 1.5,
                "C_accel":    mom_ok and m3[2] > m3[1] > m3[0] and (m3[2]-m3[1]) > (m3[1]-m3[0]),
                "mom_rising": mom_ok and m3[2] > m3[1] > m3[0],
                "B_traj":     rr_ok and r3[2] > r3[1] > r3[0] and r3[2] < 100,
                "rs_low":     cr < 95,               # 깊은 소외
                "rs_mid":     95 <= cr < 100,        # 소외 후반
                "ret5d_neg":  ret_5d < 0,            # 최근 5일 하락(과매도)
                "ret5d_pos":  ret_5d > 0,
            }
            fut = {f"r{h}": (closes[t+h]-closes[t])/closes[t] if closes[t] else 0 for h in HORIZONS}
            recs.append({**f, **fut})

    df = pd.DataFrame(recs)
    df.to_parquet("factor_records.parquet")
    logger.info(f"총 관측: {len(df)}개, 팩터 {sum(df.dtypes==bool)}개")

    factor_cols = [c for c in df.columns if not c.startswith("r") or c in ("rs_low","rs_mid")]
    factor_cols = [c for c in df.columns if df[c].dtype == bool]

    lines = []
    def stats(sub, base_df, h):
        if len(sub) < MIN_SIGNAL:
            return None
        m = sub[f"r{h}"].mean()
        base = base_df[f"r{h}"].mean()
        sd = sub[f"r{h}"].std()
        win = (sub[f"r{h}"] > 0).mean()
        sharpe = (m - base) / sd if sd > 0 else 0
        return {"n": len(sub), "mean": m, "edge": m-base, "win": win, "sharpe": sharpe}

    # ── 단독 팩터 ──
    lines.append("="*72)
    lines.append("[단독 팩터] 5일 보유 기준 엣지 순 (신호수>=200)")
    lines.append("="*72)
    single = []
    for fc in factor_cols:
        s = stats(df[df[fc]], df, 5)
        if s:
            single.append((fc, s))
    single.sort(key=lambda x: -x[1]["edge"])
    for fc, s in single:
        lines.append(f"  {fc:<12} n={s['n']:>6} | edge {s['edge']*100:+.2f}%p | 상승 {s['win']*100:.1f}% | sharpe {s['sharpe']:+.3f}")

    # ── 2조합 ──
    lines.append("\n" + "="*72)
    lines.append("[2팩터 조합] 5일 엣지 상위 20 (신호수>=200)")
    lines.append("="*72)
    combos2 = []
    for a, b in combinations(factor_cols, 2):
        sub = df[df[a] & df[b]]
        s = stats(sub, df, 5)
        if s:
            combos2.append((f"{a}+{b}", s))
    combos2.sort(key=lambda x: -x[1]["edge"])
    for name, s in combos2[:20]:
        lines.append(f"  {name:<28} n={s['n']:>5} | edge {s['edge']*100:+.2f}%p | 상승 {s['win']*100:.1f}% | sharpe {s['sharpe']:+.3f}")

    # ── 3조합 ──
    lines.append("\n" + "="*72)
    lines.append("[3팩터 조합] 5일 엣지 상위 20 (신호수>=200)")
    lines.append("="*72)
    combos3 = []
    for a, b, c in combinations(factor_cols, 3):
        sub = df[df[a] & df[b] & df[c]]
        s = stats(sub, df, 5)
        if s:
            combos3.append((f"{a}+{b}+{c}", s))
    combos3.sort(key=lambda x: -x[1]["edge"])
    for name, s in combos3[:20]:
        lines.append(f"  {name:<40} n={s['n']:>5} | edge {s['edge']*100:+.2f}%p | 상승 {s['win']*100:.1f}% | sharpe {s['sharpe']:+.3f}")

    # ── 최우수 조합의 보유기간별 ──
    lines.append("\n" + "="*72)
    lines.append("[상위 3조합] 보유기간별(3·5·10·20일) 엣지")
    lines.append("="*72)
    for name, _ in combos3[:5]:
        parts = name.split("+")
        mask = df[parts[0]]
        for p in parts[1:]:
            mask = mask & df[p]
        sub = df[mask]
        lines.append(f"\n  [{name}] n={len(sub)}")
        for h in HORIZONS:
            s = stats(sub, df, h)
            if s:
                lines.append(f"    {h:>2}일: edge {s['edge']*100:+.2f}%p | 상승 {s['win']*100:.1f}% | sharpe {s['sharpe']:+.3f}")

    txt = "\n".join(lines)
    open("factor_ranking.txt", "w", encoding="utf-8").write(txt)
    print(txt)
    print(f"\n저장: factor_records.parquet ({len(df)}시점), factor_ranking.txt")


if __name__ == "__main__":
    main()