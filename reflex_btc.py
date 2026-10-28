#!/usr/bin/env python3
"""
reflex_btc.py — 비트코인 재귀성 9단계: 형세(F)·기세(M) 판정 → docs/data/reflex_btc.json

입력: data/reflex_btc/{macro,onchain,stable,funding,alts}.parquet  (reflex_btc_data.py 산출물)
출력: docs/data/reflex_btc.json

주식판(reflex.py v2)과 같은 판정 엔진, 비트코인용 재료와 시간축:
  - UTC 일봉, 진행 중인 오늘(UTC) 봉 제외. 백분위 창 3년(1,095일)
  - 형세: 연준 순유동성·실질금리·달러, 나스닥·VIX, 스테이블코인 증가, 해시레이트·활성 주소
  - 기세: 이평선 이격·120일 수익률, 펀딩비, MVRV, 알트 이평선 위 비율, 방향성 거래량
  - 2015~2026 백테스트로 설정 (평활 30, 기세 기울기 21일, 형세 기울기 90일, 임계 12,
    5일 확인 + 21일 체류, 상승 기세는 형세 40 이상일 때만 — 약세장 반등 오판 방지)

국면 설명 지표이며 매매 신호가 아님. 완전한 사이클이 3~4번뿐이라 통계 표본이 작음.

사용법:
  python reflex_btc.py            # 계산 + JSON 저장
  python reflex_btc.py --push     # 저장 후 git push
"""
import json
import argparse
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
DATA = BASE / "data" / "reflex_btc"
OUT_JSON = BASE / "docs" / "data" / "reflex_btc.json"
VERSION = "reflex_btc_v1"

PCT_WIN, PCT_MIN = 1095, 365
HORIZON = 90
P = dict(smooth=30, fast_smooth=5, m_slope=21, f_slope=90, fast_slope=10, th=12,
         confirm=5, dwell=21, crash_level=15, crash_drop=35, crash_confirm=3, up2_min_f=40)
HISTORY_DAYS = 730
SPLIT = "2020-01-01"

STAGES = {
    1: ("초기 회복", "유동성·거시 여건 개선이 먼저 시작 — 가격은 아직 미반영"),
    2: ("상승 가속", "Momentum 반응 시작"),
    3: ("확인", "Fundamentals 개선에 Momentum 동반"),
    4: ("확산", "높은 Momentum 유지, 상승 종목 확산"),
    5: ("과열", "Momentum ≫ Fundamentals — 기대가 가격을 끌어올리는 구간"),
    6: ("반전", "고점에서 Momentum 꺾임 시작"),
    7: ("둔화", "Momentum이 중립 수준으로 둔화"),
    8: ("악화", "Fundamentals 약화, Momentum 부진"),
    9: ("투매", "Momentum 급락 — 매도가 매도를 부르는 구간"),
}
UP_PHASE, DOWN_PHASE = (2, 3, 4, 5), (6, 7, 8, 9)

COMP = {
    "M": {"trend": ("추세", 0.35), "lev": ("레버리지", 0.15), "val": ("과열도", 0.15),
          "breadth": ("알트 시장 폭", 0.20), "energy": ("거래량", 0.15)},
    "Mf": {"trend": ("추세", 0.35), "breadth": ("알트 참여", 0.25), "lev": ("레버리지", 0.15),
           "energy": ("거래량", 0.25)},
    "F": {"liq": ("달러 유동성", 0.30), "risk": ("위험선호", 0.20), "stable": ("스테이블코인", 0.30),
          "net": ("네트워크", 0.20)},
}


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


# ── 데이터 ───────────────────────────────────────────────────────────
def load():
    rd = lambda n: pd.read_parquet(DATA / f"{n}.parquet").sort_index()
    m, o = rd("macro"), rd("onchain")
    st = rd("stable")["stable_usd"]
    fu = rd("funding")["funding_day"]
    al = rd("alts")

    today_utc = pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()
    idx = pd.date_range("2013-01-01", today_utc - pd.Timedelta(days=1), freq="D")

    yb, cap = m["BTC"].dropna(), o["CapMrktCurUSD"].dropna()
    if "BTC" in al.columns:          # yfinance 누락일은 Binance BTCUSDT 일봉 종가로 보충
        yb = yb.combine_first(al["BTC"].dropna())
        al = al.drop(columns=["BTC"])
    yb = yb[yb.index < today_utc]
    ratio = (cap / yb).dropna().iloc[:30].median()      # 2014-09 이전은 시가총액으로 연장
    px = yb.reindex(idx)
    px = px.fillna((cap / ratio).reindex(idx).where(px.index < yb.index[0])).ffill()
    idx = idx[idx <= yb.index[-1]]
    px = px.reindex(idx)

    def ff(s, lag=0):
        s = s.dropna()
        return s.reindex(s.index.union(idx)).ffill().reindex(idx).shift(lag)

    D = {
        "px": px, "vol": ff(m["BTC거래량"]), "nas": ff(m["나스닥"]), "vix": ff(m["VIX"]),
        "dxy": ff(m["달러인덱스"]), "ry": ff(m["실질금리10Y"], 2),
        "netliq": ff(m["연준자산"], 2) / 1e3 - ff(m["TGA"], 2) / 1e3 - ff(m["역레포"], 2),
        "mvrv": ff(o["CapMVRVCur"]), "hr": ff(o["HashRate"]), "adr": ff(o["AdrActCnt"]),
        "stab": ff(st), "fund": fu.reindex(idx), "alts": al.reindex(idx),
    }
    asof = {
        "btc": yb.index[-1], "onchain": o["CapMVRVCur"].last_valid_index(),
        "macro": m["나스닥"].last_valid_index(), "funding": fu.index[-1],
    }
    return idx, D, asof


def rolling_pct(s):
    s = s.astype(float)

    def f(x):
        last = x[-1]
        if np.isnan(last):
            return np.nan
        v = x[~np.isnan(x)]
        if len(v) < PCT_MIN:
            return np.nan
        return ((v < last).sum() + 0.5 * ((v == last).sum() - 1)) / (len(v) - 1) * 100

    return s.rolling(PCT_WIN, min_periods=1).apply(f, raw=True)


def wmean(df, weights, min_w=0.5):
    W = pd.DataFrame({c: np.where(df[c].notna(), weights[c], 0.0) for c in df.columns}, index=df.index)
    den = W.sum(axis=1)
    return ((df.fillna(0) * W).sum(axis=1) / den).where(den >= min_w)


def build(idx, D):
    ma = lambda s, w: s.rolling(w, min_periods=w).mean()
    px, vol, alts = D["px"], D["vol"], D["alts"]
    lr = np.log(px).diff()

    def above(w):
        mm = alts.rolling(w, min_periods=w).mean()
        a = (alts > mm).astype(float).where(mm.notna() & alts.notna())
        c = a.notna().sum(axis=1)
        return (a.sum(axis=1) / c.replace(0, np.nan)).where(c >= 5) * 100

    X = {}
    X["t_ma60"] = px / ma(px, 60) - 1
    X["t_ma120"] = px / ma(px, 120) - 1
    X["t_ma200"] = px / ma(px, 200) - 1
    X["t_r120"] = px.pct_change(120, fill_method=None)
    X["l_fund30"] = D["fund"].rolling(30, min_periods=20).mean()
    X["v_mvrv"] = D["mvrv"]
    X["b_a200"] = above(200)
    X["b_a50"] = above(50)
    X["e_up60"] = (vol * (lr > 0)).rolling(60).sum() / vol.rolling(60).sum()
    X["e_v60"] = ma(vol, 60) / vol.rolling(365, min_periods=180).mean()
    X["e_dir60"] = (X["e_up60"] - 0.5) * X["e_v60"]
    X["f_ma20"] = px / ma(px, 20) - 1
    X["f_r20"] = px.pct_change(20, fill_method=None)
    X["f_a20"] = above(20)
    X["f_fund7"] = D["fund"].rolling(7, min_periods=5).mean()
    X["f_up10"] = (vol * (lr > 0)).rolling(10).sum() / vol.rolling(10).sum()
    X["f_v10"] = ma(vol, 10) / vol.rolling(120, min_periods=60).mean()
    X["f_dir10"] = (X["f_up10"] - 0.5) * X["f_v10"]
    X["q_netliq"] = D["netliq"].pct_change(60, fill_method=None)
    X["q_netliq_lvl"] = D["netliq"]
    X["q_ry"] = D["ry"].diff(60)
    X["q_ry_lvl"] = D["ry"]
    X["q_dxy"] = D["dxy"].pct_change(60, fill_method=None)
    X["r_nas"] = D["nas"] / ma(D["nas"], 60) - 1
    X["r_vix"] = D["vix"]
    X["s_stab60"] = D["stab"].pct_change(60, fill_method=None)
    X["s_stab180"] = D["stab"].pct_change(180, fill_method=None)
    X["s_stab_lvl"] = D["stab"]
    X["n_hash"] = ma(D["hr"], 30) / ma(D["hr"], 120) - 1
    X["n_adr"] = ma(D["adr"], 30) / D["adr"].rolling(365, min_periods=180).mean() - 1
    return pd.DataFrame(X, index=idx)


pp = lambda v: f"{v * 100:+.1f}%"
pl = lambda v: f"{v:.0f}%"
ITEMS = [
    ("M", "trend", "t_ma60", "60일선 이격", 1, lambda X, t: pp(X.at[t, "t_ma60"])),
    ("M", "trend", "t_ma120", "120일선 이격", 1, lambda X, t: pp(X.at[t, "t_ma120"])),
    ("M", "trend", "t_ma200", "200일선 이격", 1, lambda X, t: pp(X.at[t, "t_ma200"])),
    ("M", "trend", "t_r120", "120일 수익률", 1, lambda X, t: pp(X.at[t, "t_r120"])),
    ("M", "lev", "l_fund30", "펀딩비 30일 평균 (일)", 1, lambda X, t: f"{X.at[t, 'l_fund30'] * 100:+.3f}%"),
    ("M", "val", "v_mvrv", "MVRV (시가총액/실현시가총액)", 1, lambda X, t: f"{X.at[t, 'v_mvrv']:.2f}"),
    ("M", "breadth", "b_a200", "알트 20종 중 200일선 위", 1, lambda X, t: pl(X.at[t, "b_a200"])),
    ("M", "breadth", "b_a50", "알트 20종 중 50일선 위", 1, lambda X, t: pl(X.at[t, "b_a50"])),
    ("M", "energy", "e_dir60", "방향성 거래량 (60일)", 1,
     lambda X, t: f"상승일 비중 {X.at[t, 'e_up60'] * 100:.0f}% · 1년 평균 대비 {X.at[t, 'e_v60']:.2f}배"),
    ("Mf", "trend", "f_ma20", "20일선 이격", 1, lambda X, t: pp(X.at[t, "f_ma20"])),
    ("Mf", "trend", "f_r20", "20일 수익률", 1, lambda X, t: pp(X.at[t, "f_r20"])),
    ("Mf", "breadth", "f_a20", "알트 20종 중 20일선 위", 1, lambda X, t: pl(X.at[t, "f_a20"])),
    ("Mf", "lev", "f_fund7", "펀딩비 7일 평균 (일)", 1, lambda X, t: f"{X.at[t, 'f_fund7'] * 100:+.3f}%"),
    ("Mf", "energy", "f_dir10", "방향성 거래량 (10일)", 1,
     lambda X, t: f"상승일 비중 {X.at[t, 'f_up10'] * 100:.0f}% · 평소 대비 {X.at[t, 'f_v10']:.2f}배"),
    ("F", "liq", "q_netliq", "연준 순유동성 60일 변화", 1,
     lambda X, t: f"{X.at[t, 'q_netliq_lvl'] / 1e3:,.2f}조 달러 ({X.at[t, 'q_netliq'] * 100:+.1f}%)"),
    ("F", "liq", "q_ry", "미국 실질금리 60일 변화", -1,
     lambda X, t: f"{X.at[t, 'q_ry_lvl']:.2f}% ({X.at[t, 'q_ry'] * 100:+.0f}bp)"),
    ("F", "liq", "q_dxy", "달러인덱스 60일 변화", -1, lambda X, t: pp(X.at[t, "q_dxy"])),
    ("F", "risk", "r_nas", "나스닥 60일선 이격", 1, lambda X, t: pp(X.at[t, "r_nas"])),
    ("F", "risk", "r_vix", "VIX", -1, lambda X, t: f"{X.at[t, 'r_vix']:.1f}"),
    ("F", "stable", "s_stab60", "스테이블코인 60일 증가", 1,
     lambda X, t: f"{X.at[t, 's_stab_lvl'] / 1e9:,.0f}억 달러 ({X.at[t, 's_stab60'] * 100:+.1f}%)"),
    ("F", "stable", "s_stab180", "스테이블코인 180일 증가", 1, lambda X, t: pp(X.at[t, "s_stab180"])),
    ("F", "net", "n_hash", "해시레이트 30일/120일", 1, lambda X, t: pp(X.at[t, "n_hash"])),
    ("F", "net", "n_adr", "활성 주소 30일/1년 평균", 1, lambda X, t: pp(X.at[t, "n_adr"])),
]


def score(X):
    S = {}
    for axis, comp, key, label, d, _ in ITEMS:
        if key not in S:
            p = rolling_pct(X[key])
            S[key] = p if d > 0 else 100 - p
    S = pd.DataFrame(S, index=X.index)
    comps, axes = {}, {}
    for axis, cdef in COMP.items():
        for c in cdef:
            keys = [k for a, cc, k, *_ in ITEMS if a == axis and cc == c]
            comps[f"{axis}:{c}"] = S[keys].mean(axis=1)
    comps = pd.DataFrame(comps, index=X.index)
    for axis, cdef in COMP.items():
        raw = wmean(comps[[f"{axis}:{c}" for c in cdef]].set_axis(list(cdef), axis=1),
                    {k: v[1] for k, v in cdef.items()})
        span = P["fast_smooth"] if axis == "Mf" else P["smooth"]
        axes[axis] = raw.ewm(span=span, adjust=False, ignore_na=True).mean().where(raw.notna())
    return S, comps, pd.DataFrame(axes, index=X.index)


# ── 9단계 판정 ───────────────────────────────────────────────────────
def classify(M, F, dM, dF, gap, Mf, dMf):
    if any(pd.isna(v) for v in (M, F, dM, dF, gap, Mf, dMf)):
        return None
    th = P["th"]
    if Mf < P["crash_level"] and dMf < -P["crash_drop"]:
        return 9
    if M < 35 and dM < -2 * th:
        return 9
    if M >= 55 and dM < -th:
        return 6
    if M >= 70 and gap >= 15 and dM >= 0:
        return 5
    if M >= 60 and dM >= -th:
        return 4
    if 35 <= M < 60 and dM < -th:
        return 7
    if M < 45 and dF < -th and abs(dM) <= th:
        return 8
    if 40 <= M < 70 and dM >= 0 and dF > 0:
        return 3
    if M < 55 and dM > th and F >= P["up2_min_f"]:
        return 2
    if M < 40 and dF > th and dM <= th:
        return 1
    return None


def stage_series(A):
    feat = pd.DataFrame({
        "M": A["M"], "F": A["F"], "Mf": A["Mf"],
        "dM": A["M"].diff(P["m_slope"]), "dF": A["F"].diff(P["f_slope"]),
        "gap": A["M"] - A["F"], "dMf": A["Mf"].diff(P["fast_slope"])}, index=A.index)
    raw = [classify(r.M, r.F, r.dM, r.dF, r.gap, r.Mf, r.dMf) for r in feat.itertuples()]
    conf, cur, cand, cnt, dwell = [], None, None, 0, 0
    for x in raw:
        dwell += 1
        if x is not None:
            if x == cur:
                cand, cnt = None, 0
            elif x == cand:
                cnt += 1
            else:
                cand, cnt = x, 1
            need = P["crash_confirm"] if cand == 9 else P["confirm"]
            if cand is not None and cnt >= need and (cur is None or dwell >= P["dwell"] or cand == 9):
                cur, cand, cnt, dwell = cand, None, 0, 0
        conf.append(cur)
    need = (P["crash_confirm"] if cand == 9 else P["confirm"]) if cand else None
    return (pd.Series(raw, index=A.index, dtype="object"),
            pd.Series(conf, index=A.index, dtype="object"), feat, (cand, cnt, need))


def episodes(conf):
    out, prev = [], "__start__"
    for t, s in conf.items():
        if s != prev:
            out.append([s, t, t, 0])
            prev = s
        out[-1][2] = t
        out[-1][3] += 1
    return [e for e in out if e[0] is not None]


def fwd_mdd(c, h):
    a = c.values
    out = np.full(len(a), np.nan)
    for i in range(len(a) - h):
        w = a[i:i + h + 1]
        out[i] = (w / np.maximum.accumulate(w) - 1).min()
    return pd.Series(out, index=c.index)


def stage_stats(conf, px):
    lr = np.log(px).diff()
    v = pd.DataFrame({
        "s": conf,
        "f": px.shift(-HORIZON) / px - 1,
        "vol": lr[::-1].rolling(HORIZON).std()[::-1].shift(-1) * np.sqrt(365),
        "mdd": fwd_mdd(px, HORIZON)}).dropna(subset=["s"])
    base = {"fwd": v.f.mean(), "vol": v.vol.mean(), "mdd": v.mdd.mean()}
    eps = episodes(conf)
    pc = lambda x: None if pd.isna(x) else round(float(x) * 100, 2)
    hit = lambda s: round(float((s.dropna() > 0).mean() * 100), 1) if s.notna().any() else None

    out = {}
    for s in STAGES:
        x = v[v.s == s]
        lens = [e[3] for e in eps if e[0] == s]
        out[s] = {"days": int(len(x)), "episodes": len(lens),
                  "avg_len": round(float(np.mean(lens)), 1) if lens else None,
                  "fwd": pc(x.f.mean()), "fwd_excess": pc(x.f.mean() - base["fwd"]),
                  "hit": hit(x.f), "vol": pc(x.vol.mean()), "mdd": pc(x.mdd.mean())}
    halves = []
    first = v.index[0].year
    for name, m in ((f"{first}~2019", v.index < SPLIT), ("2020~현재", v.index >= SPLIT)):
        h = v[m]
        b = h.f.mean()
        row = {"period": name}
        for ph, grp in (("up", UP_PHASE), ("down", DOWN_PHASE)):
            x = h[h.s.isin(grp)]
            row[ph] = {"fwd_excess": pc(x.f.mean() - b), "hit": hit(x.f), "mdd": pc(x.mdd.mean())}
        halves.append(row)
    trans = {s: {} for s in STAGES}
    seq = [e[0] for e in eps]
    for a, b in zip(seq, seq[1:]):
        trans[a][b] = trans[a].get(b, 0) + 1
    trans = {a: {b: round(c / sum(d.values()) * 100, 1) for b, c in sorted(d.items(), key=lambda z: -z[1])}
             for a, d in trans.items() if d}
    return out, halves, trans, {k: pc(val) for k, val in base.items()}, eps


# ── JSON ─────────────────────────────────────────────────────────────
def rnd(v, nd=1):
    return None if v is None or pd.isna(v) else round(float(v), nd)


def trend_word(v, th):
    if pd.isna(v):
        return "—"
    return "개선" if v > th else "악화" if v < -th else "보합"


def build_json(idx, X, S, comps, raw, conf, feat, pending, px, asof):
    t = idx[-1]
    cur = conf.iloc[-1]
    run = 0
    for s in reversed(conf.tolist()):
        if s != cur:
            break
        run += 1
    since = conf.index[-run] if run else None

    def axis_block(axis):
        blocks = {}
        for c, (label, w) in COMP[axis].items():
            items = []
            for a, cc, key, ilabel, d, disp in ITEMS:
                if a != axis or cc != c:
                    continue
                try:
                    ds = disp(X, t)
                except Exception:
                    ds = "—"
                items.append({"label": ilabel, "disp": "—" if "nan" in ds else ds,
                              "score": rnd(S.at[t, key]), "dir": d})
            col = comps[f"{axis}:{c}"]
            blocks[c] = {"label": label, "weight": w, "score": rnd(col.iloc[-1]),
                         "d20": rnd(col.diff(20).iloc[-1]), "items": items}
        return blocks

    stats, halves, trans, base, eps = stage_stats(conf, px)
    f = feat.iloc[-1]
    accel = f.Mf - f.M
    acc_txt = "Short-term 가속" if accel > 10 else "Short-term 둔화" if accel < -10 else "속도 비슷"
    summary = (f"Fundamentals {f.F:.0f}({trend_word(f.dF, P['th'])}) · Momentum {f.M:.0f}({trend_word(f.dM, P['th'])}) · "
               f"Short-term {f.Mf:.0f} — " + (STAGES[cur][1] if cur else "판정 대기"))
    hist = pd.DataFrame({"F": feat.F, "M": feat.M, "Mf": feat.Mf, "g": feat.gap, "s": conf,
                         "k": px}).dropna(subset=["F", "M"])

    def rows(df):
        return [{"d": d.strftime("%Y-%m-%d"), "F": rnd(r.F), "M": rnd(r.M), "Mf": rnd(r.Mf),
                 "g": rnd(r.g), "s": None if pd.isna(r.s) else int(r.s), "k": rnd(r.k, 2)}
                for d, r in df.iterrows()]

    cand, cnt, need = pending
    recent = [{"s": int(e[0]), "start": e[1].strftime("%Y-%m-%d"), "end": e[2].strftime("%Y-%m-%d"),
               "days": e[3], "k0": rnd(px.loc[e[1]], 2), "k1": rnd(px.loc[e[2]], 2)} for e in eps[-14:]]
    ds = lambda x: x.strftime("%Y-%m-%d") if x is not None and not pd.isna(x) else None
    return {
        "version": VERSION, "asset": "BTC", "horizon": HORIZON,
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
        "as_of": t.strftime("%Y-%m-%d"),
        "sources_as_of": {k: ds(v) for k, v in asof.items()},
        "stage": {"id": cur, "name": STAGES[cur][0] if cur else None,
                  "desc": STAGES[cur][1] if cur else None,
                  "phase": "up" if cur in UP_PHASE else "down" if cur in DOWN_PHASE else "base",
                  "since": ds(since), "days": run, "raw_today": raw.iloc[-1],
                  "pending": {"id": cand, "name": STAGES[cand][0], "count": cnt, "need": need} if cand else None},
        "F": {"value": rnd(f.F), "dslope": rnd(f.dF), "slope_days": P["f_slope"], "components": axis_block("F")},
        "M": {"value": rnd(f.M), "dslope": rnd(f.dM), "slope_days": P["m_slope"], "components": axis_block("M")},
        "Mf": {"value": rnd(f.Mf), "dslope": rnd(f.dMf), "slope_days": P["fast_slope"], "accel": rnd(accel),
               "accel_text": acc_txt, "components": axis_block("Mf")},
        "gap": rnd(f.gap), "summary": summary, "price": rnd(px.iloc[-1], 2),
        "history": rows(hist.tail(HISTORY_DAYS)),
        "history_long": rows(hist.iloc[::7]),
        "episodes_recent": recent,
        "stage_stats": {str(k): v for k, v in stats.items()},
        "phase_by_period": halves, "baseline": base,
        "transitions": {str(k): {str(b): p for b, p in v.items()} for k, v in trans.items()},
        "stage_defs": {str(k): {"name": v[0], "desc": v[1],
                                "phase": "up" if k in UP_PHASE else "down" if k in DOWN_PHASE else "base"}
                       for k, v in STAGES.items()},
        "params": P,
        "disclaimer": "국면 설명 지표이며 매매 신호가 아님. 완전한 사이클이 3~4번뿐이라 통계 표본이 작음.",
    }


def push():
    def run(*cmd):
        return subprocess.run(cmd, cwd=BASE, capture_output=True, text=True)
    rel = str(OUT_JSON.relative_to(BASE))
    run("git", "add", rel)
    if run("git", "diff", "--cached", "--quiet", "--", rel).returncode == 0:
        log("push: 변경 없음")
        return
    run("git", "commit", "-m", f"reflex_btc: {datetime.now(KST):%Y-%m-%d %H:%M}", "--", rel)
    r = run("git", "pull", "--rebase", "--autostash")
    if r.returncode != 0:
        log("push: pull --rebase 실패", r.stderr[-300:])
        run("git", "rebase", "--abort")
        return
    r = run("git", "push")
    log("push: 완료" if r.returncode == 0 else f"push: 실패 {r.stderr[-300:]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true")
    a = ap.parse_args()
    idx, D, asof = load()
    X = build(idx, D)
    S, comps, A = score(X)
    raw, conf, feat, pending = stage_series(A)
    out = build_json(idx, X, S, comps, raw, conf, feat, pending, D["px"], asof)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":"), default=str))
    tmp.replace(OUT_JSON)
    st = out["stage"]
    log(f"reflex_btc: {out['as_of']} | {st['id']} {st['name']} ({st['days']}일째) | {out['summary']}")
    if st["pending"]:
        p = st["pending"]
        log(f"reflex_btc: 전환 후보 {p['id']} {p['name']} ({p['count']}/{p['need']}일)")
    if a.push:
        push()


if __name__ == "__main__":
    main()
