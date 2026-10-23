#!/usr/bin/env python3
"""
reflex.py v2 — 소로스 재귀성 9단계: 형세(F)·기세(M) 판정 → docs/data/reflex.json

입력: data/reflex/{macro,breadth,flows}.parquet  (reflex_data.py 산출물)
출력: docs/data/reflex.json

원칙
  - 모든 지표는 과거 3년(756거래일) 대비 백분위로 0~100 변환 (미래 정보 없음)
  - 미국 지표는 한국 날짜 D에 대해 D-1 이전 값만 사용
  - 기세는 두 속도: 느린 기세(M, 60~250일 지표)로 단계를 판정하고,
    단기 기세(Mf, 20일 지표)는 가속/감속 표시와 급락 예외에만 사용
  - 단계 전환: 같은 판정 5일 연속 + 직전 단계 최소 10일 체류 (급락 예외는 3일, 체류 무시)
  - 2011~2026 백테스트로 설정 (평활 20, 기세 기울기 20일, 형세 기울기 40일, 임계 10)

이 단계는 국면 설명 도구이며 매매 신호가 아님:
  상승/하락 국면의 이후 수익률 차이는 2019~2026엔 뚜렷했지만 2011~2018(박스권)엔 없었음.

사용법:
  python reflex.py            # 계산 + JSON 저장
  python reflex.py --push     # 저장 후 docs/data/reflex.json git push
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
DATA = BASE / "data" / "reflex"
OUT_JSON = BASE / "docs" / "data" / "reflex.json"
VERSION = "reflex_v2"

PCT_WIN, PCT_MIN = 756, 252
P = dict(smooth=20, fast_smooth=5, m_slope=20, f_slope=40, th=10,
         confirm=5, dwell=10, crash_level=15, crash_drop=35, crash_confirm=3)
HISTORY_DAYS = 520
SPLIT = "2019-01-01"

STAGES = {
    1: ("유리한 형세", "현실이 먼저 좋아지기 시작했지만 시장은 아직 믿지 않음"),
    2: ("상승 기세", "가격이 반응하기 시작함"),
    3: ("현실 개선", "형세가 좋아지고 기세가 따라붙음"),
    4: ("확신 강화", "기세가 높은 수준에서 유지 — 참여가 넓고 추세가 이어짐"),
    5: ("기세 가속", "기세가 형세를 크게 앞섬 — 편향이 스스로를 강화하는 구간"),
    6: ("반전 시작", "높은 곳에서 기세가 꺾이기 시작함"),
    7: ("기세 약화", "기세가 중간 수준으로 식어가는 중"),
    8: ("현실 악화", "형세가 나빠지고 기세도 약함"),
    9: ("하락 가속", "기세가 무너지며 하락이 하락을 부르는 구간"),
}
UP_PHASE, DOWN_PHASE = (2, 3, 4, 5), (6, 7, 8, 9)

COMP = {
    "M": {"trend": ("추세", 0.30), "part": ("참여", 0.35), "energy": ("에너지", 0.15), "flow": ("수급", 0.20)},
    "Mf": {"trend": ("추세", 0.30), "part": ("참여", 0.35), "energy": ("에너지", 0.15), "flow": ("수급", 0.20)},
    "F": {"risk": ("글로벌 위험선호", 0.30), "semi": ("반도체 사이클", 0.25), "fx": ("원화·달러", 0.20),
          "rate": ("금리", 0.15), "cycle": ("경기", 0.10)},
}


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


# ── 데이터 정렬 ──────────────────────────────────────────────────────
def load():
    macro = pd.read_parquet(DATA / "macro.parquet").sort_index()
    br = pd.read_parquet(DATA / "breadth.parquet").sort_index()
    fp = DATA / "flows.parquet"
    fl = pd.read_parquet(fp).sort_index() if fp.exists() else None

    idx = br.index
    kr = macro[["코스피", "코스닥"]].dropna(how="all").reindex(idx, method="ffill")

    us, us_asof = {}, None
    for c in macro.columns:
        if c in ("코스피", "코스닥"):
            continue
        s = macro[c].dropna()
        if s.empty:
            continue
        a = s.reindex(idx - pd.Timedelta(days=1), method="ffill")
        a.index = idx
        us[c] = a
        if c == "S&P500":
            prev = s.index[s.index < idx[-1]]
            us_asof = prev.max() if len(prev) else None
    us = pd.DataFrame(us, index=idx)
    fl = fl.reindex(idx) if fl is not None else None
    return idx, kr, us, br, fl, us_asof


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


# ── 지표 ─────────────────────────────────────────────────────────────
def build(idx, kr, us, br, fl):
    ma = lambda s, w: s.rolling(w, min_periods=w).mean()
    r = (0.85 * kr["코스피"].pct_change(fill_method=None)
         + 0.15 * kr["코스닥"].pct_change(fill_method=None))
    comp = (1 + r.fillna(0)).cumprod() * 100
    X = {}

    # 기세 공통
    X["t_ma20"] = comp / ma(comp, 20) - 1
    X["t_ma60"] = comp / ma(comp, 60) - 1
    X["t_ma120"] = comp / ma(comp, 120) - 1
    X["t_r20"] = comp.pct_change(20, fill_method=None)
    X["t_r120"] = comp.pct_change(120, fill_method=None)
    X["p_a20"] = br["ALL_above20"]
    X["p_a60"] = br["ALL_above60"]
    X["p_a200"] = br["ALL_above200"]
    X["p_hl"] = (br["ALL_nh60"] - br["ALL_nl60"]) / br["ALL_n"]
    X["p_hl252"] = (br["ALL_nh252"] - br["ALL_nl252"]) / br["ALL_n"]
    ad = br["ALL_adv"] - br["ALL_dec"]
    X["p_ad10"] = ad.rolling(10).sum() / br["ALL_n"].rolling(10).sum()
    X["p_ad60"] = ad.rolling(60).sum() / br["ALL_n"].rolling(60).sum()
    val = br["ALL_value"]
    X["e_upshare"] = (val * (r > 0)).rolling(10).sum() / val.rolling(10).sum()
    X["e_v10"] = val.rolling(10).mean() / val.rolling(120, min_periods=60).mean()
    X["e_dir"] = (X["e_upshare"] - 0.5) * X["e_v10"]
    X["e_upshare60"] = (val * (r > 0)).rolling(60).sum() / val.rolling(60).sum()
    X["e_v60"] = val.rolling(60).mean() / val.rolling(250, min_periods=120).mean()
    X["e_dir60"] = (X["e_upshare60"] - 0.5) * X["e_v60"]
    if fl is not None:
        cols = [c for c in ("KOSPI_foreign", "KOSPI_inst", "KOSDAQ_foreign", "KOSDAQ_inst") if c in fl.columns]
        net = fl[cols].sum(axis=1, min_count=1)
        for w, mp in ((20, 15), (60, 45)):
            s = net.rolling(w, min_periods=mp).sum()
            X[f"f_net{w}"] = s / (val * 1e8).rolling(w, min_periods=mp).sum()
            X[f"f_net{w}_eok"] = s / 1e8
    else:
        for k in ("f_net20", "f_net20_eok", "f_net60", "f_net60_eok"):
            X[k] = pd.Series(np.nan, index=idx)

    # 형세 (미국 지표는 D-1 정렬 완료)
    g = lambda c: us[c] if c in us.columns else pd.Series(np.nan, index=idx)
    spx, ndx, vix, sox = g("S&P500"), g("나스닥"), g("VIX"), g("필라델피아반도체")
    X["r_spx"] = spx / ma(spx, 60) - 1
    X["r_ndx"] = ndx / ma(ndx, 60) - 1
    X["r_vix"] = vix
    X["s_sox"] = sox / ma(sox, 60) - 1
    X["s_rel"] = (sox / spx).pct_change(60, fill_method=None)
    X["x_krw"] = g("원달러").pct_change(60, fill_method=None)
    X["x_dxy"] = g("달러인덱스").pct_change(60, fill_method=None)
    X["x_krw_lvl"] = g("원달러")
    X["i_10y"] = g("미국10Y").diff(60)
    X["i_10y_lvl"] = g("미국10Y")
    X["i_curve"] = g("미국10Y") - g("미국2Y")
    X["c_cugd"] = (g("구리") / g("금")).pct_change(60, fill_method=None)

    return pd.DataFrame(X, index=idx), comp


pp = lambda v: f"{v * 100:+.1f}%"
pl = lambda v: f"{v:.1f}%"
pf = lambda v: f"{v * 100:+.1f}%p"
ITEMS = [
    # axis, comp, key, label, direction, display
    # 느린 기세 (단계 판정)
    ("M", "trend", "t_ma60", "합성지수 60일선 이격", 1, lambda X, t: pp(X.at[t, "t_ma60"])),
    ("M", "trend", "t_ma120", "합성지수 120일선 이격", 1, lambda X, t: pp(X.at[t, "t_ma120"])),
    ("M", "trend", "t_r120", "합성지수 120일 수익률", 1, lambda X, t: pp(X.at[t, "t_r120"])),
    ("M", "part", "p_a60", "60일선 위 종목 비율", 1, lambda X, t: pl(X.at[t, "p_a60"])),
    ("M", "part", "p_a200", "200일선 위 종목 비율", 1, lambda X, t: pl(X.at[t, "p_a200"])),
    ("M", "part", "p_hl252", "52주 신고가−신저가", 1, lambda X, t: pf(X.at[t, "p_hl252"])),
    ("M", "part", "p_ad60", "상승−하락 60일 누적", 1, lambda X, t: pf(X.at[t, "p_ad60"])),
    ("M", "energy", "e_dir60", "방향성 거래대금 (60일)", 1,
     lambda X, t: f"상승일 비중 {X.at[t, 'e_upshare60'] * 100:.0f}% · 1년 평균 대비 {X.at[t, 'e_v60']:.2f}배"),
    ("M", "flow", "f_net60", "외국인+기관 60일 순매수", 1,
     lambda X, t: f"{X.at[t, 'f_net60_eok']:+,.0f}억 (거래대금 대비 {X.at[t, 'f_net60'] * 100:+.1f}%)"),
    # 단기 기세 (가속/감속, 급락 예외)
    ("Mf", "trend", "t_ma20", "합성지수 20일선 이격", 1, lambda X, t: pp(X.at[t, "t_ma20"])),
    ("Mf", "trend", "t_ma60", "합성지수 60일선 이격", 1, lambda X, t: pp(X.at[t, "t_ma60"])),
    ("Mf", "trend", "t_r20", "합성지수 20일 수익률", 1, lambda X, t: pp(X.at[t, "t_r20"])),
    ("Mf", "part", "p_a20", "20일선 위 종목 비율", 1, lambda X, t: pl(X.at[t, "p_a20"])),
    ("Mf", "part", "p_a60", "60일선 위 종목 비율", 1, lambda X, t: pl(X.at[t, "p_a60"])),
    ("Mf", "part", "p_hl", "60일 신고가−신저가", 1, lambda X, t: pf(X.at[t, "p_hl"])),
    ("Mf", "part", "p_ad10", "상승−하락 10일 누적", 1, lambda X, t: pf(X.at[t, "p_ad10"])),
    ("Mf", "energy", "e_dir", "방향성 거래대금 (10일)", 1,
     lambda X, t: f"상승일 비중 {X.at[t, 'e_upshare'] * 100:.0f}% · 평소 대비 {X.at[t, 'e_v10']:.2f}배"),
    ("Mf", "flow", "f_net20", "외국인+기관 20일 순매수", 1,
     lambda X, t: f"{X.at[t, 'f_net20_eok']:+,.0f}억 (거래대금 대비 {X.at[t, 'f_net20'] * 100:+.1f}%)"),
    # 형세
    ("F", "risk", "r_spx", "S&P500 60일선 이격", 1, lambda X, t: pp(X.at[t, "r_spx"])),
    ("F", "risk", "r_ndx", "나스닥 60일선 이격", 1, lambda X, t: pp(X.at[t, "r_ndx"])),
    ("F", "risk", "r_vix", "VIX", -1, lambda X, t: f"{X.at[t, 'r_vix']:.1f}"),
    ("F", "semi", "s_sox", "SOX 60일선 이격", 1, lambda X, t: pp(X.at[t, "s_sox"])),
    ("F", "semi", "s_rel", "SOX/S&P 60일 상대강도", 1, lambda X, t: pp(X.at[t, "s_rel"])),
    ("F", "fx", "x_krw", "원달러 60일 변화", -1,
     lambda X, t: f"{X.at[t, 'x_krw_lvl']:,.0f}원 ({X.at[t, 'x_krw'] * 100:+.1f}%)"),
    ("F", "fx", "x_dxy", "달러인덱스 60일 변화", -1, lambda X, t: pp(X.at[t, "x_dxy"])),
    ("F", "rate", "i_10y", "미국 10Y 60일 변화", -1,
     lambda X, t: f"{X.at[t, 'i_10y_lvl']:.2f}% ({X.at[t, 'i_10y'] * 100:+.0f}bp)"),
    ("F", "rate", "i_curve", "미국 10Y−2Y", 1, lambda X, t: f"{X.at[t, 'i_curve'] * 100:+.0f}bp"),
    ("F", "cycle", "c_cugd", "구리/금 60일 변화", 1, lambda X, t: pp(X.at[t, "c_cugd"])),
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
        cols = [f"{axis}:{c}" for c in cdef]
        raw = wmean(comps[cols].set_axis(list(cdef), axis=1), {k: v[1] for k, v in cdef.items()})
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
    if 35 <= M < 55 and dM < -th:
        return 7
    if M < 45 and dF < -th and abs(dM) <= th:
        return 8
    if 40 <= M < 70 and dM >= 0 and dF > 0:
        return 3
    if M < 55 and dM > th:
        return 2
    if M < 40 and dF > th and dM <= th:
        return 1
    return None


def stage_series(A):
    feat = pd.DataFrame({
        "M": A["M"], "F": A["F"], "Mf": A["Mf"],
        "dM": A["M"].diff(P["m_slope"]), "dF": A["F"].diff(P["f_slope"]),
        "gap": A["M"] - A["F"], "dMf": A["Mf"].diff(10)}, index=A.index)
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


def fwd_mdd(c, h=60):
    a = c.values
    out = np.full(len(a), np.nan)
    for i in range(len(a) - h):
        w = a[i:i + h + 1]
        out[i] = (w / np.maximum.accumulate(w) - 1).min()
    return pd.Series(out, index=c.index)


def stage_stats(conf, comp):
    lr = np.log(comp).diff()
    v = pd.DataFrame({
        "s": conf,
        "f20": comp.shift(-20) / comp - 1,
        "f60": comp.shift(-60) / comp - 1,
        "vol": lr[::-1].rolling(60).std()[::-1].shift(-1) * np.sqrt(252),
        "mdd": fwd_mdd(comp)}).dropna(subset=["s"])
    base = {"f20": v.f20.mean(), "f60": v.f60.mean(), "vol": v.vol.mean(), "mdd": v.mdd.mean()}
    eps = episodes(conf)

    pc = lambda x: None if pd.isna(x) else round(float(x) * 100, 2)
    out = {}
    for s in STAGES:
        x = v[v.s == s]
        lens = [e[3] for e in eps if e[0] == s]
        out[s] = {
            "days": int(len(x)), "episodes": len(lens),
            "avg_len": round(float(np.mean(lens)), 1) if lens else None,
            "fwd20": pc(x.f20.mean()), "fwd60": pc(x.f60.mean()),
            "fwd60_excess": pc(x.f60.mean() - base["f60"]),
            "hit60": round(float((x.f60.dropna() > 0).mean() * 100), 1) if x.f60.notna().any() else None,
            "vol60": pc(x.vol.mean()), "mdd60": pc(x.mdd.mean()),
        }

    halves = []
    for name, m in (("2011~2018", v.index < SPLIT), ("2019~현재", v.index >= SPLIT)):
        h = v[m]
        b = h.f60.mean()
        row = {"period": name}
        for ph, grp in (("up", UP_PHASE), ("down", DOWN_PHASE)):
            x = h[h.s.isin(grp)]
            row[ph] = {"fwd60_excess": pc(x.f60.mean() - b),
                       "hit60": round(float((x.f60.dropna() > 0).mean() * 100), 1),
                       "vol60": pc(x.vol.mean()), "mdd60": pc(x.mdd.mean())}
        halves.append(row)

    trans = {s: {} for s in STAGES}
    seq = [e[0] for e in eps]
    for a, b in zip(seq, seq[1:]):
        trans[a][b] = trans[a].get(b, 0) + 1
    trans = {a: {b: round(c / sum(d.values()) * 100, 1) for b, c in sorted(d.items(), key=lambda z: -z[1])}
             for a, d in trans.items() if d}
    base = {k: pc(val) for k, val in base.items()}
    return out, halves, trans, base, eps


# ── JSON ─────────────────────────────────────────────────────────────
def rnd(v, nd=1):
    return None if v is None or pd.isna(v) else round(float(v), nd)


def trend_word(v, th):
    if pd.isna(v):
        return "—"
    return "개선" if v > th else "악화" if v < -th else "보합"


def build_json(idx, X, S, comps, A, raw, conf, feat, pending, comp, kr, us_asof):
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

    stats, halves, trans, base, eps = stage_stats(conf, comp)
    f = feat.iloc[-1]
    accel = f.Mf - f.M
    acc_txt = "단기 기세가 더 강함(가속)" if accel > 10 else "단기 기세가 더 약함(감속)" if accel < -10 else "속도 비슷"
    summary = (f"형세 {f.F:.0f}({trend_word(f.dF, P['th'])}) · 기세 {f.M:.0f}({trend_word(f.dM, P['th'])}) · "
               f"단기 기세 {f.Mf:.0f} — " + (STAGES[cur][1] if cur else "판정 대기"))

    hist = pd.DataFrame({"F": feat.F, "M": feat.M, "Mf": feat.Mf, "g": feat.gap, "s": conf,
                         "k": kr["코스피"]}).dropna(subset=["F", "M"])

    def rows(df):
        return [{"d": d.strftime("%Y-%m-%d"), "F": rnd(r.F), "M": rnd(r.M), "Mf": rnd(r.Mf),
                 "g": rnd(r.g), "s": None if pd.isna(r.s) else int(r.s), "k": rnd(r.k, 2)}
                for d, r in df.iterrows()]

    cand, cnt, need = pending
    recent_eps = [{"s": int(e[0]), "start": e[1].strftime("%Y-%m-%d"), "end": e[2].strftime("%Y-%m-%d"),
                   "days": e[3], "k0": rnd(kr["코스피"].loc[e[1]], 2), "k1": rnd(kr["코스피"].loc[e[2]], 2)}
                  for e in eps[-14:]]
    return {
        "version": VERSION,
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M KST"),
        "as_of": t.strftime("%Y-%m-%d"),
        "us_as_of": us_asof.strftime("%Y-%m-%d") if us_asof is not None else None,
        "stage": {"id": cur, "name": STAGES[cur][0] if cur else None,
                  "desc": STAGES[cur][1] if cur else None,
                  "phase": "up" if cur in UP_PHASE else "down" if cur in DOWN_PHASE else "base",
                  "since": since.strftime("%Y-%m-%d") if since is not None else None, "days": run,
                  "raw_today": raw.iloc[-1],
                  "pending": {"id": cand, "name": STAGES[cand][0], "count": cnt, "need": need}
                  if cand else None},
        "F": {"value": rnd(f.F), "d40": rnd(f.dF), "components": axis_block("F")},
        "M": {"value": rnd(f.M), "d20": rnd(f.dM), "components": axis_block("M")},
        "Mf": {"value": rnd(f.Mf), "d10": rnd(f.dMf), "accel": rnd(accel), "accel_text": acc_txt,
               "components": axis_block("Mf")},
        "gap": rnd(f.gap),
        "summary": summary,
        "kospi": rnd(kr["코스피"].iloc[-1], 2),
        "history": rows(hist.tail(HISTORY_DAYS)),
        "history_long": rows(hist.iloc[::5]),
        "episodes_recent": recent_eps,
        "stage_stats": {str(k): v for k, v in stats.items()},
        "phase_by_period": halves,
        "baseline": base,
        "transitions": {str(k): {str(b): p for b, p in v.items()} for k, v in trans.items()},
        "stage_defs": {str(k): {"name": v[0], "desc": v[1],
                                "phase": "up" if k in UP_PHASE else "down" if k in DOWN_PHASE else "base"}
                       for k, v in STAGES.items()},
        "params": P,
        "disclaimer": "국면 설명 지표이며 매매 신호가 아님. 상승/하락 국면의 이후 수익률 차이는 기간에 따라 크게 다름.",
    }


def push():
    def run(*cmd):
        return subprocess.run(cmd, cwd=BASE, capture_output=True, text=True)
    rel = str(OUT_JSON.relative_to(BASE))
    run("git", "add", rel)
    if run("git", "diff", "--cached", "--quiet", "--", rel).returncode == 0:
        log("push: 변경 없음")
        return
    run("git", "commit", "-m", f"reflex: {datetime.now(KST):%Y-%m-%d %H:%M}", "--", rel)
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

    idx, kr, us, br, fl, us_asof = load()
    X, comp = build(idx, kr, us, br, fl)
    S, comps, A = score(X)
    raw, conf, feat, pending = stage_series(A)
    out = build_json(idx, X, S, comps, A, raw, conf, feat, pending, comp, kr, us_asof)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_JSON.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":"), default=str))
    tmp.replace(OUT_JSON)
    st = out["stage"]
    log(f"reflex: {out['as_of']} | {st['id']} {st['name']} ({st['days']}일째) | {out['summary']}")
    if st["pending"]:
        p = st["pending"]
        log(f"reflex: 전환 후보 {p['id']} {p['name']} ({p['count']}/{p['need']}일)")
    if a.push:
        push()


if __name__ == "__main__":
    main()
