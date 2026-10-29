#!/usr/bin/env python3
"""
focus.py — Focus 2: 실제 매매용 1~3종목 선정 (Picks 엔진의 집중 버전)

규칙 (백테스트 2012~2026과 같은 식):
  1) 시점   : Reflex 시장 Fundamentals ≥ 55 이고 Momentum ≥ 50 일 때만 보유. 아니면 전량 현금(대기).
  2) 업종   : 거래대금 상위 600종목 중 5종목 이상인 업종에서
              6개월 상대강도(업종 시총가중 6개월 수익 − 600종목 시총가중) 순위 백분위
              + 업종 이익 증가율(업종 합산 EPS×주식수, 1년 전 대비) 순위 백분위 → 상위 2개 업종
  3) 종목   : 각 업종 안에서 점수 1위
              점수 = ½ × 업종 내 Composite 백분위 (Quality·Trend·Low Vol 각각의 업종 내 백분위 평균)
                   + ½ × 업종 내 목표가 상향 우위 백분위 (최근 3개월, 같은 증권사 직전 대비 상향 − 하향, ÷ 커버 증권사 수;
                         리포트 없으면 중간값)
  4) 교체   : 월 1회(그 달 첫 실행) 또는 조건이 새로 충족될 때. 조건이 깨지면 다음 날 전량 매도.
  5) 장부   : data/picks/focus_ledger.parquet (종목 균등, 편도 0.25%, 현금은 CD 금리)
산출: docs/data/focus.json (picks.html의 Focus 섹션), --push면 GitHub 반영, 매수·매도 때 텔레그램.
사용법: python focus.py [--push] [--no-telegram]
"""
import json
import argparse
from datetime import datetime

import numpy as np
import pandas as pd

import stock_score as ss

BASE, KST, log = ss.BASE, ss.KST, ss.log
CONS = BASE / "data" / "consensus" / "opinions.parquet"
OUT_JSON = BASE / "docs" / "data" / "focus.json"
STATE = ss.STATE_DIR / "focus_state.json"
LEDGER = ss.STATE_DIR / "focus_ledger.parquet"
VERSION = "focus_v1"

K = 2                  # 종목 수
F_MIN, M_MIN = 55, 50  # 시장 Fundamentals·Momentum 조건
W_CONS = 0.5           # 목표가 신호 비중
MIN_SEC = 5            # 업종 최소 종목 수
COST = 0.0025          # 편도
BACKTEST = dict(period="2012.1~2026.10", rows=[
    ["Focus 2 (이 규칙)", "10.3%", "0.3% / 21.5%", "−31%"],
    ["Focus 2, 목표가 신호 없이", "8.6%", "−2.0% / 20.8%", "−50%"],
    ["코스피, 같은 시점 조건", "10.2%", "2.1% / 18.9%", "−19%"],
    ["코스피 보유", "5.9%", "0.0% / 12.3%", "−46%"]])


def z(s):
    return s.rank(pct=True) - 0.5


def reflex_fm():
    try:
        j = json.loads(ss.REFLEX_JSON.read_text(encoding="utf-8"))
        F, M = (j.get("F") or {}).get("value"), (j.get("M") or {}).get("value")
        hist = [[h["d"], h.get("F"), h.get("M")] for h in (j.get("history") or [])[-120:] if "d" in h]
        return (float(F) if F is not None else None), (float(M) if M is not None else None), j.get("as_of"), hist
    except Exception as e:
        log(f"reflex.json 읽기 실패: {e!r}")
        return None, None, None, []


def consensus(today):
    """그날까지 공개된 리포트로 종목별 목표가 신호 (백테스트 cons_factors.py와 같은 식)."""
    if not CONS.exists():
        log("투자의견 데이터 없음 — 목표가 신호는 중립(중간값)으로 처리")
        return pd.DataFrame(), None
    op = pd.read_parquet(CONS)
    op["date"] = pd.to_datetime(op["date"]).dt.normalize().astype("datetime64[ns]")
    asof = op.date.max()
    op = op[(op.date > today - pd.Timedelta(days=730)) & (op.date <= today)]
    op = op[op.target > 0].sort_values(["ticker", "broker", "date"])
    g = op.groupby(["ticker", "broker"])
    op["prev_t"], op["prev_d"] = g.target.shift(1), g.date.shift(1)
    ok = (op.date - op.prev_d).dt.days <= 365
    op["rev"] = np.where(ok, op.target / op.prev_t - 1, np.nan)
    op["rev"] = op["rev"].clip(-0.8, 2.0)
    op["up"] = (op.rev > 0.001).astype(float).where(op.rev.notna())
    op["dn"] = (op.rev < -0.001).astype(float).where(op.rev.notna())
    w91 = op[op.date > today - pd.Timedelta(days=91)]
    w180 = op[op.date > today - pd.Timedelta(days=180)]
    a = w91.groupby("ticker").agg(n_up=("up", "sum"), n_dn=("dn", "sum"), rev_mean=("rev", "mean"))
    last = w180.groupby(["ticker", "broker"]).tail(1)
    b = last.groupby("ticker").agg(cov=("broker", "nunique"), tgt=("target", "median"), last_date=("date", "max"))
    d = a.join(b, how="outer")
    d["net_up"] = (d.n_up.fillna(0) - d.n_dn.fillna(0)) / d["cov"].clip(lower=1)
    return d, asof


def build(D, sc, i, cons):
    """업종 점수와 업종 내 종목 점수 (백테스트 focus_prep.py와 같은 식)."""
    ret, cap, close, eps = D["ret"], D["cap"], D["close"], D["eps"]
    idx = (1 + ret.fillna(0)).cumprod()
    d = sc[sc.sec.notna()].copy()
    tk = d.index
    d["r126"] = idx.iloc[i][tk] / idx.iloc[i - 126][tk] - 1
    sh = cap.iloc[i][tk] / close.iloc[i][tk]
    d["E0"], d["E1"] = eps.iloc[i][tk] * sh, eps.iloc[i - 252][tk] * sh

    def cw(x, c):
        x = x.dropna(subset=[c])
        return (x[c] * x.cap).sum() / x.cap.sum() if len(x) else np.nan
    m126 = cw(d, "r126")
    rows = {}
    for s, x in d.groupby("sec"):
        if len(x) < MIN_SEC:
            continue
        e = x[["E0", "E1"]].dropna()
        segE = (e.E0.sum() - e.E1.sum()) / abs(e.E1.sum()) if len(e) >= 3 and e.E1.sum() != 0 else np.nan
        rows[s] = dict(n=len(x), rs6=cw(x, "r126") - m126, segE=segE)
    st = pd.DataFrame(rows).T.astype(float)
    st["score"] = st.rs6.rank(pct=True) + st.segE.rank(pct=True).fillna(0)
    st = st.sort_values("score", ascending=False)
    st["rank"] = np.arange(1, len(st) + 1)

    d = d[d.sec.isin(st.index)].copy()
    Z = {f: z(d[f]).fillna(0) for f in ["epos", "ep", "eg1y", "fl60", "hi52", "nr21", "nvol"]}
    d["q"] = (Z["epos"] + Z["ep"] + Z["eg1y"] + Z["fl60"]) / 4
    d["t"] = (Z["hi52"] + Z["nr21"]) / 2
    d["v"] = Z["nvol"]
    g = d.groupby("sec")
    d["w_q"], d["w_t"], d["w_v"] = g.q.rank(pct=True), g.t.rank(pct=True), g.v.rank(pct=True)
    d["w_comp"] = (d.w_q + d.w_t + d.w_v) / 3
    for c in ["net_up", "n_up", "n_dn", "cov", "rev_mean", "tgt", "last_date"]:
        d[c] = cons[c].reindex(d.index) if len(cons) and c in cons else np.nan
    d["w_cons"] = d.groupby("sec").net_up.rank(pct=True).fillna(0.5)
    d["score"] = (1 - W_CONS) * d.w_comp + W_CONS * d.w_cons
    d["upside"] = d.tgt / close.iloc[i].reindex(d.index) - 1
    return st, d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-telegram", action="store_true")
    a = ap.parse_args()
    ss.STATE_DIR.mkdir(parents=True, exist_ok=True)

    D = ss.load()
    dates = D["ret"].index
    i = len(dates) - 1
    today = dates[i]
    sc, _, _ = ss.scores(D, i)
    cons, cons_asof = consensus(today)
    st, d = build(D, sc, i, cons)
    F, M, rx_asof, fm_hist = reflex_fm()
    active = F is not None and M is not None and F >= F_MIN and M >= M_MIN

    top_secs = list(st.index[:K])
    picks = [d[d.sec == s].score.idxmax() for s in top_secs if (d.sec == s).any()]

    state = json.loads(STATE.read_text(encoding="utf-8")) if STATE.exists() else {}
    already = state.get("last_run") == str(today.date())
    msgs = []

    # ── 장부: 어제까지의 보유로 오늘까지 성과 반영 ──
    led = pd.read_parquet(LEDGER) if LEDGER.exists() else pd.DataFrame()
    ret = D["ret"]
    cd = (D["cd"] / 100 / 252).fillna(0) if D["cd"] is not None else pd.Series(0.0, index=dates)
    kret = ss.kospi_ret(dates)
    w = D["cap"].shift(1)
    kret = kret.fillna(((ret * w).sum(axis=1) / w.where(ret.notna()).sum(axis=1)).fillna(0))
    if not already and len(led) and state:
        last = pd.Timestamp(led.date.max())
        hold = state.get("holdings", [])
        nav, navk, navg = (float(led[c].iloc[-1]) for c in ("nav", "nav_kospi", "nav_kospi_gated"))
        pend = float(state.get("pending_cost", 0.0))
        rows = []
        for dt in [x for x in dates if x > last]:
            r = ret.loc[dt].reindex(hold).fillna(0).mean() if hold else cd.loc[dt]
            r -= pend
            rg = kret.loc[dt] if hold else cd.loc[dt]
            pend = 0.0
            nav *= 1 + r; navk *= 1 + kret.loc[dt]; navg *= 1 + rg
            rows.append(dict(date=dt, n=len(hold), r=r, nav=nav, nav_kospi=navk, nav_kospi_gated=navg, active=bool(hold)))
        if rows:
            led = pd.concat([led, pd.DataFrame(rows)], ignore_index=True)
        state["pending_cost"] = 0.0

    # ── 오늘 결정 (다음 거래일부터 반영) ──
    prev = state.get("holdings", [])
    hold = prev
    if not already:
        new_month = state.get("rebal_date", "")[:7] != today.strftime("%Y-%m")
        if active and (not prev or new_month):
            hold = picks
        elif not active:
            hold = []
        buys = [t for t in hold if t not in prev]
        sells = [t for t in prev if t not in hold]
        n_prev, n_new = max(len(prev), 1), max(len(hold), 1)
        cost = (len(sells) / n_prev + len(buys) / n_new) * COST
        entry = {t: v for t, v in state.get("entry", {}).items() if t in hold}
        for t in buys:
            entry[t] = str(today.date())
        if buys or sells:
            state["rebal_date"] = str(today.date()) if hold else state.get("rebal_date", "")
            nm = D["names"]
            if hold and not prev:
                msgs.append(f"🎯 Focus 매수 ({today.date()}): " + ", ".join(nm.get(t, t) for t in hold)
                            + f" — Fundamentals {F:.0f} · Momentum {M:.0f}")
            elif not hold:
                msgs.append(f"⏸ Focus 전량 매도 ({today.date()}): 시장 조건 해제 (Fundamentals {F:.0f} · Momentum {M:.0f})")
            else:
                msgs.append(f"🔄 Focus 교체 ({today.date()}): 매수 " + ", ".join(nm.get(t, t) for t in buys)
                            + " / 매도 " + ", ".join(nm.get(t, t) for t in sells))
        state.update(holdings=hold, entry=entry, active=active, last_run=str(today.date()),
                     pending_cost=float(state.get("pending_cost", 0.0)) + cost, buys=buys, sells=sells)
        if led.empty:
            led = pd.DataFrame([dict(date=today, n=len(hold), r=0.0, nav=1.0, nav_kospi=1.0, nav_kospi_gated=1.0,
                                     active=bool(hold))])
            msgs.append(f"📒 Focus 가상 운용 시작 ({today.date()})")
        led.to_parquet(LEDGER)
        ss.atomic_write(STATE, json.dumps(state, ensure_ascii=False, indent=1))

    # ── JSON ──
    names = D["names"]
    entry = state.get("entry", {})

    def since(t):
        e = entry.get(t)
        if not e:
            return None
        x = ret.loc[ret.index > pd.Timestamp(e)].get(t)
        return ss.fnum((1 + x.fillna(0)).prod() - 1, 4) if x is not None and len(x) else 0.0

    def stock(t):
        r = d.loc[t]
        return dict(t=t, n=names.get(t, t), s=r.sec, score=ss.fnum(r.score), comp=ss.fnum(r.w_comp), cons=ss.fnum(r.w_cons),
                    q=ss.fnum(r.w_q), tr=ss.fnum(r.w_t), lv=ss.fnum(r.w_v), net_up=ss.fnum(r.net_up),
                    n_up=None if pd.isna(r.n_up) else int(r.n_up), n_dn=None if pd.isna(r.n_dn) else int(r.n_dn),
                    cov=None if pd.isna(r["cov"]) else int(r["cov"]), rev=ss.fnum(r.rev_mean, 4), upside=ss.fnum(r.upside, 3),
                    hi52=ss.fnum(r.hi52), r21=ss.fnum(r.r21, 4), eg=ss.fnum(r.eg1y), cap=ss.fnum(r.cap / 1e12, 2),
                    since=since(t), entry=entry.get(t))

    sectors = [dict(s=s, rank=int(r["rank"]), n=int(r.n), rs6=ss.fnum(r.rs6, 4), segE=ss.fnum(r.segE, 3), pick=s in top_secs)
               for s, r in st.head(10).iterrows()]
    cands = {s: [stock(t) for t in d[d.sec == s].sort_values("score", ascending=False).index[:5]] for s in top_secs}
    lg = led.copy()
    lg["date"] = pd.to_datetime(lg["date"]).dt.strftime("%Y-%m-%d")
    perf = None
    if len(lg) > 1:
        perf = dict(days=len(lg) - 1, focus=ss.fnum(lg.nav.iloc[-1] - 1, 4), kospi=ss.fnum(lg.nav_kospi.iloc[-1] - 1, 4),
                    gated=ss.fnum(lg.nav_kospi_gated.iloc[-1] - 1, 4), mdd=ss.fnum((lg.nav / lg.nav.cummax() - 1).min(), 4))
    out = dict(
        version=VERSION, as_of=str(today.date()), generated=datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        gate=dict(F=F, M=M, F_min=F_MIN, M_min=M_MIN, active=active, reflex_asof=rx_asof, history=fm_hist),
        status="hold" if state.get("holdings") else "wait",
        holdings=[stock(t) if t in d.index else dict(t=t, n=names.get(t, t), since=since(t), entry=entry.get(t))
                  for t in state.get("holdings", [])],
        picks=[stock(t) for t in picks], candidates=cands, sectors=sectors,
        rebal_date=state.get("rebal_date"), consensus_asof=str(cons_asof.date()) if cons_asof is not None else None,
        ledger=dict(start=lg.date.iloc[0] if len(lg) else None, perf=perf,
                    series=[[a_, ss.fnum(b_, 4), ss.fnum(c_, 4), ss.fnum(g_, 4), int(bool(h_))] for a_, b_, c_, g_, h_ in
                            zip(lg.date, lg.nav, lg.nav_kospi, lg.nav_kospi_gated, lg.active)]),
        params=dict(k=K, F_min=F_MIN, M_min=M_MIN, w_cons=W_CONS, min_sec=MIN_SEC, cost=COST),
        backtest=BACKTEST,
    )
    ss.atomic_write(OUT_JSON, json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    log(f"focus: {today.date()} | F {F} M {M} → {'조건 충족' if active else '대기'} | 업종 {top_secs} | "
        f"후보 {[names.get(t, t) for t in picks]} | 보유 {[names.get(t, t) for t in state.get('holdings', [])]}"
        + (f" | 투자의견 기준일 {cons_asof.date()}" if cons_asof is not None else ""))
    if msgs and not a.no_telegram:
        ss.telegram("\n".join(msgs))
    if a.push:
        ss.OUT_JSON = OUT_JSON
        ss.push()


if __name__ == "__main__":
    main()
