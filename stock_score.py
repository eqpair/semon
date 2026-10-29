#!/usr/bin/env python3
"""
stock_score.py — 형세·기세 종목 엔진 (Picks)

매일 장 마감 데이터로:
  1) 시장 국면: Reflex 조합 평균 노출(docs/data/regime.json, reflex_ensemble.py) — 2,430개 기준값 조합 중
     Risk-On(1~5단계) 비율을 주식 비중으로 사용 (10%p 이상 바뀔 때만 조정). 70% 이상 Risk-On, 30% 이하 Risk-Off
  2) 종목 점수: 거래대금 상위 600종목(우선주 제외)
       형세(Position) = 흑자 여부, 이익수익률(E/P), 1년 EPS 증가율, 60일 외국인+기관 순매수/시총, 소속 업종 주도권
       기세(Momentum) = 52주 고점 근접도, 최근 1개월 급등 회피(−1개월 수익률)
       안정성(Stability) = 60일 변동성 낮음
       종합 = 세 점수의 평균 (각 지표는 그날 600종목 안의 순위 백분위)
  3) 포트폴리오: 매월 첫 거래일에 종합 상위 50종목으로 교체(기존 보유가 100위 안이면 유지).
     Risk-On이면 업종 주도권 하위 5개 업종 종목 제외. 한 업종 최대 10종목(금융계는 하나로 묶음).
     권장 배분: 주식 비중 × (코스피 200 ETF 50% + 50종목 50%), 나머지 현금(단기채)
  4) 가상 운용 장부: 위 규칙을 그대로 따른 일별 성과를 data/picks/ledger.parquet에 누적
  5) docs/data/picks.json 저장, --push면 GitHub에 반영. 리밸런싱·국면 전환 때 텔레그램 알림.

백테스트(2011~2026, 거래비용 왕복 0.5%)와 같은 계산식을 씁니다. 매매 신호가 아니라 참고용입니다.
사용법: python stock_score.py [--push] [--no-telegram]
"""
import os
import re
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
KRX_DIR = BASE / "data" / "reflex" / "krx"
ST = BASE / "data" / "stocks"
KR_DAILY = BASE / "data" / "reflex" / "kr_daily.parquet"
REFLEX_JSON = BASE / "docs" / "data" / "reflex.json"
REGIME_JSON = BASE / "docs" / "data" / "regime.json"     # reflex_ensemble.py 산출 (조합 평균 노출)
MACRO = BASE / "data" / "reflex" / "macro.parquet"        # 코스피 지수 (지수 ETF 성과 기준)
STATE_DIR = BASE / "data" / "picks"
OUT_JSON = BASE / "docs" / "data" / "picks.json"
VERSION = "picks_v2"

UNIVERSE = 600
N_HOLD = 50
BUFFER = 2.0          # 기존 보유 종목은 순위 100위 안이면 유지
EXCL_SECTORS = 5      # Risk-On일 때 주도권 하위 업종 수
MAX_PER_GROUP = 10    # 한 업종(금융·보험·은행·증권은 하나로 묶음)에서 최대 10종목
FIN = {'금융': '금융계', '보험': '금융계', '은행': '금융계', '증권': '금융계'}
CORE = 0.5            # Risk-On 때 시장 지수 비중
COST_STOCK = 0.0025   # 편도 (세금·수수료·슬리피지)
COST_INDEX = 0.0005
UP_STAGES = {1, 2, 3, 4, 5}
EXP_BAND = 0.10       # 노출 비중은 10%p 이상 바뀔 때만 조정 (0%·100% 도달은 즉시)
INDEX_ETF = [dict(t="069500", n="KODEX 200"), dict(t="102110", n="TIGER 200")]
LOOKBACK = 330        # 필요한 거래일 수 (252일 + 여유)

UNI = {'반도체': '전기전자', 'IT부품': '전기전자', '통신장비': '전기전자', '정보기기': '전기전자', '전기·전자': '전기전자',
       'IT 서비스': 'IT서비스', '소프트웨어': 'IT서비스', '컴퓨터서비스': 'IT서비스', '인터넷': 'IT서비스', '디지털컨텐츠': 'IT서비스',
       '통신서비스': '통신', '방송서비스': '오락·문화', '출판·매체복제': '오락·문화', '기타금융': '금융',
       '전기·가스': '유틸리티', '전기·가스·수도': '유틸리티', '광업': '기타제조', '농업 임업 및 어업': '음식료·담배', '부동산': '건설'}


import re as _re
_FIN = _re.compile("금융|카드|페이|캐피탈|인프라|증권")


def unify_sector(industry, name="", ticker=""):
    """KRX 업종 → 묶은 업종. 기타금융은 금융지주·카드 등은 '금융', 나머지(일반 지주회사)는 '지주'."""
    if industry == "기타금융":
        return "금융" if (_FIN.search(str(name)) or ticker == "055550") else "지주"
    return UNI.get(industry, industry)


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


def atomic_write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def read_years(d, years, cols=None):
    fr = []
    for y in years:
        f = d / f"{y}.parquet"
        if f.exists():
            fr.append(pd.read_parquet(f, columns=cols))
    return pd.concat(fr, ignore_index=True) if fr else pd.DataFrame()


# ── 데이터 ───────────────────────────────────────────────────────────
def load():
    years = sorted(int(f.stem) for f in KRX_DIR.glob("*.parquet"))
    use = [y for y in years if y >= years[-1] - 2]
    px = read_years(KRX_DIR, use, ["ticker", "chg", "cap", "value", "close", "date"])
    px["date"] = pd.to_datetime(px["date"]).astype("datetime64[ns]")
    px = px[px.ticker.astype(str).str[-1] == "0"]
    dates = pd.DatetimeIndex(sorted(px.date.unique()))[-LOOKBACK:]
    px = px[px.date.isin(dates)]

    def P(col):
        return px.pivot_table(index="date", columns="ticker", values=col, aggfunc="last").reindex(dates)
    ret = (P("chg") / 100).clip(-0.31, 0.31)
    cap, val, close = P("cap"), P("value"), P("close")
    T = ret.columns

    fl = read_years(ST / "flow", use)
    flow_asof = None
    if len(fl):
        fl["date"] = pd.to_datetime(fl["date"]).astype("datetime64[ns]")
        flow_asof = fl.date.max()
        fl = fl[fl.ticker.isin(T)]
        fo = fl.pivot_table(index="date", columns="ticker", values="foreign", aggfunc="last").reindex(index=dates, columns=T)
        ins = fl.pivot_table(index="date", columns="ticker", values="inst", aggfunc="last").reindex(index=dates, columns=T)
    else:
        fo = ins = pd.DataFrame(np.nan, index=dates, columns=T)

    fd = read_years(ST / "fund", use)
    fd["date"] = pd.to_datetime(fd["date"]).astype("datetime64[ns]")
    fund_asof = fd.date.max()
    fd = fd[fd.ticker.isin(T)]

    def F(col):
        x = fd.pivot_table(index="date", columns="ticker", values=col, aggfunc="last").reindex(columns=T)
        return x.reindex(dates.union(x.index)).ffill(limit=30).reindex(dates)
    eps = F("EPS")

    sp = pd.read_parquet(ST / "sector" / "sector_pit.parquet")
    sp["date"] = pd.to_datetime(sp["date"]).astype("datetime64[ns]")
    sector_asof = sp.date.max()
    last = sp[sp.date == sector_asof]
    names = dict(zip(sp.sort_values("date").ticker, sp.sort_values("date").name))
    sec_raw = last.set_index("ticker").industry
    sec = pd.Series({t: unify_sector(v, names.get(t, ""), t) for t, v in sec_raw[sec_raw != "기타"].items()}, dtype=object)

    cd = None
    if KR_DAILY.exists():
        k = pd.read_parquet(KR_DAILY)
        if "CD91" in k.columns:
            cd = k["CD91"].reindex(dates.union(k.index)).ffill().reindex(dates)
    return dict(ret=ret, cap=cap, val=val, close=close, fo=fo, ins=ins, eps=eps, sec=sec, names=names, cd=cd,
                asof=dict(price=dates[-1], flow=flow_asof, fund=fund_asof, sector=sector_asof))


def z(s):
    return s.rank(pct=True) - 0.5


def scores(D, i):
    """i번째 날짜 기준 600종목 점수 (백테스트 bt_prep.py와 같은 식)."""
    ret, cap, val, close, fo, ins, eps, sec = (D[k] for k in "ret cap val close fo ins eps sec".split())
    idx = (1 + ret.fillna(0)).cumprod()
    liq = val.rolling(20, min_periods=15).mean()
    hi = idx.rolling(252, min_periods=200).max()
    vol60 = ret.rolling(60, min_periods=40).std()
    fl60 = (fo.fillna(0) + ins.fillna(0)).rolling(60).sum()
    tk = liq.iloc[i].dropna().sort_values(ascending=False).index[:UNIVERSE]
    d = pd.DataFrame(index=tk)
    d["sec"] = sec.reindex(tk)
    d["cap"] = cap.iloc[i][tk]
    d["r21"] = idx.iloc[i][tk] / idx.iloc[i - 21][tk] - 1
    d["r252x"] = idx.iloc[i - 21][tk] / idx.iloc[i - 252][tk] - 1
    d["hi52"] = idx.iloc[i][tk] / hi.iloc[i][tk]
    d["nh"] = (d.hi52 >= 0.98).astype(float)
    d["nvol"] = -vol60.iloc[i][tk]
    d["fl60"] = fl60.iloc[i][tk] / d.cap
    e0, e1 = eps.iloc[i][tk], eps.iloc[i - 252][tk]
    d["epos"] = (e0 > 0).astype(float).where(e0.notna())
    d["ep"] = (e0 / close.iloc[i][tk]).clip(-1, 1)
    d["eg1y"] = ((e0 - e1) / e1.abs().clip(lower=1)).clip(-3, 3)
    g = d.dropna(subset=["sec"]).groupby("sec")
    dd = d.dropna(subset=["sec"])  # 업종 시총가중 12개월 수익 (groupby.apply 경고 없이 같은 값)
    sl = pd.DataFrame({"r": (dd.r252x * dd.cap).groupby(dd.sec).sum() / dd.cap.groupby(dd.sec).sum(), "nh": g.nh.mean(), "n": g.size()})
    sl = sl[sl.n >= 5]
    sl["lead"] = (z(sl.r) + z(sl.nh)) / 2
    d["lead"] = d.sec.map(sl.lead)
    d["secrank"] = d.sec.map(sl.lead.rank(ascending=True))
    d["nr21"] = -d.r21
    Z = {f: z(d[f]).fillna(0) for f in ["epos", "ep", "eg1y", "fl60", "lead", "hi52", "nr21", "nvol"]}
    d["HS"] = (Z["epos"] + Z["ep"] + Z["eg1y"] + Z["fl60"] + Z["lead"]) / 5
    d["GS"] = (Z["hi52"] + Z["nr21"]) / 2
    d["RK"] = Z["nvol"]
    d["ALL"] = (d.HS + d.GS + d.RK) / 3
    d["rank"] = d.ALL.rank(ascending=False, method="first").astype(int)
    sl["rank"] = sl.lead.rank(ascending=False, method="first").astype(int)
    return d.sort_values("rank"), sl.sort_values("rank"), idx


def ensemble():
    """조합 평균 노출 (reflex_ensemble.py). 없거나 5일 넘게 오래되면 None."""
    try:
        j = json.loads(REGIME_JSON.read_text(encoding="utf-8"))
        if (pd.Timestamp.now() - pd.Timestamp(j["as_of"])).days > 5:
            log(f"regime.json 오래됨 ({j['as_of']}) — Reflex 단계로 대체")
            return None
        return j
    except Exception as e:
        log(f"regime.json 읽기 실패: {e!r} — Reflex 단계로 대체")
        return None


def kospi_ret(dates):
    try:
        k = pd.read_parquet(MACRO)["코스피"].dropna()
        k.index = pd.to_datetime(k.index).astype("datetime64[ns]")
        # 지수 데이터에 빠진 날은 전일 값 유지(그날 0%, 다음 날에 합산) — 같은 움직임을 두 번 세지 않도록
        return k.reindex(k.index.union(dates)).ffill().reindex(dates).pct_change()
    except Exception:
        return pd.Series(float("nan"), index=dates)


def reflex_stage():
    try:
        j = json.loads(REFLEX_JSON.read_text(encoding="utf-8"))
        st = j.get("stage") or {}
        return st.get("id"), st.get("name"), j.get("as_of") or j.get("asof")
    except Exception as e:
        log(f"reflex.json 읽기 실패: {e!r}")
        return None, None, None


# ── 상태·장부 ────────────────────────────────────────────────────────
def load_state():
    p = STATE_DIR / "state.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def update_ledger(D, state):
    """마지막 장부 날짜 이후의 거래일마다 어제까지 정한 비중으로 하루 성과를 계산."""
    path = STATE_DIR / "ledger.parquet"
    led = pd.read_parquet(path) if path.exists() else pd.DataFrame()
    ret, cap = D["ret"], D["cap"]
    w = cap.shift(1)
    mret = ((ret * w).sum(axis=1) / w.where(ret.notna()).sum(axis=1)).fillna(0)
    cd = (D["cd"] / 100 / 252).fillna(0) if D["cd"] is not None else pd.Series(0.0, index=ret.index)
    kret = kospi_ret(ret.index).fillna(mret)      # 지수 ETF 부분 = 코스피 (없는 날은 시장 시총가중으로 대체)
    if led.empty or not state:
        return led, mret
    last = pd.Timestamp(led.date.max())
    new = [d for d in ret.index if d > last]
    rows = []
    nav_s, nav_m = float(led.nav_strat.iloc[-1]), float(led.nav_mkt.iloc[-1])
    exp = float(state.get("exp", 0.0))
    hold = state.get("holdings", [])
    pend_cost = float(state.get("pending_cost", 0.0))
    for d in new:
        rs = ret.loc[d].reindex(hold).fillna(0).mean() if hold else 0.0
        r = exp * (CORE * kret.loc[d] + (1 - CORE) * rs) + (1 - exp) * cd.loc[d] - pend_cost
        pend_cost = 0.0
        nav_s *= 1 + r
        nav_m *= 1 + kret.loc[d]
        rows.append(dict(date=d, exp=exp, n_hold=len(hold), r_strat=r, r_mkt=kret.loc[d], nav_strat=nav_s, nav_mkt=nav_m,
                         stage=state.get("stage")))
    if rows:
        led = pd.concat([led, pd.DataFrame(rows)], ignore_index=True)
    state["pending_cost"] = 0.0
    return led, mret


# ── 텔레그램·푸시 ─────────────────────────────────────────────────────
def telegram(msg):
    env = BASE / ".env"
    if not env.exists():
        return
    t = env.read_text(encoding="utf-8")
    tok = re.search(r"TELEGRAM_TOKEN=['\"]?([^'\"\s]+)", t)
    cid = re.search(r"TELEGRAM_CHAT_ID=['\"]?([^'\"\s]+)", t)
    if not (tok and cid):
        return
    try:
        import requests
        requests.post(f"https://api.telegram.org/bot{tok.group(1)}/sendMessage",
                      data={"chat_id": cid.group(1), "text": msg}, timeout=10)
    except Exception as e:
        log(f"telegram 실패: {e!r}")


def push():
    def run(*cmd):
        return subprocess.run(cmd, cwd=BASE, capture_output=True, text=True)
    rel = str(OUT_JSON.relative_to(BASE))
    run("git", "add", rel)
    if run("git", "diff", "--cached", "--quiet", "--", rel).returncode == 0:
        log("push: 변경 없음")
        return
    run("git", "commit", "-m", f"picks: {datetime.now(KST):%Y-%m-%d %H:%M}", "--", rel)
    for _ in range(3):
        r = run("git", "pull", "--rebase", "--autostash")
        if r.returncode != 0:
            log("push: pull --rebase 실패", r.stderr[-300:])
            run("git", "rebase", "--abort")
            continue
        r = run("git", "push")
        if r.returncode == 0:
            log("push: 완료")
            return
        log(f"push: 실패 {r.stderr[-300:]}")
    log("push: 3회 실패 — 다음 실행 때 다시 시도")


# ── 메인 ─────────────────────────────────────────────────────────────
def fnum(v, nd=3):
    return None if v is None or (isinstance(v, float) and not np.isfinite(v)) else round(float(v), nd)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--push", action="store_true")
    ap.add_argument("--no-telegram", action="store_true")
    a = ap.parse_args()
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    D = load()
    dates = D["ret"].index
    i = len(dates) - 1
    today = dates[i]
    if i < 260:
        raise SystemExit("가격 데이터가 260거래일보다 짧습니다")
    sc, sl, idx = scores(D, i)
    stage, stage_name, reflex_asof = reflex_stage()
    state = load_state()
    msgs = []

    # 이미 처리한 날이면 장부·포트폴리오는 그대로 두고 화면만 갱신
    already = state.get("last_run") == str(today.date())
    led, mret = update_ledger(D, state) if not already else (pd.read_parquet(STATE_DIR / "ledger.parquet")
                                                             if (STATE_DIR / "ledger.parquet").exists() else pd.DataFrame(), None)

    ens = ensemble()
    old_exp = float(state.get("exp", 0.0)) if state else None
    if ens is not None:
        target = float(ens["exposure"])
    else:
        target = 1.0 if ((stage in UP_STAGES) if stage is not None else bool(state.get("up", True))) else 0.0
    target = round(target, 2)
    if old_exp is None or abs(target - old_exp) >= EXP_BAND or (target in (0.0, 1.0) and target != old_exp):
        exp_new = target
    else:
        exp_new = old_exp                      # 작은 변화는 매매하지 않음
    up = exp_new >= 0.5                         # 업종 하위 제외 등은 노출 50% 이상일 때
    reg_label = "Risk-On" if exp_new >= 0.7 else "Risk-Off" if exp_new <= 0.3 else "Neutral"
    prev_hold = state.get("holdings", [])
    rebal_month = state.get("rebal_date", "")[:7]
    need_rebal = (not prev_hold) or rebal_month != today.strftime("%Y-%m")
    added, removed = state.get("added", []), state.get("removed", [])
    hold = prev_hold
    if not already and need_rebal:
        cand = sc.copy()
        if up:
            cand = cand[~(cand.secrank <= EXCL_SECTORS)]
        top = set(cand.index[:int(N_HOLD * BUFFER)])
        keep = [t for t in prev_hold if t in top]
        order = keep + [t for t in cand.index if t not in keep]
        cnt, hold = {}, []
        for t in order:
            g = cand.sec.get(t) if t in cand.index else sc.sec.get(t)
            g = FIN.get(g, g) if isinstance(g, str) else "기타"
            if cnt.get(g, 0) >= MAX_PER_GROUP:
                continue
            cnt[g] = cnt.get(g, 0) + 1
            hold.append(t)
            if len(hold) >= N_HOLD:
                break
        added = [t for t in hold if t not in prev_hold]
        removed = [t for t in prev_hold if t not in hold]
        state["rebal_date"] = str(today.date())
        if prev_hold:
            msgs.append(f"🔄 Picks 월간 교체 ({today.date()}): 편입 {len(added)} · 편출 {len(removed)}")
    # 비용: 종목 교체분 + 국면 전환에 따른 비중 변화 (다음 날 장부에서 차감)
    if not already:
        old_exp = float(state.get("exp", 0.0)) if state else 0.0
        churn = (len(added) / N_HOLD if (need_rebal and prev_hold) else 0.0)
        cost = 0.0
        if state:
            cost += exp_new * (1 - CORE) * churn * 2 * COST_STOCK
            cost += abs(exp_new - old_exp) * ((1 - CORE) * COST_STOCK + CORE * COST_INDEX)
        if state and abs(exp_new - old_exp) >= EXP_BAND - 1e-9:
            icon = {"Risk-On": "🟢", "Neutral": "🟡", "Risk-Off": "🔵"}[reg_label]
            msgs.append(f"{icon} Picks 주식 비중 {old_exp:.0%} → {exp_new:.0%} ({reg_label}) — "
                        f"지수 ETF {exp_new * CORE:.0%} · 종목 {exp_new * (1 - CORE):.0%} · 현금 {1 - exp_new:.0%}"
                        + (f" / Reflex {stage} {stage_name or ''}" if stage else ""))
        state.update(exp=exp_new, up=up, label=reg_label, stage=stage, holdings=hold, added=added, removed=removed,
                     pending_cost=float(state.get("pending_cost", 0.0)) + cost, last_run=str(today.date()))
        if led.empty:
            led = pd.DataFrame([dict(date=today, exp=exp_new, n_hold=len(hold), r_strat=0.0, r_mkt=0.0,
                                     nav_strat=1.0, nav_mkt=1.0, stage=stage)])
            msgs.append(f"📒 Picks 가상 운용 시작 ({today.date()})")
        led.to_parquet(STATE_DIR / "ledger.parquet")
        atomic_write(STATE_DIR / "state.json", json.dumps(state, ensure_ascii=False, indent=1))

    # ── JSON ──
    names = D["names"]
    rb = pd.Timestamp(state.get("rebal_date") or today)
    since = D["ret"].loc[D["ret"].index > rb].fillna(0)
    cum = (1 + since).prod() - 1 if len(since) else pd.Series(0.0, index=D["ret"].columns)

    def row(t, r):
        return dict(t=t, n=names.get(t, t), s=r.get("sec") if isinstance(r.get("sec"), str) else None,
                    rank=int(r["rank"]) if r is not None and pd.notna(r.get("rank")) else None,
                    all=fnum(r.get("ALL")), hs=fnum(r.get("HS")), gs=fnum(r.get("GS")), rk=fnum(r.get("RK")),
                    hi52=fnum(r.get("hi52")), r21=fnum(r.get("r21")), eg=fnum(r.get("eg1y")),
                    ret=fnum(cum.get(t), 4) if t in hold else None)

    holdings = []
    for t in hold:
        r = sc.loc[t] if t in sc.index else pd.Series(dtype=object)
        holdings.append(row(t, r))
    holdings.sort(key=lambda x: (x["rank"] is None, x["rank"] or 0))
    ranking = [row(t, sc.loc[t]) for t in sc.index[:100]]
    excl = set(sl.sort_values("lead").index[:EXCL_SECTORS])
    sectors = [dict(s=s, rank=int(r["rank"]), n=int(r.n), r12=fnum(r.r, 4), nh=fnum(r.nh, 3), lead=fnum(r.lead, 3),
                    excl=s in excl) for s, r in sl.iterrows()]
    lg = led.copy()
    lg["date"] = pd.to_datetime(lg["date"]).dt.strftime("%Y-%m-%d")
    perf = None
    if len(lg) > 1:
        perf = dict(days=len(lg) - 1, strat=fnum(lg.nav_strat.iloc[-1] - 1, 4), mkt=fnum(lg.nav_mkt.iloc[-1] - 1, 4),
                    mdd=fnum((lg.nav_strat / lg.nav_strat.cummax() - 1).min(), 4),
                    mdd_mkt=fnum((lg.nav_mkt / lg.nav_mkt.cummax() - 1).min(), 4))
    asof = {k: (str(pd.Timestamp(v).date()) if v is not None else None) for k, v in D["asof"].items()}
    stale = [k for k in ("flow", "fund") if asof[k] and (pd.Timestamp(asof["price"]) - pd.Timestamp(asof[k])).days > 10]
    out = dict(
        version=VERSION, as_of=str(today.date()), generated=datetime.now(KST).strftime("%Y-%m-%d %H:%M"),
        data_asof=asof, stale=stale,
        regime=dict(stage=stage, name=stage_name, reflex_asof=reflex_asof, up=up,
                    exposure=fnum(float(state.get("exp", exp_new)), 2), target=fnum(target, 3),
                    label=state.get("label", reg_label), source="ensemble" if ens is not None else "stage",
                    ens=dict(as_of=ens["as_of"], n_up=ens["n_up"], n=ens["n"],
                             history=ens.get("history", [])[-120:]) if ens is not None else None,
                    alloc=dict(index=fnum(float(state.get("exp", exp_new)) * CORE, 3),
                               stocks=fnum(float(state.get("exp", exp_new)) * (1 - CORE), 3),
                               cash=fnum(1 - float(state.get("exp", exp_new)), 3)),
                    index_etf=INDEX_ETF),
        portfolio=dict(rebal_date=state.get("rebal_date"), n=len(hold), holdings=holdings,
                       added=[dict(t=t, n=names.get(t, t)) for t in added],
                       removed=[dict(t=t, n=names.get(t, t)) for t in removed]),
        ranking=ranking, sectors=sectors,
        ledger=dict(start=lg.date.iloc[0] if len(lg) else None, perf=perf,
                    series=[[d, fnum(s, 4), fnum(m, 4), fnum(e, 2)] for d, s, m, e in
                            zip(lg.date, lg.nav_strat, lg.nav_mkt, lg.exp)]),
        params=dict(universe=UNIVERSE, n=N_HOLD, buffer=int(N_HOLD * BUFFER), excl=EXCL_SECTORS, core=CORE, max_group=MAX_PER_GROUP),
    )
    atomic_write(OUT_JSON, json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    log(f"picks: {today.date()} | Reflex {stage} {stage_name} | 노출 {float(state.get('exp', exp_new)):.0%} (목표 {target:.0%}, {state.get('label', reg_label)}) | 보유 {len(hold)} "
        f"(리밸런싱 {state.get('rebal_date')}) | 1위 {ranking[0]['n'] if ranking else '-'}" + (f" | 오래된 데이터 {stale}" if stale else ""))
    if stale:
        msgs.append(f"⚠️ Picks 데이터 지연: {', '.join(stale)} 기준일 {[asof[k] for k in stale]}")
    if msgs and not a.no_telegram:
        telegram("\n".join(msgs))
    if a.push:
        push()


if __name__ == "__main__":
    main()
