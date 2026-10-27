#!/usr/bin/env python3
"""
eqai_domestic.py — EQAI 프롬프트용 국내 시장 컨텍스트

EQAI(eqai.py)가 국내 판단에 쓰던 입력은 섹터 RRG 표 하나뿐이었다.
이 모듈은 이미 semon이 매일 만드는 데이터로 국내 판단 재료를 모은다.

  1. 시장 국면  — docs/data/reflex.json (Reflexivity 9단계, 형세·기세·편향, 구성 요소)
  2. 시장 폭    — data/reflex/breadth.parquet (이평선 위 비율, 상승/하락, 신고/신저, 거래대금)
  3. 시장 수급  — data/reflex/flows.parquet (외국인·기관 순매수)
  4. 섹터 현황  — docs/data/signals.json (5일 수익률, 상승 종목 비율, RRG 사분면)
  5. 종목 후보  — stock_quality.py '추세의 질' 점수 상위 + (있으면) data/stocks/flow 종목별 수급

모든 항목은 fail-open: 파일이 없거나 깨져도 그 블록만 빠지고 나머지는 동작한다.

사용:
    from eqai_domestic import domestic_context
    text, candidate_codes = domestic_context()
"""
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BASE = Path("/home/ubuntu/semon")
REFLEX_JSON = BASE / "docs/data/reflex.json"
SIGNALS_JSON = BASE / "docs/data/signals.json"
BREADTH = BASE / "data/reflex/breadth.parquet"
FLOWS = BASE / "data/reflex/flows.parquet"
STOCK_FLOW_DIR = BASE / "data/stocks/flow"

N_CANDIDATES = 40       # 종목 후보 수 (추세의 질 점수 상위)
PER_SECTOR = 5          # 섹터당 최대 후보 수 (한 섹터 쏠림 방지)
STAGE_EN = {1: "Inception", 2: "Acceleration", 3: "Confirmation", 4: "Conviction", 5: "Overshoot",
            6: "Reversal", 7: "Twilight", 8: "Deterioration", 9: "Capitulation"}


def _won(eok):
    """억원 값을 사람이 읽기 쉬운 문자열로: 1조 이상은 '−27.9조', 미만은 '−5,970억'."""
    if eok is None or (isinstance(eok, float) and np.isnan(eok)):
        return "—"
    return f"{eok / 1e4:+.1f}조" if abs(eok) >= 1e4 else f"{eok:+,.0f}억"


def _amt(eok):
    """부호 없는 금액(거래대금 등)."""
    if eok is None or (isinstance(eok, float) and np.isnan(eok)):
        return "—"
    return f"{eok / 1e4:.1f}조" if abs(eok) >= 1e4 else f"{eok:,.0f}억"


def _f(v, nd=1, sign=False):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    return f"{v:+.{nd}f}" if sign else f"{v:.{nd}f}"


# ── 1. 시장 국면 ─────────────────────────────────────────────────────
def regime_block():
    j = json.loads(REFLEX_JSON.read_text(encoding="utf-8"))
    st = j["stage"]
    lines = [f"기준일 {j['as_of']} 장 마감",
             f"현재 단계: {st['id']} {STAGE_EN.get(st['id'], '')}({st['name']}) — {st['desc']} / {st.get('since')}부터 {st.get('days')}거래일째"]
    if st.get("pending"):
        p = st["pending"]
        lines.append(f"전환 후보: {p['id']} {STAGE_EN.get(p['id'], '')}({p['name']}) {p['count']}/{p['need']}일 확인 중")
    lines.append(f"형세(Fundamentals) {j['F']['value']} (40일 변화 {_f(j['F'].get('d40'), 0, True)}) · "
                 f"기세(Momentum) {j['M']['value']} (20일 변화 {_f(j['M'].get('d20'), 0, True)}) · "
                 f"단기 기세 {j['Mf']['value']} · 편향(Bias, 기세−형세) {_f(j.get('gap'), 0, True)}")
    for ax, name in (("F", "형세"), ("M", "기세")):
        comps = j[ax].get("components", {})
        parts = [f"{c['label']} {_f(c.get('score'), 0)}" for c in comps.values()]
        lines.append(f"{name} 구성(백분위, 50=평소): " + ", ".join(parts))
    ex = j.get("phase_by_period") or []
    if ex:
        last = ex[-1]
        lines.append(f"참고 — {last['period']} 상승 국면(2~5단계) 이후 60일 평균 대비 {_f(last['up'].get('fwd60_excess'), 1, True)}%p, "
                     f"하락 국면(6~9단계) {_f(last['down'].get('fwd60_excess'), 1, True)}%p")
    return "\n".join(lines)


# ── 2. 시장 폭 ───────────────────────────────────────────────────────
def breadth_block():
    b = pd.read_parquet(BREADTH).sort_index()
    t, p5 = b.iloc[-1], b.iloc[-6] if len(b) > 5 else b.iloc[0]
    v20 = b["ALL_value"].iloc[-21:-1].mean()
    adv5 = b["ALL_adv"].iloc[-5:].sum()
    dec5 = b["ALL_dec"].iloc[-5:].sum()
    lines = [f"기준일 {b.index[-1].date()} (KOSPI+KOSDAQ 전 종목, 우선주 제외)"]
    for w in (20, 60, 200):
        lines.append(f"{w}일선 위 종목 비율 {t[f'ALL_above{w}']:.1f}% (5일 전 {p5[f'ALL_above{w}']:.1f}%) "
                     f"— KOSPI {t[f'KOSPI_above{w}']:.1f}% / KOSDAQ {t[f'KOSDAQ_above{w}']:.1f}%")
    lines.append(f"당일 상승 {t['ALL_adv']:.0f} / 하락 {t['ALL_dec']:.0f}, 최근 5일 누적 상승 {adv5:.0f} / 하락 {dec5:.0f}")
    lines.append(f"52주 신고가 {t['ALL_nh252']:.0f}종목 / 신저가 {t['ALL_nl252']:.0f}종목")
    lines.append(f"거래대금 {t['ALL_value'] / 1e4:.1f}조 (직전 20일 평균 대비 {t['ALL_value'] / v20:.2f}배)")
    return "\n".join(lines)


# ── 3. 시장 수급 ─────────────────────────────────────────────────────
def flows_block():
    f = pd.read_parquet(FLOWS).sort_index().dropna(how="all")
    lines = [f"기준일 {f.index[-1].date()} (+ 순매수 / − 순매도)"]
    for mkt in ("KOSPI", "KOSDAQ"):
        row = []
        for inv, name in (("foreign", "외국인"), ("inst", "기관"), ("retail", "개인")):
            c = f"{mkt}_{inv}"
            if c not in f.columns:
                continue
            s = f[c] / 1e8
            row.append(f"{name} 1일 {_won(s.iloc[-1])} · 5일 {_won(s.iloc[-5:].sum())} · 20일 {_won(s.iloc[-20:].sum())}")
        lines.append(f"{mkt}: " + " | ".join(row))
    return "\n".join(lines)


# ── 4·5. 섹터 현황과 종목 후보 ──────────────────────────────────────
def _stock_flows():
    """종목별 외국인+기관 5일·20일 순매수(억원). data/stocks/flow 없으면 빈 dict."""
    files = sorted(STOCK_FLOW_DIR.glob("*.parquet"))[-2:]
    if not files:
        return {}
    df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    days = sorted(df["date"].unique())
    if len(days) < 5 or (pd.Timestamp.now() - pd.Timestamp(days[-1])).days > 7:
        return {}   # 백필 중이거나 갱신이 멈춘 경우: 오래된 수급을 최근 것으로 쓰지 않음
    df["net"] = (df["foreign"] + df["inst"]) / 1e8
    d5 = df[df["date"].isin(days[-5:])].groupby("ticker")["net"].sum()
    d20 = df[df["date"].isin(days[-20:])].groupby("ticker")["net"].sum()
    return {t: (d5.get(t, 0.0), d20.get(t, 0.0)) for t in set(d5.index) | set(d20.index)}


def sectors_and_candidates():
    """섹터 현황 + 종목 후보.
    후보는 '추세의 질' 점수(stock_quality.py: 낮은 변동성·52주 고점 근접, 국면별 비중) 상위.
    2011~2026 검증에서 단기 급등·RRG 상대강도 상위는 이후 덜 올랐으므로 후보 선정에 쓰지 않는다."""
    from stock_quality import quality_scores
    sig = json.loads(SIGNALS_JSON.read_text(encoding="utf-8"))
    sectors = {k: v for k, v in sig.get("sectors", {}).items() if k != "미분류"}
    srrg = sig.get("sector_rrg", {})
    # 여러 섹터에 속한 종목은 목록 내 상대 위치가 가장 앞인(핵심 구성원에 가까운) 섹터로 표시
    info, pos = {}, {}
    for sname, s in sectors.items():
        cands = s.get("candidates", [])
        for i, x in enumerate(cands):
            rel = i / max(len(cands), 1)
            if x["code"] not in pos or rel < pos[x["code"]]:
                pos[x["code"]] = rel
                info[x["code"]] = {**x, "sector": sname}

    q = quality_scores()
    meta = q.attrs
    q = q[q.index.isin(info.keys())].copy()
    q["sector"] = [info[c]["sector"] for c in q.index]
    q["name"] = [info[c]["name"] for c in q.index]

    # 섹터 현황: 섹터 내 유니버스 종목의 점수 중앙값 순 (5일 수익률·RRG는 참고)
    rows = []
    for sname, s in sectors.items():
        qs = q[q["sector"] == sname]
        rows.append({"sector": sname, "score": qs["score"].median() if len(qs) else np.nan, "n": len(qs),
                     "r5": s.get("sector_ret_5d"), "rise": s.get("rising_ratio"),
                     "r60": qs["r60"].median() if len(qs) else np.nan,
                     "q": srrg.get(sname, {}).get("quadrant", "")})
    sec = pd.DataFrame(rows).sort_values("score", ascending=False, na_position="last")
    sec_lines = ["섹터 | 추세의 질 점수(중앙값) | 유니버스 종목 수 | 5일 | 60일(중앙값) | 상승 종목 비율 | RRG 사분면(참고)"]
    for r in sec.itertuples():
        sec_lines.append(f"{r.sector} | {_f(r.score, 0)} | {r.n} | {_f(r.r5, 1, True)}% | {_f(r.r60, 1, True)}% | "
                         f"{(r.rise or 0) * 100:.0f}% | {r.q}")

    sflow = {}
    try:
        sflow = _stock_flows()
    except Exception as e:
        logger.warning(f"종목별 수급 로드 실패(무시): {e}")

    picked, per = [], {}
    for code, r in q.iterrows():
        if per.get(r["sector"], 0) >= PER_SECTOR:
            continue
        picked.append(code)
        per[r["sector"]] = per.get(r["sector"], 0) + 1
        if len(picked) >= N_CANDIDATES:
            break

    head = ("종목(코드) | 섹터 | 점수 | 변동성(60일, 연율) | 52주 고점 대비 | 20일 | 60일 | 거래대금(20일 평균) | RRG(참고)"
            + (" | 외국인+기관 5일/20일" if sflow else ""))
    lines = [head]
    for code in picked:
        r = q.loc[code]
        line = (f"{r['name']}({code}) | {r['sector']} | {r['score']:.0f} | {r['vol60']:.0f}% | {(r['hi52'] - 1) * 100:+.1f}% | "
                f"{r['r20']:+.1f}% | {r['r60']:+.1f}% | {_amt(r['avgval'])} | {info[code].get('quadrant', '')}")
        if sflow:
            a, b = sflow.get(code, (0.0, 0.0))
            line += f" | {_won(a)} / {_won(b)}"
        lines.append(line)
    note = (f"점수 = 낮은 변동성 백분위 × {meta['w_lowvol']:.2f} + 52주 고점 근접 백분위 × {1 - meta['w_lowvol']:.2f} "
            f"(현재 국면 {meta['phase']}, 기준일 {meta['as_of']}, 거래대금 상위 {meta['universe']}종목 안에서 계산). "
            "2011~2026 검증: 점수 상위 30종목의 60일 후 초과수익 평균 +2.2%, 승률 63%. "
            "같은 검증에서 최근 20~60일 급등 종목과 RRG 상대강도 상위 종목은 이후 덜 올랐음.")
    return "\n".join(sec_lines), note + "\n" + "\n".join(lines), set(picked), sig.get("updated_at", "")


def domestic_context():
    """(프롬프트용 국내 컨텍스트 텍스트, 후보 종목 코드 집합)."""
    blocks, codes = [], set()
    for title, fn in (("국내 시장 국면 (Reflexivity 9단계)", regime_block),
                      ("국내 시장 폭", breadth_block),
                      ("국내 시장 수급 (투자자별 순매수)", flows_block)):
        try:
            blocks.append(f"=== {title} ===\n{fn()}")
        except Exception as e:
            logger.warning(f"{title} 생성 실패(생략): {e}")
    try:
        sec_text, cand_text, codes, upd = sectors_and_candidates()
        blocks.append(f"=== 섹터 현황 (추세의 질 점수 순, signals.json {upd}) ===\n{sec_text}")
        blocks.append(f"=== 종목 후보 (추세의 질 점수 상위 {N_CANDIDATES}, 섹터당 최대 {PER_SECTOR}) ===\n{cand_text}")
    except Exception as e:
        logger.warning(f"섹터·후보 생성 실패(생략): {e}")
    logger.info(f"국내 컨텍스트: 블록 {len(blocks)}개, 후보 {len(codes)}종목")
    return "\n\n".join(blocks), codes


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    text, codes = domestic_context()
    print(text)
    print(f"\n후보 {len(codes)}종목, {len(text)}자")
