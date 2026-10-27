#!/usr/bin/env python3
"""
eqai_domestic.py — EQAI 프롬프트용 국내 시장 컨텍스트

EQAI(eqai.py)가 국내 판단에 쓰던 입력은 섹터 RRG 표 하나뿐이었다.
이 모듈은 이미 semon이 매일 만드는 데이터로 국내 판단 재료를 모은다.

  1. 시장 국면  — docs/data/reflex.json (Reflexivity 9단계, 형세·기세·편향, 구성 요소)
  2. 시장 폭    — data/reflex/breadth.parquet (이평선 위 비율, 상승/하락, 신고/신저, 거래대금)
  3. 시장 수급  — data/reflex/flows.parquet (외국인·기관 순매수)
  4. 섹터 현황  — docs/data/signals.json (5일 수익률, 상승 종목 비율, RRG 사분면)
  5. 종목 후보  — signals.json 종목 + (있으면) data/stocks/flow 종목별 수급

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

N_SECTORS = 15          # 후보를 뽑을 섹터 수 (섹터 5일 수익률·상승 비율 기준 상위)
PER_SECTOR = 4          # 섹터당 후보 종목 수 (거래대금 상위)
STAGE_EN = {1: "Inception", 2: "Acceleration", 3: "Confirmation", 4: "Conviction", 5: "Overshoot",
            6: "Reversal", 7: "Twilight", 8: "Deterioration", 9: "Capitulation"}


def _won(eok):
    """억원 값을 사람이 읽기 쉬운 문자열로: 1조 이상은 '−27.9조', 미만은 '−5,970억'."""
    if eok is None or (isinstance(eok, float) and np.isnan(eok)):
        return "—"
    return f"{eok / 1e4:+.1f}조" if abs(eok) >= 1e4 else f"{eok:+,.0f}억"


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
    sig = json.loads(SIGNALS_JSON.read_text(encoding="utf-8"))
    sectors = {k: v for k, v in sig.get("sectors", {}).items() if k != "미분류"}
    srrg = sig.get("sector_rrg", {})

    rows = []
    for name, s in sectors.items():
        c = s.get("candidates", [])
        r20 = [x["ret_20d"] for x in c if x.get("ret_20d") is not None]
        rows.append({"sector": name, "r1": s.get("sector_ret_1d"), "r5": s.get("sector_ret_5d"),
                     "r20": float(np.median(r20)) if r20 else None, "rise": s.get("rising_ratio"),
                     "q": srrg.get(name, {}).get("quadrant", ""), "n": len(c)})
    sec = pd.DataFrame(rows)
    # 섹터 순위: 5일 수익률과 상승 종목 비율의 평균 순위 (RRG는 참고 정보로만 표시)
    sec["rank"] = sec["r5"].rank(ascending=False) + sec["rise"].rank(ascending=False)
    sec = sec.sort_values("rank")

    sec_lines = ["섹터 | 1일 | 5일 | 20일(종목 중앙값) | 상승 종목 비율 | RRG 사분면"]
    for r in sec.itertuples():
        sec_lines.append(f"{r.sector} | {_f(r.r1, 1, True)}% | {_f(r.r5, 1, True)}% | {_f(r.r20, 1, True)}% | "
                         f"{(r.rise or 0) * 100:.0f}% | {r.q}")

    sflow = {}
    try:
        sflow = _stock_flows()
    except Exception as e:
        logger.warning(f"종목별 수급 로드 실패(무시): {e}")

    all_vals = [x.get("value") or 0 for s in sectors.values() for x in s.get("candidates", [])]
    v_med = float(np.median(all_vals)) if all_vals else 0
    cand_lines, codes = [], set()
    head = "종목(코드) | 1일 | 5일 | 20일 | 60일 | 거래대금(억) | 거래량 배율 | RRG"
    if sflow:
        head += " | 외국인+기관 5일/20일"
    for r in sec.head(N_SECTORS).itertuples():
        c = [x for x in sectors[r.sector].get("candidates", []) if (x.get("value") or 0) >= v_med]
        c = sorted(c, key=lambda x: -(x.get("value") or 0))[:PER_SECTOR]
        if not c:
            continue
        cand_lines.append(f"[{r.sector}]")
        for x in c:
            codes.add(x["code"])
            line = (f"  {x['name']}({x['code']}) | {_f(x.get('ret_1d'), 1, True)}% | {_f(x.get('ret_5d'), 1, True)}% | "
                    f"{_f(x.get('ret_20d'), 1, True)}% | {_f(x.get('ret_60d'), 1, True)}% | {x.get('value') or 0:,.0f} | "
                    f"{_f(x.get('vol_ratio'), 2)} | {x.get('quadrant', '')}")
            if sflow:
                a, b = sflow.get(x["code"], (0.0, 0.0))
                line += f" | {_won(a)} / {_won(b)}"
            cand_lines.append(line)
    return "\n".join(sec_lines), head + "\n" + "\n".join(cand_lines), codes, sig.get("updated_at", "")


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
        blocks.append(f"=== 섹터 현황 (signals.json {upd}, 5일 수익률·상승 비율 순) ===\n{sec_text}")
        blocks.append(f"=== 종목 후보 (상위 {N_SECTORS}개 섹터 × 거래대금 상위 {PER_SECTOR}종목, 거래대금 중앙값 이상) ===\n{cand_text}")
    except Exception as e:
        logger.warning(f"섹터·후보 생성 실패(생략): {e}")
    logger.info(f"국내 컨텍스트: 블록 {len(blocks)}개, 후보 {len(codes)}종목")
    return "\n\n".join(blocks), codes


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    text, codes = domestic_context()
    print(text)
    print(f"\n후보 {len(codes)}종목, {len(text)}자")
