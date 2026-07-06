"""
macro_regress.py — 섹터 × 매크로 민감도 회귀 (4조합 전부 비교)

조합:
  집계:   시총가중 / 동등가중
  수익률: 원수익률 / 잔차수익률(시장제거)
  회귀:   단순(변수별 따로) / 다중(전변수 동시)
  기간:   일별(단기) / 5일(중기)

전제:
  - config.py (섹터 분류)
  - macro_history.parquet (fetch_macro_history.py 산출, 15변수 1년치)
  - ohlcv는 crawler로 재수집 (섹터 수익률 집계용 — 시총 필요)

산출:
  macro_beta.json          — 섹터별 유의 매크로 민감도 (인과 필터 적용)
  macro_regress_report.txt — 4조합 비교 상세

실행:
  python3 macro_regress.py
"""
import asyncio
import json
import logging
import numpy as np
import pandas as pd
from collections import defaultdict

import config
from crawler import fetch_all_ohlcv, fetch_kospi_ohlcv, fetch_kosdaq_ohlcv
from fetch_stocks import load_market_caps

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MARKET_CAP_PATH = "/home/ubuntu/semon/data/market_cap.json"
MIN_DAYS = 200
T_THRESHOLD = 2.0      # |t| >= 2 (약 95% 유의)
EXCLUDE_SECTOR = {"미분류"}

# ── 인과경로 부호 사전 (양수 기대 / 음수 기대) ──
# 회귀 계수 부호가 이와 맞아야 '인과 일치'로 채택
CAUSAL_SIGN = {
    # (섹터, 매크로): 기대부호  — 명시 안 된 건 부호 무관(참고만)
    ("정유", "WTI유가"): +1, ("석유화학", "WTI유가"): +1,
    ("조선", "WTI유가"): +1, ("항공", "WTI유가"): -1, ("해운", "WTI유가"): +1,
    ("은행", "미국10Y"): +1, ("보험", "미국10Y"): +1,
    ("바이오_신약", "미국10Y"): -1, ("제약", "미국10Y"): -1,
    ("반도체_소자", "필라델피아반도체"): +1, ("반도체_장비", "필라델피아반도체"): +1,
    ("반도체_소재", "필라델피아반도체"): +1, ("반도체_패키징", "필라델피아반도체"): +1,
    ("반도체_팹리스", "필라델피아반도체"): +1, ("IT부품", "필라델피아반도체"): +1,
    ("비철금속", "구리"): +1, ("전력기기", "구리"): +1,
    ("비철금속", "금"): +1,
    ("화장품_브랜드", "항셍"): +1, ("화장품_ODM", "항셍"): +1,
    ("철강", "항셍"): +1, ("석유화학", "항셍"): +1,
    ("자동차_완성차", "원달러"): +1, ("반도체_소자", "원달러"): +1,
    ("음식료", "원달러"): +1, ("항공", "원달러"): -1,
    ("AI_SW", "나스닥"): +1, ("플랫폼", "나스닥"): +1, ("게임", "나스닥"): +1,
}


def code_maps():
    name_of, sector_members = {}, defaultdict(list)
    for sec, lst in config.SECTORS.items():
        if sec in EXCLUDE_SECTOR:
            continue
        for code, name in lst:
            name_of[code] = name
            sector_members[sec].append(code)
    return name_of, sector_members


async def get_stock_returns():
    """종목별 일별 로그수익률 + 시총. crawler 재사용."""
    name_of, sector_members = code_maps()
    codes = list(name_of.keys())
    logger.info(f"종목 {len(codes)}개 OHLCV 수집")
    raw = await fetch_all_ohlcv(codes)
    kospi = await fetch_kospi_ohlcv()
    kosdaq = await fetch_kosdaq_ohlcv()

    series = {}
    for code, d in raw.items():
        if d and len(d["closes"]) >= MIN_DAYS:
            series[code] = np.asarray(d["closes"], dtype=float)
    L = min([len(s) for s in series.values()] + [len(kospi), len(kosdaq)])
    L = min(L, 260)

    px = pd.DataFrame({c: s[-L:] for c, s in series.items()})
    R = np.log(px / px.shift(1)).iloc[1:]   # 원 로그수익률

    # 시장 2팩터 잔차
    mkt = pd.DataFrame({
        "KOSPI":  np.log(np.array(kospi[-L:])[1:] / np.array(kospi[-L:])[:-1]),
        "KOSDAQ": np.log(np.array(kosdaq[-L:])[1:] / np.array(kosdaq[-L:])[:-1]),
    }, index=R.index)
    X = np.column_stack([np.ones(len(mkt)), mkt["KOSPI"], mkt["KOSDAQ"]])
    H = X @ np.linalg.pinv(X.T @ X) @ X.T
    resid = pd.DataFrame(R.values - H @ R.values, index=R.index, columns=R.columns)

    caps = load_market_caps(MARKET_CAP_PATH)
    cap_of = {}
    for c in series:
        v = caps.get(c, 0)
        cap_of[c] = (v.get("cap", 0) if isinstance(v, dict) else v) or 0
    return R, resid, sector_members, cap_of


def agg_sector(returns_df, sector_members, cap_of, weighted):
    """섹터 수익률 시계열 = 구성종목 가중평균"""
    out = {}
    for sec, codes in sector_members.items():
        cols = [c for c in codes if c in returns_df.columns]
        if len(cols) < 2:
            continue
        sub = returns_df[cols]
        if weighted:
            w = np.array([cap_of.get(c, 0) for c in cols], dtype=float)
            if w.sum() <= 0:
                w = np.ones(len(cols))
            w = w / w.sum()
            out[sec] = (sub * w).sum(axis=1)
        else:
            out[sec] = sub.mean(axis=1)
    return pd.DataFrame(out)


def macro_returns(horizon):
    m = pd.read_parquet("macro_history.parquet")
    m.index = pd.to_datetime(m.index).tz_localize(None).normalize()
    if horizon == 1:
        return np.log(m / m.shift(1)).iloc[1:]
    else:
        return np.log(m / m.shift(horizon)).iloc[horizon:]


def align(sec_ret, mac_ret):
    """섹터·매크로는 인덱스가 정수(거래일)라 날짜 매칭 불가 →
    길이 기준 최근 구간 정렬 (둘 다 최신이 끝). 근사적이지만 실용."""
    n = min(len(sec_ret), len(mac_ret))
    s = sec_ret.iloc[-n:].reset_index(drop=True)
    m = mac_ret.iloc[-n:].reset_index(drop=True)
    return s, m


def simple_reg(y, x):
    """단순회귀 y~x, 반환 (beta, t)"""
    mask = ~(np.isnan(y) | np.isnan(x))
    y, x = y[mask], x[mask]
    if len(y) < 30 or x.std() == 0:
        return None, None
    X = np.column_stack([np.ones(len(x)), x])
    b = np.linalg.lstsq(X, y, rcond=None)[0]
    resid = y - X @ b
    s2 = (resid @ resid) / (len(y) - 2)
    se = np.sqrt(s2 * np.linalg.pinv(X.T @ X)[1, 1])
    t = b[1] / se if se > 0 else 0
    return b[1], t


def multi_reg(y, Xdf):
    """다중회귀 y~all macros, 반환 {macro: (beta,t)}"""
    mask = ~y.isna() & ~Xdf.isna().any(axis=1)
    y2, X2 = y[mask], Xdf[mask]
    if len(y2) < 40:
        return {}
    Xmat = np.column_stack([np.ones(len(X2)), X2.values])
    b = np.linalg.lstsq(Xmat, y2.values, rcond=None)[0]
    resid = y2.values - Xmat @ b
    dof = len(y2) - Xmat.shape[1]
    if dof <= 0:
        return {}
    s2 = (resid @ resid) / dof
    cov = s2 * np.linalg.pinv(Xmat.T @ Xmat)
    out = {}
    for i, col in enumerate(X2.columns):
        beta = b[i + 1]
        se = np.sqrt(cov[i + 1, i + 1]) if cov[i + 1, i + 1] > 0 else 0
        t = beta / se if se > 0 else 0
        out[col] = (beta, t)
    return out


def run():
    R, resid, sector_members, cap_of = asyncio.run(get_stock_returns())

    report = []
    beta_result = {}

    for weighted in [True, False]:
        for ret_kind, ret_df in [("원수익률", R), ("잔차수익률", resid)]:
            for horizon in [1, 5]:
                tag = f"{'시총' if weighted else '동등'}가중 / {ret_kind} / {horizon}일"
                sec_ret_full = agg_sector(ret_df, sector_members, cap_of, weighted)
                mac_ret = macro_returns(horizon)
                sec_ret, mac = align(sec_ret_full, mac_ret)

                report.append("\n" + "=" * 70)
                report.append(f"[{tag}]  n={len(sec_ret)}일, 섹터 {sec_ret.shape[1]}개")
                report.append("=" * 70)

                # 다중회귀 (해석 핵심)
                for sec in sec_ret.columns:
                    res = multi_reg(sec_ret[sec], mac)
                    sig = {k: v for k, v in res.items() if abs(v[1]) >= T_THRESHOLD}
                    if not sig:
                        continue
                    # 인과 부호 체크
                    parts = []
                    for mk, (beta, t) in sorted(sig.items(), key=lambda x: -abs(x[1][1])):
                        exp = CAUSAL_SIGN.get((sec, mk))
                        mark = ""
                        if exp is not None:
                            mark = " ✓인과일치" if np.sign(beta) == exp else " ✗부호반대"
                        parts.append(f"{mk}(β={beta:+.2f},t={t:+.1f}){mark}")
                    report.append(f"  [{sec}] " + " | ".join(parts))

                    # 대표 조합(시총·원수익률·일별)만 beta_result 저장
                    if weighted and ret_kind == "원수익률" and horizon == 1:
                        beta_result[sec] = {
                            mk: {"beta": round(beta, 3), "t": round(t, 2),
                                 "causal_ok": (CAUSAL_SIGN.get((sec, mk)) is None
                                               or np.sign(beta) == CAUSAL_SIGN.get((sec, mk)))}
                            for mk, (beta, t) in sig.items()
                        }

    txt = "\n".join(report)
    open("macro_regress_report.txt", "w", encoding="utf-8").write(txt)
    json.dump(beta_result, open("macro_beta.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print(txt[:4000])
    print("\n... (전체 macro_regress_report.txt)")
    print(f"\n대표조합(시총·원수익률·일별) 유의섹터: {len(beta_result)}개 → macro_beta.json")


if __name__ == "__main__":
    run()