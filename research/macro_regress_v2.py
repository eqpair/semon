"""
macro_regress_v2.py — 날짜 정밀매칭 + 시차 반영 매크로 회귀

v1 문제 해결:
  - 정수인덱스 길이정렬 → 실제 거래일(YYYYMMDD) 날짜 매칭
  - 미국 매크로 시차 없음 → 전일(D-1) 미국장 → 당일(D) 한국장 lag 반영
  - 위험선호 오염 → 잔차수익률(시장 2팩터 제거) 우선

시차 규칙:
  미국계(WTI,SOX,S&P,나스닥,금,구리,알루미늄,VIX,러셀,미국10Y/2Y,달러) → D-1 (전일)
  아시아계(니케이,항셍,원달러)                                    → D0 (동시점, 한국장과 겹침)

산출:
  macro_beta_v2.json / macro_regress_v2_report.txt

실행:
  python3 macro_regress_v2.py
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
from fetch_stocks import load_market_caps

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MARKET_CAP_PATH = "/home/ubuntu/semon/data/market_cap.json"
MIN_DAYS = 200
T_THRESHOLD = 2.0
EXCLUDE_SECTOR = {"미분류"}

# 미국계 매크로 = 전일(D-1) → 당일 한국장에 반영
US_LAGGED = {"WTI유가", "필라델피아반도체", "S&P500", "나스닥", "금", "구리",
             "알루미늄", "VIX", "러셀2000", "미국10Y", "미국2Y", "달러인덱스"}
# 아시아계 = 동시점 (한국장과 시간대 겹침)
ASIA_SAME = {"니케이", "항셍", "원달러"}

CAUSAL_SIGN = {
    ("정유", "WTI유가"): +1, ("석유화학", "WTI유가"): +1, ("조선", "WTI유가"): +1,
    ("항공", "WTI유가"): -1, ("해운", "WTI유가"): +1,
    ("은행", "미국10Y"): +1, ("보험", "미국10Y"): +1,
    ("바이오_신약", "미국10Y"): -1, ("제약", "미국10Y"): -1,
    ("반도체_소자", "필라델피아반도체"): +1, ("반도체_장비", "필라델피아반도체"): +1,
    ("반도체_소재", "필라델피아반도체"): +1, ("반도체_패키징", "필라델피아반도체"): +1,
    ("반도체_팹리스", "필라델피아반도체"): +1, ("IT부품", "필라델피아반도체"): +1,
    ("비철금속", "구리"): +1, ("전력기기", "구리"): +1, ("비철금속", "금"): +1,
    ("화장품_브랜드", "항셍"): +1, ("화장품_ODM", "항셍"): +1,
    ("철강", "항셍"): +1, ("석유화학", "항셍"): +1,
    ("자동차_완성차", "원달러"): +1, ("반도체_소자", "원달러"): +1,
    ("음식료", "원달러"): +1, ("항공", "원달러"): -1,
    ("AI_SW", "나스닥"): +1, ("플랫폼", "나스닥"): +1, ("게임", "나스닥"): +1,
}

SISE = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=500&requestType=0"


async def _fetch_dated(session, code):
    """날짜 포함 종가 fetch → (code, {date: close})"""
    try:
        async with session.get(SISE.format(code=code), timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return code, None
            text = await r.text(encoding="euc-kr")
        soup = BeautifulSoup(text, "html.parser")
        out = {}
        for it in soup.find_all("item"):
            p = it.get("data", "").split("|")
            if len(p) >= 5:
                try:
                    out[pd.Timestamp(p[0])] = float(p[4])
                except (ValueError, TypeError):
                    continue
        return code, (out if out else None)
    except Exception:
        return code, None


async def fetch_all_dated(codes):
    results = {}
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(codes), 10):
            chunk = codes[i:i+10]
            for code, data in await asyncio.gather(*[_fetch_dated(session, c) for c in chunk]):
                results[code] = data
            if i + 10 < len(codes):
                await asyncio.sleep(1.0)
    ok = sum(1 for v in results.values() if v)
    logger.info(f"날짜포함 OHLCV: {ok}/{len(codes)}개")
    return results


def code_maps():
    name_of, sector_members = {}, defaultdict(list)
    for sec, lst in config.SECTORS.items():
        if sec in EXCLUDE_SECTOR:
            continue
        for code, name in lst:
            name_of[code] = name
            sector_members[sec].append(code)
    return name_of, sector_members


def build_price_df(dated):
    """{code:{date:close}} → 종가 DataFrame (날짜 인덱스)"""
    series = {c: pd.Series(d) for c, d in dated.items() if d and len(d) >= MIN_DAYS}
    df = pd.DataFrame(series).sort_index()
    return df


def main():
    name_of, sector_members = code_maps()
    codes = list(name_of.keys())
    logger.info(f"종목 {len(codes)}개 날짜포함 수집")

    dated = asyncio.run(fetch_all_dated(codes))
    px = build_price_df(dated)
    logger.info(f"종가 매트릭스: {px.shape[0]}일 × {px.shape[1]}종목")

    # KOSPI/KOSDAQ 날짜포함
    kospi_dated = asyncio.run(fetch_all_dated(["KOSPI"]))["KOSPI"]
    kosdaq_dated = asyncio.run(fetch_all_dated(["KOSDAQ"]))["KOSDAQ"]
    kospi = pd.Series(kospi_dated).sort_index()
    kosdaq = pd.Series(kosdaq_dated).sort_index()

    # 종목 로그수익률 (날짜 인덱스)
    R = np.log(px / px.shift(1)).iloc[1:]

    # 시장 2팩터 잔차
    mkt = pd.DataFrame({
        "KOSPI":  np.log(kospi / kospi.shift(1)),
        "KOSDAQ": np.log(kosdaq / kosdaq.shift(1)),
    }).reindex(R.index).dropna()
    Rm = R.reindex(mkt.index)
    X = np.column_stack([np.ones(len(mkt)), mkt["KOSPI"], mkt["KOSDAQ"]])
    H = X @ np.linalg.pinv(X.T @ X) @ X.T
    resid = pd.DataFrame(Rm.values - H @ Rm.values, index=mkt.index, columns=Rm.columns)

    caps = load_market_caps(MARKET_CAP_PATH)
    cap_of = {c: (caps.get(c, {}).get("cap", 0) if isinstance(caps.get(c), dict)
                  else caps.get(c, 0)) or 0 for c in px.columns}

    # 매크로 로그수익률 (날짜 인덱스) + 시차
    macro_px = pd.read_parquet("macro_history.parquet")
    macro_px.index = pd.to_datetime(macro_px.index).tz_localize(None).normalize()
    macro_ret = np.log(macro_px / macro_px.shift(1)).iloc[1:]
    # 미국계는 shift(1) 추가 (전일값을 당일에)
    macro_lagged = macro_ret.copy()
    for col in macro_ret.columns:
        if col in US_LAGGED:
            macro_lagged[col] = macro_ret[col].shift(1)

    def agg(returns_df, weighted):
        out = {}
        for sec, cds in sector_members.items():
            cols = [c for c in cds if c in returns_df.columns]
            if len(cols) < 2:
                continue
            sub = returns_df[cols]
            if weighted:
                w = np.array([cap_of.get(c, 0) for c in cols], float)
                w = w / w.sum() if w.sum() > 0 else np.ones(len(cols)) / len(cols)
                out[sec] = (sub * w).sum(axis=1)
            else:
                out[sec] = sub.mean(axis=1)
        return pd.DataFrame(out)

    def multi_reg(y, Xdf):
        d = pd.concat([y.rename("_y"), Xdf], axis=1).dropna()
        if len(d) < 40:
            return {}
        yv = d["_y"].values
        Xv = d.drop(columns="_y")
        Xmat = np.column_stack([np.ones(len(Xv)), Xv.values])
        b = np.linalg.lstsq(Xmat, yv, rcond=None)[0]
        r = yv - Xmat @ b
        dof = len(yv) - Xmat.shape[1]
        if dof <= 0:
            return {}
        cov = (r @ r) / dof * np.linalg.pinv(Xmat.T @ Xmat)
        out = {}
        for i, col in enumerate(Xv.columns):
            beta, var = b[i+1], cov[i+1, i+1]
            se = np.sqrt(var) if var > 0 else 0
            out[col] = (beta, beta/se if se > 0 else 0)
        return out

    report = []
    beta_result = {}
    # 잔차수익률 우선(핵심), 원수익률 대조
    for ret_kind, ret_df in [("잔차수익률", resid), ("원수익률", R)]:
        sec_ret = agg(ret_df, weighted=True)  # 시총가중 고정 (v1에서 큰 차이 없었음)
        # 날짜 정렬: 섹터 ∩ 매크로(시차반영)
        common = sec_ret.index.intersection(macro_lagged.index)
        sret = sec_ret.reindex(common)
        mret = macro_lagged.reindex(common)
        report.append("\n" + "=" * 70)
        report.append(f"[{ret_kind} / 시총가중 / 시차반영 / 날짜매칭]  n={len(common)}일")
        report.append("=" * 70)

        for sec in sret.columns:
            res = multi_reg(sret[sec], mret)
            sig = {k: v for k, v in res.items() if abs(v[1]) >= T_THRESHOLD}
            if not sig:
                continue
            parts = []
            for mk, (beta, t) in sorted(sig.items(), key=lambda x: -abs(x[1][1])):
                exp = CAUSAL_SIGN.get((sec, mk))
                mark = ""
                if exp is not None:
                    mark = " ✓" if np.sign(beta) == exp else " ✗"
                parts.append(f"{mk}(β={beta:+.2f},t={t:+.1f}){mark}")
            report.append(f"  [{sec}] " + " | ".join(parts))

            if ret_kind == "잔차수익률":
                beta_result[sec] = {
                    mk: {"beta": round(float(beta), 3), "t": round(float(t), 2),
                         "causal_ok": bool(CAUSAL_SIGN.get((sec, mk)) is None
                                           or np.sign(beta) == CAUSAL_SIGN.get((sec, mk)))}
                    for mk, (beta, t) in sig.items()
                }

    txt = "\n".join(report)
    open("macro_regress_v2_report.txt", "w", encoding="utf-8").write(txt)
    json.dump(beta_result, open("macro_beta_v2.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)
    print(txt)
    print(f"\n잔차수익률 유의섹터: {len(beta_result)}개 → macro_beta_v2.json")


if __name__ == "__main__":
    main()