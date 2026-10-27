#!/usr/bin/env python3
"""
stock_quality.py — 종목 '추세의 질' 점수 (검증 기반)

2011~2026 전 종목 검증 결과(거래대금 상위 600, 2주 리밸런싱, 상장폐지 포함):
  - 단기 모멘텀(20·60일 수익률, 50일선 이격)과 RRG RS-Ratio는 이후 수익률과 음의 상관
  - 60일 변동성이 낮을수록, 52주 고점에 가까울수록 이후 수익률이 높음 (두 기간 모두)
  - 52주 고점 근접은 Boom 국면에서 강하고 Bust 국면에선 효과 없음
  → 점수 = 백분위(낮은 변동성) × w + 백분위(52주 고점 근접) × (1 − w)
     w = 0.40 (Boom: Reflex 2~5단계), 0.65 (그 외)
  상위 30종목의 60일 후 초과수익(유니버스 평균 대비): 평균 +2.2%, 승률 63%
  (2011~18 +1.6%, 2019~26 +2.8%). 거래비용 미반영.

입력: data/reflex/krx/*.parquet (reflex_data.py), docs/data/reflex.json (국면)
사용:
    from stock_quality import quality_scores
    df = quality_scores()           # 전 종목 점수 (거래대금 상위 UNIVERSE_N 안에서 백분위)
    python stock_quality.py         # 상위 30종목 출력
"""
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent
KRX_DIR = BASE / "data" / "reflex" / "krx"
REFLEX_JSON = BASE / "docs" / "data" / "reflex.json"

UNIVERSE_N = 600          # 20일 평균 거래대금 상위 N (검증과 동일)
MIN_HISTORY = 260         # 최소 거래일 수
W_LOWVOL = {"Boom": 0.40, "Bust": 0.65, "Base": 0.65}


def current_phase():
    try:
        sid = json.loads(REFLEX_JSON.read_text(encoding="utf-8"))["stage"]["id"]
    except Exception as e:
        logger.warning(f"국면 로드 실패 — Base로 간주: {e}")
        return "Base", None
    return ("Boom" if sid in (2, 3, 4, 5) else "Bust" if sid in (6, 7, 8, 9) else "Base"), sid


def _load_recent(n_files=3):
    files = sorted(KRX_DIR.glob("*.parquet"))[-n_files:]
    df = pd.concat([pd.read_parquet(f, columns=["ticker", "date", "close", "chg", "volume", "value", "cap", "mkt"])
                    for f in files], ignore_index=True)
    return df[df["ticker"].astype(str).str[-1] == "0"]   # 보통주만


def quality_scores(phase=None):
    if phase is None:
        phase, _ = current_phase()
    df = _load_recent()
    piv = lambda c: df.pivot(index="date", columns="ticker", values=c).sort_index()
    chg, vol, val = piv("chg").astype("float64"), piv("volume"), piv("value").astype("float64")
    t = chg.index[-1]
    exists = chg.notna()
    px = (1 + (chg / 100).fillna(0)).cumprod().where(exists)

    hist = exists.sum()
    lr = np.log(px).diff()
    w60 = lr.iloc[-60:]
    vol60 = (w60.std() * np.sqrt(252) * 100).where(w60.count() >= 50)       # 연율 %
    w252 = px.iloc[-252:]
    hi = w252.max().where(w252.count() >= 200)
    hi52 = px.iloc[-1] / hi                                                  # 1 = 52주 고점
    avgval = val.iloc[-20:].mean() / 1e8                                     # 억원
    r20 = px.iloc[-1] / px.iloc[-21] - 1
    r60 = px.iloc[-1] / px.iloc[-61] - 1

    ok = (vol.iloc[-1] > 0) & px.iloc[-1].notna() & (hist >= MIN_HISTORY) & vol60.notna() & hi52.notna()
    uni = avgval.where(ok).dropna().nlargest(UNIVERSE_N).index

    out = pd.DataFrame({
        "vol60": vol60[uni], "hi52": hi52[uni], "avgval": avgval[uni],
        "r20": r20[uni] * 100, "r60": r60[uni] * 100,
        "close": piv("close").iloc[-1][uni], "cap": piv("cap").iloc[-1][uni] / 1e8,
        "mkt": piv("mkt").iloc[-1][uni],
    })
    w = W_LOWVOL.get(phase, 0.65)
    out["p_lowvol"] = 1 - out["vol60"].rank(pct=True)
    out["p_hi52"] = out["hi52"].rank(pct=True)
    out["score"] = (w * out["p_lowvol"] + (1 - w) * out["p_hi52"]) * 100
    out["rank"] = out["score"].rank(ascending=False, method="first").astype(int)
    out.index.name = "ticker"
    out.attrs.update(as_of=str(t.date()), phase=phase, w_lowvol=w, universe=len(uni))
    return out.sort_values("score", ascending=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    q = quality_scores()
    a = q.attrs
    print(f"기준일 {a['as_of']} | 국면 {a['phase']} (저변동 비중 {a['w_lowvol']:.2f}) | 유니버스 {a['universe']}종목")
    print(q.head(30)[["score", "vol60", "hi52", "r20", "r60", "avgval", "mkt"]].round(2).to_string())
