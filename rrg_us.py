#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
rrg_us.py — US Sector RRG (Phase 1)
====================================
유니버스 : SPDR 섹터 ETF 11개
벤치마크 : SPY
데이터   : yfinance 일봉 (auto_adjust=True, 수정주가)
계산     : Bloomberg ROC 방식 (semon KR 엔진 v6와 동일 수식)
             RS        = 100 * (price / benchmark), 기간 시작점 100 리베이스
             RS_Ratio  = 100 * RS / SMA(RS, RATIO_WINDOW)
             RS_Mom    = 100 * RS_Ratio / SMA(RS_Ratio, MOMENTUM_WINDOW)
출력     : docs/signals_us.json      (최신 스냅샷 + tail)
           data/rrg_history_us.json  (일별 누적, merge 방식)

운영     : cron 화~토 06:30 KST (미 정규장 마감 + 여유)
           30 6 * * 2-6  cd /home/ubuntu/semon && /usr/bin/python3 rrg_us.py >> logs/rrg_us.log 2>&1

주의     : RATIO_WINDOW / MOMENTUM_WINDOW 는 반드시 semon KR 엔진의
           MA_LONG 값과 동일하게 맞출 것. (아래 CONFIG 참고)
"""

import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone, timedelta

import pandas as pd

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
VERSION = "us-v1 (engine v6 ROC)"

# ★★★ semon KR 엔진(signals.json 생성부)의 윈도우 값과 반드시 동일하게 설정 ★★★
RATIO_WINDOW = 50      # RS_Ratio  = 100 * RS / SMA(RS, RATIO_WINDOW)
MOMENTUM_WINDOW = 20   # RS_Mom    = 100 * RS_Ratio / SMA(RS_Ratio, MOMENTUM_WINDOW)

LOOKBACK_DAYS = 400    # yfinance 요청 기간 (윈도우 + tail 여유분)
TAIL_LEN = 15          # 프론트 궤적 길이 (거래일 기준)

BENCHMARK = "SPY"

UNIVERSE = {
    "XLK":  "Technology",
    "XLF":  "Financials",
    "XLE":  "Energy",
    "XLV":  "Health Care",
    "XLI":  "Industrials",
    "XLY":  "Cons. Discretionary",
    "XLP":  "Cons. Staples",
    "XLU":  "Utilities",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLC":  "Communication",
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUT_SIGNALS = os.path.join(BASE_DIR, "docs", "data", "signals_us.json")
OUT_HISTORY = os.path.join(BASE_DIR, "data", "rrg_history_us.json")
HISTORY_KEEP_DAYS = 250   # history 보존 기간 (거래일 기준 약 1년)

KST = timezone(timedelta(hours=9))


# ──────────────────────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────────────────────
def fetch_prices() -> pd.DataFrame:
    """유니버스 + 벤치마크 수정 종가 DataFrame (index=date, columns=ticker)"""
    import yfinance as yf

    tickers = list(UNIVERSE.keys()) + [BENCHMARK]
    df = yf.download(
        tickers,
        period=f"{LOOKBACK_DAYS}d",
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="column",
    )
    if df is None or df.empty:
        raise RuntimeError("yfinance 응답이 비어 있음")

    close = df["Close"] if isinstance(df.columns, pd.MultiIndex) else df[["Close"]]
    close = close.dropna(how="all")

    # ── frozen-feed / 결측 가드 (macro 파이프라인 패턴 재사용) ──
    missing = [t for t in tickers if t not in close.columns or close[t].dropna().empty]
    if missing:
        raise RuntimeError(f"가격 데이터 누락: {missing}")

    last_date = close.index[-1].date()
    stale_days = (datetime.now(timezone.utc).date() - last_date).days
    if stale_days > 5:
        raise RuntimeError(f"frozen feed 의심: 마지막 봉 {last_date} ({stale_days}일 경과)")

    # 티커별 마지막 값이 직전 5봉과 완전히 동일하면 프리즈 의심
    for t in tickers:
        s = close[t].dropna()
        if len(s) >= 6 and s.iloc[-6:].nunique() == 1:
            raise RuntimeError(f"frozen feed 의심: {t} 최근 6봉 동일값 {s.iloc[-1]}")

    return close.ffill()


# ──────────────────────────────────────────────────────────────
# RRG 계산 (semon KR v6와 동일한 ROC 방식 — 순수 함수)
# ──────────────────────────────────────────────────────────────
def compute_rrg(prices: pd.DataFrame, benchmark: pd.Series,
                ratio_window: int, momentum_window: int) -> dict:
    """
    returns {ticker: DataFrame[ratio, momentum]} (index=date)
    """
    out = {}
    for t in prices.columns:
        rs = 100.0 * prices[t] / benchmark
        rs = 100.0 * rs / rs.iloc[0]  # 리베이스 (v6 패턴)

        rs_ma = rs.rolling(ratio_window).mean()
        ratio = 100.0 * rs / rs_ma

        ratio_ma = ratio.rolling(momentum_window).mean()
        momentum = 100.0 * ratio / ratio_ma

        df = pd.DataFrame({"ratio": ratio, "momentum": momentum}).dropna()
        out[t] = df
    return out


def quadrant(ratio: float, momentum: float) -> str:
    if ratio >= 100 and momentum >= 100:
        return "Leading"
    if ratio >= 100:
        return "Weakening"
    if momentum >= 100:
        return "Improving"
    return "Lagging"


# ──────────────────────────────────────────────────────────────
# 출력
# ──────────────────────────────────────────────────────────────
def atomic_write(path: str, obj: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        ts = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
        shutil.copy2(path, f"{path}.bak_{ts}")
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, path)


def build_signals(rrg: dict) -> dict:
    now = datetime.now(KST)
    items = []
    for t, name in UNIVERSE.items():
        df = rrg[t]
        if df.empty:
            continue
        last = df.iloc[-1]
        tail = df.iloc[-TAIL_LEN:]
        items.append({
            "ticker": t,
            "name": name,
            "ratio": round(float(last["ratio"]), 3),
            "momentum": round(float(last["momentum"]), 3),
            "quadrant": quadrant(last["ratio"], last["momentum"]),
            "tail": [
                [d.strftime("%Y-%m-%d"),
                 round(float(r["ratio"]), 3),
                 round(float(r["momentum"]), 3)]
                for d, r in tail.iterrows()
            ],
        })
    return {
        "version": VERSION,
        "benchmark": BENCHMARK,
        "ratio_window": RATIO_WINDOW,
        "momentum_window": MOMENTUM_WINDOW,
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S KST"),
        "as_of": items[0]["tail"][-1][0] if items else None,
        "sectors": items,
    }


def merge_history(rrg: dict):
    """일별 스냅샷을 history에 merge (earnings calendar 누적 패턴)"""
    hist = {}
    if os.path.exists(OUT_HISTORY):
        try:
            with open(OUT_HISTORY, encoding="utf-8") as f:
                hist = json.load(f)
        except Exception:
            hist = {}

    days = hist.setdefault("days", {})
    for t in UNIVERSE:
        df = rrg[t]
        for d, row in df.iterrows():
            key = d.strftime("%Y-%m-%d")
            days.setdefault(key, {})[t] = [
                round(float(row["ratio"]), 3),
                round(float(row["momentum"]), 3),
            ]

    # 보존 기간 초과분 정리
    keys = sorted(days.keys())
    for k in keys[:-HISTORY_KEEP_DAYS]:
        del days[k]

    hist["version"] = VERSION
    hist["benchmark"] = BENCHMARK
    atomic_write(OUT_HISTORY, hist)


def push_signals():
    """signals_us.json 커밋/push (update_earnings.sh 패턴, 실패해도 계산 결과는 보존)"""
    import subprocess
    def run(*cmd):
        return subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)
    try:
        run("git", "add", "docs/data/signals_us.json")
        r = run("git", "commit", "-m", "Update US RRG signals")
        if "nothing to commit" in r.stdout + r.stderr:
            print("[rrg_us] no signal change, skip push")
            return
        run("git", "pull", "--rebase")
        r = run("git", "push")
        if r.returncode != 0:
            raise RuntimeError(r.stderr.strip()[-300:])
        print("[rrg_us] git push 완료")
    except Exception as e:
        print(f"[rrg_us] git push 실패 (signals는 저장됨): {e}", file=sys.stderr)


# ──────────────────────────────────────────────────────────────
def main():
    close = fetch_prices()
    bench = close[BENCHMARK]
    prices = close[list(UNIVERSE.keys())]

    rrg = compute_rrg(prices, bench, RATIO_WINDOW, MOMENTUM_WINDOW)

    signals = build_signals(rrg)
    if len(signals["sectors"]) < len(UNIVERSE):
        got = {s["ticker"] for s in signals["sectors"]}
        raise RuntimeError(f"섹터 누락: {set(UNIVERSE) - got}")

    atomic_write(OUT_SIGNALS, signals)
    merge_history(rrg)
    push_signals()

    q = {s["ticker"]: s["quadrant"] for s in signals["sectors"]}
    print(f"[rrg_us] OK as_of={signals['as_of']} {q}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[rrg_us] FAIL: {e}", file=sys.stderr)
        sys.exit(1)