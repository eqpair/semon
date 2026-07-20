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

# 섹터 설명·투자 포인트·대표주식 (S&P500 11개 섹터 표준 구성)
SECTOR_META = {
    "XLK": {
        "desc": "반도체, 소프트웨어, 클라우드, AI, 하드웨어 기업. 최근 10년 이상 미국 증시를 이끌어온 핵심 섹터.",
        "points": ["AI 성장 최대 수혜", "가장 높은 장기 수익률(연평균 ~20%)", "성장성 매우 높음"],
        "stocks": {"AAPL": "Apple", "MSFT": "Microsoft", "NVDA": "NVIDIA", "AVGO": "Broadcom",
                   "ORCL": "Oracle", "AMD": "AMD", "CSCO": "Cisco", "ADBE": "Adobe", "CRM": "Salesforce"},
    },
    "XLF": {
        "desc": "은행, 카드사, 보험사, 자산운용사. 금리가 오르면 은행 이익이 증가하는 경우가 많아 금리 변화에 민감.",
        "points": ["금리 영향 큼", "경기 회복기 강세", "배당 매력 높음"],
        "stocks": {"JPM": "JPMorgan", "BAC": "Bank of America", "GS": "Goldman Sachs", "MS": "Morgan Stanley",
                   "V": "Visa", "MA": "Mastercard", "BLK": "BlackRock", "AXP": "American Express"},
    },
    "XLE": {
        "desc": "석유·천연가스 탐사, 생산, 정제 기업. 유가 상승 시 가장 큰 수혜.",
        "points": ["유가 상승 시 강세", "높은 배당", "원자재 가격 영향 큼"],
        "stocks": {"XOM": "Exxon Mobil", "CVX": "Chevron", "COP": "ConocoPhillips", "EOG": "EOG Resources",
                   "SLB": "Schlumberger", "PSX": "Phillips 66", "MPC": "Marathon Petroleum", "VLO": "Valero"},
    },
    "XLV": {
        "desc": "제약, 바이오, 의료기기, 헬스케어 서비스. 경기와 무관한 의료 수요로 대표적 경기 방어 섹터.",
        "points": ["고령화 수혜", "안정적 실적", "장기 성장 기대"],
        "stocks": {"LLY": "Eli Lilly", "JNJ": "Johnson & Johnson", "UNH": "UnitedHealth", "MRK": "Merck",
                   "ABBV": "AbbVie", "ABT": "Abbott", "PFE": "Pfizer", "MDT": "Medtronic"},
    },
    "XLI": {
        "desc": "건설장비, 항공기, 철도, 물류, 산업기계. 경기 개선 시 설비투자 증가로 실적 개선.",
        "points": ["경기 회복기 수혜", "인프라 투자 확대 시 성장", "경기 민감 업종"],
        "stocks": {"CAT": "Caterpillar", "BA": "Boeing", "HON": "Honeywell", "GE": "GE Aerospace",
                   "UNP": "Union Pacific", "DE": "Deere", "RTX": "RTX", "UPS": "UPS"},
    },
    "XLY": {
        "desc": "자동차, 여행, 호텔, 명품, 의류, 온라인 쇼핑. 경기가 좋아질수록 소비가 늘어 가장 경기 민감한 섹터 중 하나.",
        "points": ["경기 회복기 강세", "금리 인하 시 수혜", "소비 심리 영향 최대"],
        "stocks": {"AMZN": "Amazon", "TSLA": "Tesla", "HD": "Home Depot", "MCD": "McDonald's",
                   "NKE": "Nike", "LOW": "Lowe's", "SBUX": "Starbucks", "BKNG": "Booking"},
    },
    "XLP": {
        "desc": "경기와 무관하게 꾸준히 소비되는 식품, 음료, 생활필수품 기업.",
        "points": ["경기 방어주", "꾸준한 배당", "낮은 변동성"],
        "stocks": {"PG": "P&G", "KO": "Coca-Cola", "PEP": "PepsiCo", "WMT": "Walmart",
                   "COST": "Costco", "CL": "Colgate", "MDLZ": "Mondelez", "KMB": "Kimberly-Clark"},
    },
    "XLU": {
        "desc": "전기, 가스, 수도 공공서비스. 대표적 경기 방어주로 배당 투자자 선호.",
        "points": ["안정적 배당", "경기 침체에도 실적 방어", "금리 변동에 민감"],
        "stocks": {"NEE": "NextEra", "DUK": "Duke Energy", "SO": "Southern", "D": "Dominion",
                   "AEP": "American Electric", "EXC": "Exelon", "XEL": "Xcel", "EIX": "Edison Intl"},
    },
    "XLB": {
        "desc": "화학, 철강, 광산, 시멘트 등 원재료 생산 기업. 건설·제조업 경기 영향 큼.",
        "points": ["원자재 가격 상승 수혜", "경기 회복기 강세", "경기순환 업종"],
        "stocks": {"LIN": "Linde", "SHW": "Sherwin-Williams", "FCX": "Freeport", "NEM": "Newmont",
                   "DOW": "Dow", "DD": "DuPont", "NUE": "Nucor", "VMC": "Vulcan"},
    },
    "XLRE": {
        "desc": "리츠(REITs) 중심. 물류센터, 데이터센터, 쇼핑몰 등을 운영하며 임대수익 창출.",
        "points": ["금리 영향 큼", "높은 배당", "안정적 장기 현금흐름"],
        "stocks": {"PLD": "Prologis", "AMT": "American Tower", "EQIX": "Equinix", "SPG": "Simon Property",
                   "PSA": "Public Storage", "WELL": "Welltower", "O": "Realty Income", "DLR": "Digital Realty"},
    },
    "XLC": {
        "desc": "인터넷 플랫폼, SNS, 광고, 통신, 스트리밍. 성장성과 경기민감성이 공존.",
        "points": ["광고시장 성장 수혜", "AI·디지털 광고 확대", "플랫폼 기업 중심"],
        "stocks": {"GOOGL": "Alphabet", "META": "Meta", "NFLX": "Netflix", "CMCSA": "Comcast",
                   "VZ": "Verizon", "T": "AT&T", "TMUS": "T-Mobile", "DIS": "Disney"},
    },
}
STOCK_TAIL_LEN = 10  # 대표주식 궤적 길이 (섹터보다 짧게 — 용량 관리)

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

    stock_tickers = [t for m in SECTOR_META.values() for t in m["stocks"]]
    tickers = list(UNIVERSE.keys()) + [BENCHMARK] + stock_tickers
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
    # ETF/벤치마크는 엄격(중단), 대표주식은 관대(제외 후 진행 — 상장폐지/티커변경 내성)
    core = list(UNIVERSE.keys()) + [BENCHMARK]
    missing = [t for t in core if t not in close.columns or close[t].dropna().empty]
    if missing:
        raise RuntimeError(f"가격 데이터 누락: {missing}")
    missing_stocks = [t for t in stock_tickers
                      if t not in close.columns or close[t].dropna().empty]
    if missing_stocks:
        print(f"[rrg_us] 대표주식 데이터 누락(제외 후 진행): {missing_stocks}", file=sys.stderr)

    tickers = [t for t in tickers if t not in missing_stocks]
    close = close[[t for t in close.columns if t in tickers]]

    last_date = close.index[-1].date()
    stale_days = (datetime.now(timezone.utc).date() - last_date).days
    if stale_days > 5:
        raise RuntimeError(f"frozen feed 의심: 마지막 봉 {last_date} ({stale_days}일 경과)")

    # 티커별 마지막 값이 직전 5봉과 완전히 동일하면 프리즈 의심 (core만)
    for t in core:
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


def _row_metrics(df: pd.DataFrame, px: pd.Series, tail_len: int) -> dict:
    """RRG df + 가격 시리즈 → 공통 지표 dict (섹터/종목 공용)"""
    last = df.iloc[-1]
    tail = df.iloc[-tail_len:]
    ret_1d = round(float(px.iloc[-1] / px.iloc[-2] - 1) * 100, 2) if len(px) >= 2 else None
    ret_5d = round(float(px.iloc[-1] / px.iloc[-6] - 1) * 100, 2) if len(px) >= 6 else None
    return {
        "ratio": round(float(last["ratio"]), 3),
        "momentum": round(float(last["momentum"]), 3),
        "quadrant": quadrant(last["ratio"], last["momentum"]),
        "ret_1d": ret_1d,
        "ret_5d": ret_5d,
        "tail": [
            [d.strftime("%Y-%m-%d"),
             round(float(r["ratio"]), 3),
             round(float(r["momentum"]), 3)]
            for d, r in tail.iterrows()
        ],
    }


def build_signals(rrg: dict, prices: pd.DataFrame,
                  stock_rrg: dict = None, stock_prices: pd.DataFrame = None) -> dict:
    now = datetime.now(KST)
    items = []
    for t, name in UNIVERSE.items():
        df = rrg[t]
        if df.empty:
            continue
        meta = SECTOR_META.get(t, {})
        item = {"ticker": t, "name": name}
        item.update(_row_metrics(df, prices[t].dropna(), TAIL_LEN))
        item["desc"] = meta.get("desc", "")
        item["points"] = meta.get("points", [])
        # 대표주식 RRG
        stocks = []
        for st, st_name in meta.get("stocks", {}).items():
            if not stock_rrg or st not in stock_rrg or stock_rrg[st].empty:
                continue
            srow = {"ticker": st, "name": st_name}
            srow.update(_row_metrics(stock_rrg[st], stock_prices[st].dropna(), STOCK_TAIL_LEN))
            stocks.append(srow)
        stocks.sort(key=lambda x: -x["ratio"])
        item["stocks"] = stocks
        items.append(item)
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
    stock_cols = [t for m in SECTOR_META.values() for t in m["stocks"] if t in close.columns]
    stock_prices = close[stock_cols]

    rrg = compute_rrg(prices, bench, RATIO_WINDOW, MOMENTUM_WINDOW)
    stock_rrg = compute_rrg(stock_prices, bench, RATIO_WINDOW, MOMENTUM_WINDOW)

    signals = build_signals(rrg, prices, stock_rrg, stock_prices)
    if len(signals["sectors"]) < len(UNIVERSE):
        got = {s["ticker"] for s in signals["sectors"]}
        raise RuntimeError(f"섹터 누락: {set(UNIVERSE) - got}")

    atomic_write(OUT_SIGNALS, signals)
    merge_history(rrg)
    push_signals()

    q = {s["ticker"]: s["quadrant"] for s in signals["sectors"]}
    n_stocks = sum(len(s.get("stocks", [])) for s in signals["sectors"])
    print(f"[rrg_us] OK as_of={signals['as_of']} stocks={n_stocks} {q}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[rrg_us] FAIL: {e}", file=sys.stderr)
        sys.exit(1)