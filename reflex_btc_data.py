#!/usr/bin/env python3
"""
reflex_btc_data.py — 비트코인 형세·기세 페이지용 데이터 수집 (매번 전체 갱신, 1~2분)

저장 위치: data/reflex_btc/
  macro.parquet     BTC 가격·거래량, 나스닥, VIX, 달러인덱스, 금 (yfinance)
                    + 실질금리, 연준 자산, TGA, 역레포, 미국10Y (FRED)
  onchain.parquet   MVRV, 해시레이트, 활성 주소, 시가총액 (CoinMetrics 무료 API)
  stable.parquet    달러 스테이블코인 총 유통량 (DefiLlama)
  funding.parquet   BTCUSDT 무기한 펀딩비 일합계 (Binance, 2019-09~)
  alts.parquet      주요 알트코인 일봉 종가 (Binance, 2017~)

사용법:
  python reflex_btc_data.py            # 전체
  python reflex_btc_data.py macro onchain   # 일부만
"""
import io
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
OUT = BASE / "data" / "reflex_btc"
H = {"User-Agent": "Mozilla/5.0 (reflex_btc)"}
START = "2013-01-01"

YF = {"BTC": "BTC-USD", "나스닥": "^IXIC", "VIX": "^VIX", "달러인덱스": "DX-Y.NYB", "금": "GC=F"}
FRED = {"실질금리10Y": "DFII10", "연준자산": "WALCL", "TGA": "WTREGEN", "역레포": "RRPONTSYD", "미국10Y": "DGS10"}
CM_METRICS = ["CapMVRVCur", "HashRate", "AdrActCnt", "CapMrktCurUSD"]
ALTS = ["ETH", "BNB", "XRP", "ADA", "SOL", "DOGE", "TRX", "LINK", "DOT", "LTC",
        "BCH", "AVAX", "XLM", "ATOM", "ETC", "UNI", "FIL", "NEAR", "HBAR", "AAVE"]


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


def get(url, params=None, timeout=60, tries=3, headers=None):
    for i in range(tries):
        try:
            r = requests.get(url, params=params, headers=H if headers is None else headers, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            if i == tries - 1:
                raise
            log(f"  재시도 {i + 1}: {url[:60]} — {e!r}"[:160])
            time.sleep(5 * (i + 1))


def save(df, name):
    p = OUT / f"{name}.parquet"
    tmp = p.with_suffix(".parquet.tmp")
    df.to_parquet(tmp)
    os.replace(tmp, p)
    first = df.index.min().date() if len(df) else None
    last = df.index.max().date() if len(df) else None
    log(f"{name}: 저장 {df.shape} {first}→{last}")


def daily_index(idx):
    return pd.DatetimeIndex(pd.to_datetime(idx, utc=True)).tz_convert(None).normalize()


# ── 수집 ─────────────────────────────────────────────────────────────
def fetch_macro():
    import yfinance as yf
    others = {k: v for k, v in YF.items() if k != "BTC"}
    raw = yf.download(list(others.values()), start=START, progress=False, auto_adjust=True, threads=True)
    df = raw["Close"].rename(columns={v: k for k, v in others.items()})
    # BTC는 단독으로 받음 (여러 종목 동시 다운로드 시 최근 행이 빠지는 경우가 있음)
    b = yf.download("BTC-USD", start=START, progress=False, auto_adjust=True)
    bc, bv = b["Close"], b["Volume"]
    bc = bc.iloc[:, 0] if isinstance(bc, pd.DataFrame) else bc
    bv = bv.iloc[:, 0] if isinstance(bv, pd.DataFrame) else bv
    for frame in (df,):
        if getattr(frame.index, "tz", None) is not None:
            frame.index = frame.index.tz_localize(None)
    if getattr(bc.index, "tz", None) is not None:
        bc.index = bc.index.tz_localize(None)
        bv.index = bv.index.tz_localize(None)
    df = df.join(pd.DataFrame({"BTC": bc, "BTC거래량": bv}), how="outer")
    mp = OUT / "macro.parquet"
    try:
        prev = pd.read_parquet(mp) if mp.exists() else None
    except Exception:
        prev = None
    for name, sid in FRED.items():
        try:
            s = pd.read_csv(io.StringIO(
                get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}", timeout=30, tries=2, headers={}).text),
                index_col=0, parse_dates=True, na_values=".")
            df = df.join(s.iloc[:, 0].astype(float).rename(name), how="outer")
        except Exception as e:
            log(f"macro: FRED {name} 실패 — {e!r}"[:200])
            if prev is not None and name in prev.columns:
                df = df.join(prev[name], how="outer")
                log(f"macro: FRED {name} 이전 값 유지 (마지막 {prev[name].last_valid_index().date()})")
    df = df.sort_index()
    df = df[df.index >= START].dropna(how="all")
    save(df, "macro")


def fetch_onchain():
    url = "https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
    params = {"assets": "btc", "metrics": ",".join(CM_METRICS), "frequency": "1d",
              "start_time": START, "page_size": 10000, "paging_from": "start"}
    rows = []
    while url:
        j = get(url, params).json()
        rows += j.get("data", [])
        url, params = j.get("next_page_url"), None
        time.sleep(0.5)
    df = pd.DataFrame(rows)
    df.index = daily_index(df.pop("time"))
    df = df.drop(columns=["asset"], errors="ignore").apply(pd.to_numeric, errors="coerce")
    save(df.sort_index(), "onchain")


def fetch_stable():
    j = get("https://stablecoins.llama.fi/stablecoincharts/all").json()
    rec = {pd.Timestamp(int(x["date"]), unit="s").normalize():
           float((x.get("totalCirculatingUSD") or {}).get("peggedUSD") or 0) for x in j}
    s = pd.Series(rec, name="stable_usd").sort_index()
    save(s[s > 0].to_frame(), "stable")


def fetch_funding():
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    start = int(pd.Timestamp("2019-09-01").timestamp() * 1000)
    rows = []
    while True:
        j = get(url, {"symbol": "BTCUSDT", "limit": 1000, "startTime": start}).json()
        if not j:
            break
        rows += j
        start = j[-1]["fundingTime"] + 1
        if len(j) < 1000:
            break
        time.sleep(0.3)
    df = pd.DataFrame(rows)
    df["t"] = pd.to_datetime(df["fundingTime"], unit="ms").dt.normalize()
    df["r"] = df["fundingRate"].astype(float)
    d = df.groupby("t")["r"].agg(["sum", "count"]).rename(columns={"sum": "funding_day", "count": "n"})
    save(d, "funding")


def fetch_alts():
    url = "https://api.binance.com/api/v3/klines"
    closes = {}
    for sym in ["BTC"] + ALTS:
        start, rows = int(pd.Timestamp("2017-07-01").timestamp() * 1000), []
        try:
            while True:
                j = get(url, {"symbol": f"{sym}USDT", "interval": "1d", "limit": 1000, "startTime": start}).json()
                if not j:
                    break
                rows += j
                start = j[-1][0] + 86_400_000
                if len(j) < 1000:
                    break
                time.sleep(0.2)
        except Exception as e:
            log(f"alts: {sym} 실패 — {e!r}"[:160])
            continue
        if rows:
            s = pd.Series({pd.Timestamp(r[0], unit="ms"): float(r[4]) for r in rows})
            closes[sym] = s
    df = pd.DataFrame(closes).sort_index()
    # 진행 중인 오늘(UTC) 봉은 제외
    today = pd.Timestamp.utcnow().tz_localize(None).normalize()
    df = df[df.index < today]
    save(df, "alts")


STEPS = {"macro": fetch_macro, "onchain": fetch_onchain, "stable": fetch_stable,
         "funding": fetch_funding, "alts": fetch_alts}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    want = [a for a in sys.argv[1:] if a in STEPS] or list(STEPS)
    failed = []
    for name in want:
        try:
            STEPS[name]()
        except Exception as e:
            failed.append(name)
            log(f"{name}: 실패 — {e!r}"[:300])
    log("완료" + (f" (실패: {failed})" if failed else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
