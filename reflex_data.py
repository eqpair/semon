#!/usr/bin/env python3
"""
reflex_data.py — 형세·기세(재귀성) 페이지용 히스토리 저장소

저장 위치: data/reflex/
  macro.parquet      형세용 매크로 (yfinance + FRED 금리), 2010~
  krx/YYYY.parquet   KRX 전 종목 일별 스냅샷 (KOSPI+KOSDAQ, long format)
  flows.parquet      시장별 투자자 순매수 (기관/기타법인/개인/외국인)
  breadth.parquet    기세용 시장 폭 일별 집계 (ALL/KOSPI/KOSDAQ)

사용법:
  python reflex_data.py macro   [--start 2010-01-01]
  python reflex_data.py krx     [--start YYYY-MM-DD] [--end YYYY-MM-DD]   # 이어받기 지원
  python reflex_data.py flows   [--start YYYY-MM-DD] [--end YYYY-MM-DD]
  python reflex_data.py breadth                                          # krx → 집계
  python reflex_data.py daily                                            # 일일 갱신 (cron용)

KRX 로그인: ~/.krx_env (KRX_ID=..., KRX_PW=...) 를 스스로 읽음.
"""
import os
import sys
import json
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
OUT = BASE / "data" / "reflex"
KRX_DIR = OUT / "krx"
SKIP_PATH = OUT / "krx_skip.json"
START_DEFAULT = "2010-01-01"

YF_TICKERS = {
    "코스피": "^KS11",
    "코스닥": "^KQ11",
    "S&P500": "^GSPC",
    "나스닥": "^IXIC",
    "러셀2000": "^RUT",
    "VIX": "^VIX",
    "필라델피아반도체": "^SOX",
    "달러인덱스": "DX-Y.NYB",
    "원달러": "KRW=X",
    "WTI유가": "CL=F",
    "금": "GC=F",
    "구리": "HG=F",
    "니케이": "^N225",
    "항셍": "^HSI",
}
FRED_SERIES = {"미국10Y": "DGS10", "미국2Y": "DGS2"}

INVESTOR_COLS = {"기관합계": "inst", "기타법인": "corp", "개인": "retail", "외국인합계": "foreign"}


# ── 공통 ────────────────────────────────────────────────────────────
def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


def ensure_dirs():
    OUT.mkdir(parents=True, exist_ok=True)
    KRX_DIR.mkdir(parents=True, exist_ok=True)


def atomic_parquet(df, path):
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp)
    os.replace(tmp, path)


def load_krx_env():
    p = Path.home() / ".krx_env"
    if not p.exists():
        log("경고: ~/.krx_env 없음 — KRX 로그인 실패 가능")
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def get_stock():
    load_krx_env()
    from pykrx import stock
    return stock


def ymd(s):
    return pd.Timestamp(s).strftime("%Y%m%d")


# ── 1. 매크로 (형세) ─────────────────────────────────────────────────
def cmd_macro(start=START_DEFAULT):
    import yfinance as yf

    log(f"macro: yfinance {len(YF_TICKERS)}개 다운로드 ({start}~)")
    raw = yf.download(list(YF_TICKERS.values()), start=start, progress=False,
                      auto_adjust=True, threads=True)["Close"]
    inv = {v: k for k, v in YF_TICKERS.items()}
    df = raw.rename(columns=inv)
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)

    for name, sid in FRED_SERIES.items():
        try:
            s = pd.read_csv(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}",
                            index_col=0, parse_dates=True, na_values=".")
            s = s.iloc[:, 0].astype(float).rename(name)
            df = df.join(s, how="outer")
        except Exception as e:
            log(f"macro: FRED {name} 실패 — {e!r}"[:200])

    df = df.sort_index()
    df = df[df.index >= pd.Timestamp(start)]
    df = df.dropna(how="all")
    atomic_parquet(df, OUT / "macro.parquet")

    last = {c: df[c].last_valid_index() for c in df.columns}
    stale = {c: d.date().isoformat() for c, d in last.items()
             if d is None or (df.index[-1] - d).days > 5}
    log(f"macro: 저장 {df.shape} {df.index[0].date()}→{df.index[-1].date()}")
    if stale:
        log(f"macro: 5일 이상 멈춘 항목 {stale}")


# ── 2. KRX 전 종목 스냅샷 (기세 원천) ─────────────────────────────────
def _load_skip():
    try:
        return set(json.loads(SKIP_PATH.read_text()))
    except Exception:
        return set()


def _save_skip(skip):
    SKIP_PATH.write_text(json.dumps(sorted(skip)))


def _year_file(y):
    return KRX_DIR / f"{y}.parquet"


def _existing_dates():
    have = set()
    for f in KRX_DIR.glob("*.parquet"):
        try:
            d = pd.read_parquet(f, columns=["date"])["date"].unique()
            have.update(pd.Timestamp(x).strftime("%Y%m%d") for x in d)
        except Exception as e:
            log(f"krx: {f.name} 읽기 실패 {e!r}")
    return have


def _fetch_day(stock, d):
    frames = []
    for mkt in ("KOSPI", "KOSDAQ"):
        df = None
        for attempt in range(3):
            try:
                df = stock.get_market_ohlcv(d, market=mkt)
                break
            except Exception as e:
                log(f"krx: {d} {mkt} 재시도 {attempt + 1} — {e!r}"[:200])
                time.sleep(3 * (attempt + 1))
        if df is None or df.empty or "종가" not in df.columns:
            return None
        df = df[df["종가"] > 0]
        if df.empty:
            return None
        df.index.name = "ticker"
        df = df.reset_index().rename(columns={
            "종가": "close", "거래량": "volume", "거래대금": "value",
            "등락률": "chg", "시가총액": "cap"})
        df = df[["ticker", "close", "volume", "value", "chg", "cap"]].copy()
        df["mkt"] = mkt
        frames.append(df)
        time.sleep(0.3)
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.Timestamp(d)
    return out


def _flush(buf):
    if not buf:
        return
    new = pd.concat(buf, ignore_index=True)
    for y, part in new.groupby(new["date"].dt.year):
        f = _year_file(y)
        if f.exists():
            part = pd.concat([pd.read_parquet(f), part], ignore_index=True)
        part = (part.drop_duplicates(["date", "ticker"], keep="last")
                    .sort_values(["date", "ticker"]).reset_index(drop=True))
        atomic_parquet(part, f)
    buf.clear()


def _trading_days(start, end):
    m = OUT / "macro.parquet"
    if m.exists():
        idx = pd.read_parquet(m, columns=["코스피"])["코스피"].dropna().index
    else:
        import yfinance as yf
        idx = yf.download("^KS11", start=start, progress=False)["Close"].dropna().index
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
    idx = idx[(idx >= pd.Timestamp(start)) & (idx <= pd.Timestamp(end))]
    return [d.strftime("%Y%m%d") for d in idx]


def _last_closed_day():
    now = datetime.now(KST)
    d = now.date() if (now.hour, now.minute) >= (15, 40) else now.date() - timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def _recent_weekdays(n=10):
    now = datetime.now(KST)
    # 장 마감 데이터 확정 전(15:40 이전)이면 오늘은 제외
    last = now.date() if (now.hour, now.minute) >= (15, 40) else now.date() - timedelta(days=1)
    days, d = [], last
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y%m%d"))
        d -= timedelta(days=1)
    return sorted(days)


def cmd_krx(start=None, end=None, recent=False):
    stock = get_stock()
    if recent:
        days, record_skip = _recent_weekdays(10), False
    else:
        start = start or START_DEFAULT
        end = min(pd.Timestamp(end or _last_closed_day()), pd.Timestamp(_last_closed_day())).strftime("%Y-%m-%d")
        days, record_skip = _trading_days(start, end), True

    have, skip = _existing_dates(), _load_skip()
    todo = [d for d in days if d not in have and d not in skip]
    log(f"krx: 대상 {len(days)}일 / 수집 필요 {len(todo)}일")

    buf, got, t0 = [], 0, time.time()
    for i, d in enumerate(todo, 1):
        df = _fetch_day(stock, d)
        if df is None:
            if record_skip:
                skip.add(d)
            log(f"krx: {d} 데이터 없음 — 건너뜀")
            continue
        buf.append(df)
        got += 1
        if len(buf) >= 20:
            _flush(buf)
            _save_skip(skip)
            el = time.time() - t0
            log(f"krx: {i}/{len(todo)} ({d}) — {el / i:.1f}s/일, 남은 예상 {el / i * (len(todo) - i) / 60:.0f}분")
    _flush(buf)
    _save_skip(skip)
    log(f"krx: 완료 — 신규 {got}일")


# ── 3. 투자자별 순매수 (기세 보조) ───────────────────────────────────
def cmd_flows(start=None, end=None):
    stock = get_stock()
    start = pd.Timestamp(start or START_DEFAULT)
    end = min(pd.Timestamp(end or _last_closed_day()), pd.Timestamp(_last_closed_day()))
    per_mkt = []
    for mkt in ("KOSPI", "KOSDAQ"):
        parts = []
        for y in range(start.year, end.year + 1):
            s = max(start, pd.Timestamp(f"{y}-01-01"))
            e = min(end, pd.Timestamp(f"{y}-12-31"))
            try:
                df = stock.get_market_trading_value_by_date(ymd(s), ymd(e), mkt)
            except Exception as ex:
                log(f"flows: {mkt} {y} 실패 — {ex!r}"[:200])
                continue
            if df is None or df.empty:
                log(f"flows: {mkt} {y} 비어 있음")
                continue
            df = df[[c for c in INVESTOR_COLS if c in df.columns]].rename(columns=INVESTOR_COLS)
            parts.append(df.add_prefix(f"{mkt}_"))
            time.sleep(0.5)
        if parts:
            per_mkt.append(pd.concat(parts))
    if not per_mkt:
        log("flows: 수집 결과 없음")
        return
    new = pd.concat(per_mkt, axis=1)
    new.index = pd.to_datetime(new.index)
    new = new[~new.index.duplicated(keep="last")].astype("float64")

    path = OUT / "flows.parquet"
    if path.exists():
        new = new.combine_first(pd.read_parquet(path))
    new = new.sort_index()
    atomic_parquet(new, path)
    log(f"flows: 저장 {new.shape} {new.index[0].date()}→{new.index[-1].date()}")


# ── 4. 시장 폭 집계 (기세) ───────────────────────────────────────────
def cmd_breadth():
    files = sorted(KRX_DIR.glob("*.parquet"))
    if not files:
        log("breadth: krx 데이터 없음")
        return
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df[df["ticker"].astype(str).str[-1] == "0"]  # 보통주만 (우선주 제외)
    log(f"breadth: {df['date'].nunique()}일 × {df['ticker'].nunique()}종목 집계")

    piv = lambda col: df.pivot(index="date", columns="ticker", values=col).sort_index()
    chg = piv("chg").astype("float32")
    volume = piv("volume")
    value = piv("value").astype("float64")
    mkt = piv("mkt")

    exists = chg.notna()
    traded = exists & volume.gt(0)
    # 수정주가 지수: 등락률(기준가 대비, 분할·증자 조정 반영)을 누적
    px = (1 + (chg.astype("float64") / 100).fillna(0)).cumprod().where(exists)
    del volume

    scopes = {"ALL": mkt.notna(), "KOSPI": mkt.eq("KOSPI"), "KOSDAQ": mkt.eq("KOSDAQ")}
    out = pd.DataFrame(index=chg.index)

    for sc, m in scopes.items():
        t = traded & m
        out[f"{sc}_n"] = t.sum(1)
        out[f"{sc}_adv"] = (chg.gt(0) & t).sum(1)
        out[f"{sc}_dec"] = (chg.lt(0) & t).sum(1)
        out[f"{sc}_value"] = value.where(m).sum(1) / 1e8  # 억원

    for w in (20, 60, 200):
        ma = px.rolling(w, min_periods=w).mean()
        valid = ma.notna() & traded
        above = px.gt(ma) & valid
        for sc, m in scopes.items():
            denom = (valid & m).sum(1)
            out[f"{sc}_above{w}"] = ((above & m).sum(1) / denom.where(denom > 0) * 100).round(2)
        del ma, valid, above

    for w in (60, 252):
        hi = px.rolling(w, min_periods=w).max()
        lo = px.rolling(w, min_periods=w).min()
        nh = px.ge(hi) & hi.notna() & traded
        nl = px.le(lo) & lo.notna() & traded
        for sc, m in scopes.items():
            out[f"{sc}_nh{w}"] = (nh & m).sum(1)
            out[f"{sc}_nl{w}"] = (nl & m).sum(1)
        del hi, lo, nh, nl

    atomic_parquet(out, OUT / "breadth.parquet")
    last = out.iloc[-1]
    log(f"breadth: 저장 {out.shape} {out.index[0].date()}→{out.index[-1].date()}")
    log("breadth: 최근 ALL — 상승 {:.0f} / 하락 {:.0f} / 20일선 위 {:.1f}% / 60일선 위 {:.1f}% / "
        "52주 신고 {:.0f} · 신저 {:.0f} / 거래대금 {:,.0f}억".format(
            last["ALL_adv"], last["ALL_dec"], last["ALL_above20"], last["ALL_above60"],
            last["ALL_nh252"], last["ALL_nl252"], last["ALL_value"]))


# ── 5. 일일 갱신 ─────────────────────────────────────────────────────
def cmd_daily():
    steps = [
        ("macro", lambda: cmd_macro()),
        ("krx", lambda: cmd_krx(recent=True)),
        ("flows", lambda: cmd_flows(start=(datetime.now(KST) - timedelta(days=40)).strftime("%Y-%m-%d"))),
        ("breadth", cmd_breadth),
    ]
    failed = []
    for name, fn in steps:
        try:
            fn()
        except Exception as e:
            failed.append(name)
            log(f"daily: {name} 실패 — {e!r}"[:300])
    log("daily: 완료" + (f" (실패: {failed})" if failed else ""))
    if failed:
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["macro", "krx", "flows", "breadth", "daily"])
    ap.add_argument("--start")
    ap.add_argument("--end")
    a = ap.parse_args()
    ensure_dirs()
    if a.cmd == "macro":
        cmd_macro(a.start or START_DEFAULT)
    elif a.cmd == "krx":
        cmd_krx(a.start, a.end)
    elif a.cmd == "flows":
        cmd_flows(a.start, a.end)
    elif a.cmd == "breadth":
        cmd_breadth()
    elif a.cmd == "daily":
        cmd_daily()


if __name__ == "__main__":
    main()
    