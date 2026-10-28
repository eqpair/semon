#!/usr/bin/env python3
"""
stock_data.py — 종목 추출 엔진용 종목별 데이터 수집 (KRX, pykrx)

저장 위치: data/stocks/
  fund/YYYY.parquet   종목별 BPS·PER·PBR·EPS·DIV·DPS (long: date, ticker, ...)
                      과거는 주 1회(매주 마지막 거래일), 일일 갱신은 매일
  flow/YYYY.parquet   종목별 외국인·기관 순매수 거래대금 (원) (long: date, ticker, foreign, inst)

거래일 목록은 data/reflex/krx/*.parquet (reflex_data.py 산출물)의 실제 거래일을 사용.

사용법:
  python stock_data.py fund  --start 2010-01-01 [--end ...]   # 주 1회 백필 (이어받기)
  python stock_data.py flow  --start 2010-01-01 [--end ...]   # 일별 백필 (이어받기)
  python stock_data.py daily                                  # 최근 10거래일 (cron용)
"""
import os
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

import reflex_data as rd

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
OUT = BASE / "data" / "stocks"
KRX_DIR = BASE / "data" / "reflex" / "krx"
FUND_COLS = ["BPS", "PER", "PBR", "EPS", "DIV", "DPS"]
INVESTORS = {"외국인": "foreign", "기관합계": "inst"}
PAUSE = 1.2          # 날짜 사이 대기(초) — KRX 호출 제한 회피
MAX_FAIL_STREAK = 5  # 연속 실패 시 중단 (차단 상태에서 계속 두드리지 않음)


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


def atomic_parquet(df, path):
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_parquet(tmp)
    os.replace(tmp, path)


def trading_days(start=None, end=None):
    days = set()
    for f in sorted(KRX_DIR.glob("*.parquet")):
        days.update(pd.read_parquet(f, columns=["date"])["date"].unique())
    idx = pd.DatetimeIndex(sorted(days))
    if start:
        idx = idx[idx >= pd.Timestamp(start)]
    if end:
        idx = idx[idx <= pd.Timestamp(end)]
    return idx


def week_last(idx):
    """매주 마지막 거래일."""
    s = pd.Series(idx, index=idx)
    return pd.DatetimeIndex(s.groupby(idx.to_period("W-FRI")).max().values)


def existing_dates(kind):
    have = set()
    for f in (OUT / kind).glob("*.parquet"):
        try:
            have.update(pd.read_parquet(f, columns=["date"])["date"].unique())
        except Exception as e:
            log(f"{kind}: {f.name} 읽기 실패 {e!r}")
    return {pd.Timestamp(x) for x in have}


def flush(kind, buf):
    if not buf:
        return
    new = pd.concat(buf, ignore_index=True)
    for y, part in new.groupby(new["date"].dt.year):
        f = OUT / kind / f"{y}.parquet"
        if f.exists():
            part = pd.concat([pd.read_parquet(f), part], ignore_index=True)
        part = (part.drop_duplicates(["date", "ticker"], keep="last")
                    .sort_values(["date", "ticker"]).reset_index(drop=True))
        atomic_parquet(part, f)
    buf.clear()


def retry(fn, label, tries=3):
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            log(f"  {label} 재시도 {i + 1} — {e!r}"[:200])
            time.sleep(10 * (i + 1))
    return None


# ── 펀더멘털 ────────────────────────────────────────────────────────
def fetch_fund(stock, d):
    ds = d.strftime("%Y%m%d")
    df = retry(lambda: stock.get_market_fundamental(ds, market="ALL"), f"fund {ds}")
    if df is None or df.empty or "EPS" not in df.columns:
        return None
    df = df[[c for c in FUND_COLS if c in df.columns]].astype("float64")
    df.index.name = "ticker"
    df = df.reset_index()
    df["date"] = d
    return df


# ── 수급 ────────────────────────────────────────────────────────────
def fetch_flow(stock, d):
    ds = d.strftime("%Y%m%d")
    parts = {}
    for inv, col in INVESTORS.items():
        frames = []
        for mkt in ("KOSPI", "KOSDAQ"):
            df = retry(lambda: stock.get_market_net_purchases_of_equities(ds, ds, mkt, inv), f"flow {ds} {mkt} {inv}")
            if df is None:
                return None
            if not df.empty and "순매수거래대금" in df.columns:
                frames.append(df["순매수거래대금"].astype("float64"))
            time.sleep(0.15)
        if not frames:
            return None
        parts[col] = pd.concat(frames)
    out = pd.DataFrame(parts).fillna(0.0)
    out = out[~out.index.duplicated(keep="last")]
    out.index.name = "ticker"
    out = out.reset_index()
    out["date"] = d
    return out


FETCH = {"fund": fetch_fund, "flow": fetch_flow}


def run(kind, days):
    (OUT / kind).mkdir(parents=True, exist_ok=True)
    have = existing_dates(kind)
    todo = [d for d in days if d not in have]
    log(f"{kind}: 대상 {len(days)}일 / 수집 필요 {len(todo)}일")
    if not todo:
        return
    stock = rd.get_stock()
    buf, got, t0, streak = [], 0, time.time(), 0
    for i, d in enumerate(todo, 1):
        df = FETCH[kind](stock, d)
        time.sleep(PAUSE)
        if df is None:
            streak += 1
            log(f"{kind}: {d.date()} 데이터 없음 — 건너뜀 (연속 {streak})")
            if streak >= MAX_FAIL_STREAK:
                flush(kind, buf)
                log(f"{kind}: {MAX_FAIL_STREAK}일 연속 실패 — KRX 차단 의심, 중단. 나중에 같은 명령으로 이어받기")
                sys.exit(2)
            continue
        streak = 0
        buf.append(df)
        got += 1
        if len(buf) >= 20:
            flush(kind, buf)
            el = time.time() - t0
            log(f"{kind}: {i}/{len(todo)} ({d.date()}) — {el / i:.1f}s/일, 남은 예상 {el / i * (len(todo) - i) / 60:.0f}분")
    flush(kind, buf)
    log(f"{kind}: 완료 — 신규 {got}일")


# ── 업종 분류 (시점별, 분기 말) ─────────────────────────────────────
def quarter_last(idx):
    s = pd.Series(idx, index=idx)
    return pd.DatetimeIndex(s.groupby(idx.to_period("Q")).max().values)


def run_sector(days):
    """분기 말 거래일마다 KOSPI·KOSDAQ 전 종목의 KRX 업종 분류 → data/stocks/sector/sector_pit.parquet"""
    out_dir = OUT / "sector"; out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "sector_pit.parquet"
    have = set(pd.read_parquet(path, columns=["date"])["date"].unique()) if path.exists() else set()
    todo = [d for d in days if pd.Timestamp(d) not in {pd.Timestamp(x) for x in have}]
    log(f"sector: 대상 {len(days)}개 분기 / 수집 필요 {len(todo)}")
    if not todo:
        return
    stock = rd.get_stock()
    frames, streak = [], 0
    for d in todo:
        ds = d.strftime("%Y%m%d"); parts = []
        for mkt in ("KOSPI", "KOSDAQ"):
            df = retry(lambda: stock.get_market_sector_classifications(ds, mkt), f"sector {ds} {mkt}")
            time.sleep(PAUSE)
            if df is None or df.empty or "업종명" not in df.columns:
                parts = None; break
            df = df.rename(columns={"종목명": "name", "업종명": "industry", "시가총액": "cap"})[["name", "industry", "cap"]]
            df.index.name = "ticker"; df = df.reset_index(); df["mkt"] = mkt
            parts.append(df)
        if parts is None:
            streak += 1
            log(f"sector: {d.date()} 실패 (연속 {streak})")
            if streak >= MAX_FAIL_STREAK:
                break
            continue
        streak = 0
        x = pd.concat(parts, ignore_index=True); x["date"] = d
        frames.append(x)
        log(f"sector: {d.date()} {len(x)}종목")
    if frames:
        new = pd.concat(frames, ignore_index=True)
        if path.exists():
            new = pd.concat([pd.read_parquet(path), new], ignore_index=True)
        new = new.drop_duplicates(["date", "ticker"], keep="last").sort_values(["date", "ticker"])
        atomic_parquet(new, path)
        log(f"sector: 저장 {new.shape}")


def last_closed():
    return pd.Timestamp(rd._last_closed_day())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["fund", "flow", "daily", "sector"])
    ap.add_argument("--start", default="2010-01-01")
    ap.add_argument("--end")
    a = ap.parse_args()
    end = min(pd.Timestamp(a.end), last_closed()) if a.end else last_closed()

    if a.cmd == "fund":
        run("fund", list(week_last(trading_days(a.start, end))))
    elif a.cmd == "flow":
        run("flow", list(trading_days(a.start, end)))
    elif a.cmd == "sector":
        run_sector(list(quarter_last(trading_days(a.start, end))))
    elif a.cmd == "daily":
        recent = list(trading_days(None, end)[-10:])
        # 펀더멘털은 주 1회(그 주 마지막 거래일)만: 이번 주는 금요일에만, 지난주 것은 빠졌으면 채움
        fund_days = [d for d in week_last(trading_days(None, end)[-15:]) if d.weekday() == 4 or d < recent[-1]]
        failed = []
        for kind, days in (("flow", recent), ("fund", fund_days)):
            try:
                run(kind, days)
            except SystemExit as e:
                failed.append(kind)
                log(f"daily: {kind} 중단 (code {e.code}) — KRX 차단 의심, 내일 다시 시도")
                break
            except Exception as e:
                failed.append(kind)
                log(f"daily: {kind} 실패 — {e!r}"[:300])
        # 업종 분류는 최근 7일 안에 받은 것이 없을 때만 (주 1회)
        sp = OUT / "sector" / "sector_pit.parquet"
        last_sec = pd.read_parquet(sp, columns=["date"])["date"].max() if sp.exists() else None
        if not failed and (last_sec is None or (end - pd.Timestamp(last_sec)).days >= 7):
            try:
                run_sector([end])
            except Exception as e:
                failed.append("sector")
                log(f"daily: sector 실패 — {e!r}"[:300])
        log("daily: 완료" + (f" (실패: {failed})" if failed else ""))
        sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
