#!/usr/bin/env python3
"""
consensus_data.py — 증권사 투자의견·목표주가 기록 수집 (KIS 종목투자의견 API)

저장: data/consensus/opinions.parquet
  date, ticker, broker, opinion, opinion_code, prev_opinion, prev_code, target, prev_close

사용법:
  python consensus_data.py backfill          # 과거 전체 (이어받기 지원, 약 1시간)
  python consensus_data.py daily             # 최근 상위 600종목의 최근 30일만 갱신 (cron용)
  python consensus_data.py status            # 수집 현황

대상 종목: 2010년 이후 한 번이라도 20일 평균 거래대금 상위 600에 든 종목 (우선주 제외)
"""
import asyncio, json, sys, time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
from kis_auth import get_access_token, APP_KEY, APP_SECRET, BASE_URL  # noqa: E402

KST = ZoneInfo("Asia/Seoul")
KRX_DIR = BASE / "data" / "reflex" / "krx"
OUT = BASE / "data" / "consensus"
PQ = OUT / "opinions.parquet"
DONE = OUT / "backfill_done.json"
PATH = "/uapi/domestic-stock/v1/quotations/invest-opinion"
TR_ID = "FHKST663300C0"
MIN_GAP = 0.25          # 호출 간 최소 간격(초) — 다른 서비스와 KIS 한도를 나눠 쓰므로 여유 있게
COLS = {"stck_bsop_date": "date", "mbcr_name": "broker", "invt_opnn": "opinion", "invt_opnn_cls_code": "opinion_code",
        "rgbf_invt_opnn": "prev_opinion", "rgbf_invt_opnn_cls_code": "prev_code", "hts_goal_prc": "target",
        "stck_prdy_clpr": "prev_close"}


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


class KIS:
    def __init__(self):
        self.token = asyncio.run(get_access_token())
        self.last = 0.0
        self.s = requests.Session()

    def get(self, params):
        h = {"authorization": f"Bearer {self.token}", "appkey": APP_KEY, "appsecret": APP_SECRET,
             "tr_id": TR_ID, "custtype": "P", "content-type": "application/json; charset=utf-8"}
        for attempt in range(8):
            w = MIN_GAP - (time.time() - self.last)
            if w > 0:
                time.sleep(w)
            self.last = time.time()
            try:
                j = self.s.get(BASE_URL + PATH, headers=h, params=params, timeout=15).json()
            except Exception as e:
                log("  네트워크 오류, 재시도", repr(e)[:120]); time.sleep(2 + attempt * 2); continue
            if j.get("rt_cd") == "0":
                return j.get("output") or []
            msg = f"{j.get('msg_cd')} {j.get('msg1')}"
            if "EGW00201" in msg or "초당" in msg:          # 호출 한도 초과 → 쉬었다 재시도
                time.sleep(1 + attempt); continue
            if "EGW00123" in msg or "토큰" in msg:           # 토큰 만료 → 재발급
                self.token = asyncio.run(get_access_token()); continue
            log("  API 오류:", msg[:150]); return None
        return None


def fetch(kis, code, since="20000101", until=None):
    """한 번에 최근 100건만 오므로 가장 오래된 날짜 앞으로 기간을 옮겨 가며 반복."""
    end = until or datetime.now(KST).strftime("%Y%m%d")
    rows, seen = [], set()
    while end >= since:
        out = kis.get({"FID_COND_MRKT_DIV_CODE": "J", "FID_COND_SCR_DIV_CODE": "16633", "FID_INPUT_ISCD": code,
                       "FID_INPUT_DATE_1": since, "FID_INPUT_DATE_2": end})
        if out is None:
            return None                      # 실패 → 다음에 다시
        new = 0
        for r in out:
            k = (r.get("stck_bsop_date"), r.get("mbcr_name"), r.get("hts_goal_prc"), r.get("invt_opnn"))
            if k not in seen:
                seen.add(k); rows.append(r); new += 1
        if len(out) < 100 or new == 0:
            break
        oldest = min(r["stck_bsop_date"] for r in out)
        end = (datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    if not rows:
        return pd.DataFrame(columns=["ticker"] + list(COLS.values()))
    df = pd.DataFrame(rows)[[c for c in COLS if c in rows[0]]].rename(columns=COLS)
    df["ticker"] = code
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    for c in ("target", "prev_close"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def save(parts):
    if not parts:
        return
    new = pd.concat(parts, ignore_index=True)
    if PQ.exists():
        new = pd.concat([pd.read_parquet(PQ), new], ignore_index=True)
    new = new.drop_duplicates(["ticker", "date", "broker", "target", "opinion"]).sort_values(["ticker", "date"])
    tmp = PQ.with_suffix(".tmp")
    new.to_parquet(tmp, index=False)
    tmp.replace(PQ)


def universe(recent_only=False):
    """거래대금 20일 평균 상위 600 (recent_only면 최근 거래일 기준, 아니면 2010년 이후 한 번이라도)."""
    files = sorted(KRX_DIR.glob("*.parquet"))
    if recent_only:
        files = files[-1:]
    df = pd.concat([pd.read_parquet(f, columns=["date", "ticker", "value"]) for f in files], ignore_index=True)
    df = df[df.ticker.str[-1] == "0"]                     # 우선주 제외
    v = df.pivot_table(index="date", columns="ticker", values="value").sort_index()
    liq = v.rolling(20, min_periods=15).mean()
    if recent_only:
        return list(liq.iloc[-1].dropna().sort_values(ascending=False).index[:600])
    rk = liq.rank(axis=1, ascending=False)
    ever = (rk <= 600).any()
    last = liq.ffill().iloc[-1]
    return list(last[ever[ever].index].sort_values(ascending=False, na_position="last").index)


def cmd_backfill():
    OUT.mkdir(parents=True, exist_ok=True)
    done = set(json.load(open(DONE))) if DONE.exists() else set()
    tickers = [t for t in universe() if t not in done]
    log(f"backfill: 남은 종목 {len(tickers)}개 (완료 {len(done)}개)")
    kis, parts, t0, nrow = KIS(), [], time.time(), 0
    for i, t in enumerate(tickers, 1):
        df = fetch(kis, t)
        if df is None:
            log(f"  {t} 실패 — 다음 실행 때 다시"); continue
        parts.append(df); done.add(t); nrow += len(df)
        if i % 50 == 0 or i == len(tickers):
            save(parts); parts = []
            json.dump(sorted(done), open(DONE, "w"))
            el = time.time() - t0
            log(f"  {i}/{len(tickers)} — 누적 {nrow:,}건, 남은 예상 {el / i * (len(tickers) - i) / 60:.0f}분")
    save(parts); json.dump(sorted(done), open(DONE, "w"))
    log("backfill: 완료")


def cmd_daily():
    OUT.mkdir(parents=True, exist_ok=True)
    since = (datetime.now(KST) - timedelta(days=30)).strftime("%Y%m%d")
    tickers = universe(recent_only=True)
    kis, parts, fail = KIS(), [], 0
    for t in tickers:
        df = fetch(kis, t, since=since)
        if df is None:
            fail += 1; continue
        parts.append(df)
    save(parts)
    log(f"daily: {len(tickers)}종목, 신규 {sum(len(p) for p in parts):,}건 (중복 포함), 실패 {fail}")


def cmd_status():
    if not PQ.exists():
        print("아직 데이터 없음"); return
    d = pd.read_parquet(PQ)
    print(f"{len(d):,}건, 종목 {d.ticker.nunique():,}개, 증권사 {d.broker.nunique()}곳, {d.date.min():%Y-%m-%d} ~ {d.date.max():%Y-%m-%d}")
    print(d.groupby(d.date.dt.year).size().to_string())
    if DONE.exists():
        print("backfill 완료 종목:", len(json.load(open(DONE))))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    {"backfill": cmd_backfill, "daily": cmd_daily, "status": cmd_status}[cmd]()