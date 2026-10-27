#!/usr/bin/env python3
"""
macro_kr.py — 형세(Fundamentals) 재구성용 국내 실물·자금 지표 수집

출처: 한국은행 ECOS API (키: ~/.ecos_env 의 ECOS_KEY), yfinance(위안화)
저장: data/reflex/kr_daily.parquet    일별 (금리, 위안화)
      data/reflex/kr_monthly.parquet  월별 (수출금액지수, 경기종합지수) + 사용 가능일

월별 통계는 발표가 늦으므로 각 월 데이터를 실제로 알 수 있었던 날부터만 쓰도록
'avail' 열(사용 가능일)을 함께 저장한다. 보수적으로 잡은 시차:
  수출금액지수(403Y001)  : M월 → M+2월 1일부터 (실제 발표는 M+1월 중순 무렵)
  경기종합지수(901Y067)  : M월 → M+2월 1일부터 (통계청 산업활동동향, M+1월 말 발표)

사용법: python macro_kr.py        # 2009년부터 전체 갱신 (1분 이내)
"""
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
OUT = BASE / "data" / "reflex"
START = "2009"

DAILY = {   # (표, 항목) -> 열 이름
    ("817Y002", "010200000"): "국고3Y",
    ("817Y002", "010210000"): "국고10Y",
    ("817Y002", "010300000"): "회사채AA",
    ("817Y002", "010320000"): "회사채BBB",
    ("817Y002", "010502000"): "CD91",
}
MONTHLY = {
    ("403Y001", "*AA"): ("수출지수", 2),
    ("403Y001", "3091AA"): ("반도체수출지수", 2),
    ("901Y067", "I16E"): ("선행순환", 2),
    ("901Y067", "I16D"): ("동행순환", 2),
}


def log(*a):
    print(datetime.now(KST).strftime("%m-%d %H:%M:%S"), *a, flush=True)


def ecos_key():
    p = Path.home() / ".ecos_env"
    m = re.search(r"ECOS_KEY=(\S+)", p.read_text()) if p.exists() else None
    if not m:
        sys.exit("~/.ecos_env 에 ECOS_KEY 없음")
    return m.group(1)


def ecos(key, table, cycle, start, end, item):
    rows, i, step = [], 1, 10000
    while True:
        url = (f"https://ecos.bok.or.kr/api/StatisticSearch/{key}/json/kr/{i}/{i + step - 1}/"
               f"{table}/{cycle}/{start}/{end}/{item}")
        for t in range(3):
            try:
                j = requests.get(url, timeout=60).json()
                break
            except Exception as e:
                log(f"  재시도 {t + 1} {table}/{item}: {e!r}"[:160])
                time.sleep(5 * (t + 1))
        else:
            raise RuntimeError(f"ECOS 실패 {table}/{item}")
        if "StatisticSearch" not in j:
            msg = j.get("RESULT", {}).get("MESSAGE", str(j)[:200])
            if rows:
                break
            raise RuntimeError(f"ECOS {table}/{item}: {msg}")
        part = j["StatisticSearch"]["row"]
        rows += part
        total = int(j["StatisticSearch"].get("list_total_count", len(rows)))
        if len(rows) >= total or not part:
            break
        i += step
        time.sleep(0.3)
    s = pd.Series({r["TIME"]: pd.to_numeric(r["DATA_VALUE"], errors="coerce") for r in rows}).sort_index()
    return s


def atomic(df, path):
    tmp = path.with_suffix(".parquet.tmp")
    df.to_parquet(tmp)
    os.replace(tmp, path)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    key = ecos_key()
    today = datetime.now(KST)

    # 일별
    cols = {}
    for (tb, it), name in DAILY.items():
        s = ecos(key, tb, "D", f"{START}0101", today.strftime("%Y%m%d"), it)
        s.index = pd.to_datetime(s.index, format="%Y%m%d")
        cols[name] = s
        log(f"{name}: {len(s)}일 {s.index[0].date()}→{s.index[-1].date()}")
    try:
        import yfinance as yf
        c = yf.download("CNY=X", start=f"{START}-01-01", progress=False, auto_adjust=True)["Close"]
        c = c.iloc[:, 0] if isinstance(c, pd.DataFrame) else c
        if getattr(c.index, "tz", None) is not None:
            c.index = c.index.tz_localize(None)
        cols["위안달러"] = c.dropna()
        log(f"위안달러: {len(cols['위안달러'])}일")
    except Exception as e:
        log(f"위안달러 실패(무시): {e!r}"[:200])
    daily = pd.DataFrame(cols).sort_index()
    atomic(daily, OUT / "kr_daily.parquet")
    log(f"kr_daily 저장 {daily.shape}")

    # 월별 + 사용 가능일
    frames = []
    for (tb, it), (name, lag) in MONTHLY.items():
        s = ecos(key, tb, "M", f"{START}01", today.strftime("%Y%m"), it)
        m = pd.PeriodIndex(s.index, freq="M")
        frames.append(pd.DataFrame({"month": m.to_timestamp(), "series": name, "value": s.values,
                                    "avail": (m + lag).to_timestamp()}))
        log(f"{name}: {len(s)}개월 {m[0]}→{m[-1]} (사용 가능 시차 {lag}개월)")
    monthly = pd.concat(frames, ignore_index=True)
    atomic(monthly, OUT / "kr_monthly.parquet")
    log(f"kr_monthly 저장 {monthly.shape}")


if __name__ == "__main__":
    main()
