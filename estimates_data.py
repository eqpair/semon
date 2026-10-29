#!/usr/bin/env python3
"""
estimates_data.py — 종목 추정 실적 주간 스냅샷 (KIS 국내주식 종목추정실적)

과거 기록을 주지 않는 API라, 매주 그 시점의 추정치를 저장해 쌓는다 (나중에 '추정치 상향·하향' 검증용).
저장: data/consensus/estimates/YYYY-MM-DD.parquet
  asof, ticker, name, estdate(추정일), analyst(작성), raw(응답 전체 JSON — 항목 해석은 검증 때)
대상: 최근 거래대금 상위 600종목. 한국투자증권이 분석하지 않는 종목은 빈 응답이라 건너뜀.
사용법: python estimates_data.py            # 오늘 스냅샷 (같은 날 다시 실행하면 덮어씀)
        python estimates_data.py status     # 쌓인 현황
"""
import json, sys, time
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))
import consensus_data as cd  # noqa: E402  (KIS 토큰·호출 간격·대상 종목 재사용)

OUT = BASE / "data" / "consensus" / "estimates"
PATH = "/uapi/domestic-stock/v1/quotations/estimate-perform"
TR_ID = "HHKST668300C0"


def fetch_one(kis, code):
    h = {"authorization": f"Bearer {kis.token}", "appkey": cd.APP_KEY, "appsecret": cd.APP_SECRET,
         "tr_id": TR_ID, "custtype": "P", "content-type": "application/json; charset=utf-8"}
    for attempt in range(6):
        w = cd.MIN_GAP - (time.time() - kis.last)
        if w > 0:
            time.sleep(w)
        kis.last = time.time()
        try:
            j = kis.s.get(cd.BASE_URL + PATH, headers=h, params={"SHT_CD": code}, timeout=15).json()
        except Exception:
            time.sleep(2 + attempt * 2); continue
        if j.get("rt_cd") == "0":
            return j
        msg = f"{j.get('msg_cd')} {j.get('msg1')}"
        if "EGW00201" in msg or "초당" in msg:
            time.sleep(1 + attempt); continue
        if "EGW00123" in msg or "토큰" in msg:
            import asyncio
            kis.token = asyncio.run(cd.get_access_token()); continue
        return None
    return None


def cmd_snapshot():
    OUT.mkdir(parents=True, exist_ok=True)
    today = datetime.now(cd.KST).strftime("%Y-%m-%d")
    tickers = cd.universe(recent_only=True)
    kis, rows, fail, empty = cd.KIS(), [], 0, 0
    for t in tickers:
        j = fetch_one(kis, t)
        if j is None:
            fail += 1; continue
        o1 = j.get("output1") or {}
        if not (o1.get("item_kor_nm") or "").strip() or not (j.get("output2") or []):
            empty += 1; continue
        rows.append(dict(asof=today, ticker=t, name=o1.get("item_kor_nm"), estdate=o1.get("estdate"),
                         analyst=o1.get("rcmd_name"),
                         raw=json.dumps({k: j.get(k) for k in ("output1", "output2", "output3", "output4")}, ensure_ascii=False)))
    if rows:
        f = OUT / f"{today}.parquet"
        pd.DataFrame(rows).to_parquet(f.with_suffix(".tmp"), index=False)
        f.with_suffix(".tmp").replace(f)
    cd.log(f"estimates: {today} — 저장 {len(rows)}종목, 추정치 없음 {empty}, 실패 {fail} (대상 {len(tickers)})")


def cmd_status():
    fs = sorted(OUT.glob("*.parquet"))
    if not fs:
        print("아직 스냅샷 없음"); return
    for f in fs[-10:]:
        d = pd.read_parquet(f, columns=["ticker", "estdate"])
        print(f.stem, f"{len(d)}종목", "추정일 최신", d.estdate.max())
    print(f"총 {len(fs)}회")


if __name__ == "__main__":
    {"status": cmd_status}.get(sys.argv[1] if len(sys.argv) > 1 else "", cmd_snapshot)()
