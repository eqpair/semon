#!/usr/bin/env python3
"""
sectors_krx.py — RRG 엔진(main.py) 업종·종목을 KRX 업종으로 통일

Picks·Focus와 같은 기준:
  종목 = 최근 20거래일 평균 거래대금 상위 600 (우선주 제외)
  업종 = KRX 업종(최신 sector_pit) → stock_score.UNI로 묶은 업종 ('기타' 제외)
산출: data/sectors_krx.json  {업종: [[코드, 종목명], …]}  — config.py가 있으면 이 파일을 SECTORS로 사용
       (파일을 지우고 semon을 재시작하면 예전 테마 업종으로 돌아감)
사용법: python sectors_krx.py           # 갱신 (매주 금요일 cron, semon은 매일 08:00 재시작 때 반영)
"""
import json
from pathlib import Path

import pandas as pd

import stock_score as ss

OUT = ss.BASE / "data" / "sectors_krx.json"
N = 600


def main():
    years = sorted(int(f.stem) for f in ss.KRX_DIR.glob("*.parquet"))
    px = ss.read_years(ss.KRX_DIR, years[-2:], ["ticker", "value", "date"])
    px = px[px.ticker.astype(str).str[-1] == "0"]
    px["date"] = pd.to_datetime(px["date"])
    last20 = sorted(px.date.unique())[-20:]
    liq = px[px.date.isin(last20)].groupby("ticker").value.mean().sort_values(ascending=False)

    sp = pd.read_parquet(ss.ST / "sector" / "sector_pit.parquet")
    sp["date"] = pd.to_datetime(sp["date"])
    last = sp[sp.date == sp.date.max()].set_index("ticker")
    sec = last.industry[last.industry != "기타"].map(lambda v: ss.UNI.get(v, v))
    names = last.name

    tk = [t for t in liq.index if t in sec.index][:N]
    out = {}
    for t in tk:
        out.setdefault(sec[t], []).append([t, str(names.get(t, t))])
    out = dict(sorted(out.items(), key=lambda kv: -len(kv[1])))
    old = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else {}
    oldset = {c for v in old.values() for c, _ in v}
    newset = {c for v in out.values() for c, _ in v}
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(OUT)
    ss.log(f"sectors_krx: 업종 {len(out)}개, 종목 {len(newset)}개 (기준 {sp.date.max():%Y-%m-%d})"
           + (f" | 편입 {len(newset - oldset)} · 편출 {len(oldset - newset)}" if old else "")
           + " | " + ", ".join(f"{k} {len(v)}" for k, v in out.items()))


if __name__ == "__main__":
    main()
