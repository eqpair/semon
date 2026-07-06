"""
build_corr.py — 500일 종가 수집 → 로그수익률 상관행렬 생성

서버(crawler.py / config.py 가 있는 곳)에서 실행:
    python3 build_corr.py

산출물:
    returns.parquet     : 종목 × 일자 로그수익률 (정렬·결측 처리 완료)
    corr_matrix.parquet : 종목 × 종목 상관행렬
    meta.json           : 종목→이름, 종목→원소속섹터 매핑
"""
import asyncio
import json
import numpy as np
import pandas as pd

import config
from crawler import fetch_all_ohlcv

MIN_DAYS = 250   # 이 일수 미만이면 신뢰 불가 → 제외 (신규상장 등)


def code_maps():
    name_of, sector_of = {}, {}
    for sec, lst in config.SECTORS.items():
        for code, name in lst:
            name_of[code] = name
            sector_of.setdefault(code, []).append(sec)  # 이미 다중일 수도 있게
    return name_of, sector_of


async def main():
    name_of, sector_of = code_maps()
    codes = list(name_of.keys())
    print(f"유니크 종목: {len(codes)}개 — OHLCV 수집 시작")

    raw = await fetch_all_ohlcv(codes)  # {code: {"closes":[...], "volumes":[...]}} or None

    # 종가 시리즈만 추출 (최신이 뒤 → 그대로 index 0..n)
    series = {}
    for code, d in raw.items():
        if d is None:
            continue
        closes = d["closes"]
        if len(closes) < MIN_DAYS:
            continue
        series[code] = np.asarray(closes, dtype=float)

    dropped = [c for c in codes if c not in series]
    print(f"수집 성공: {len(series)}개 / 데이터부족·실패 제외: {len(dropped)}개")
    if dropped:
        print("  제외:", ", ".join(f"{c}({name_of[c]})" for c in dropped[:30]))

    # 길이 통일: 가장 짧은 시리즈에 맞춰 '최근 구간' 정렬
    # (각 시리즈의 마지막 L개를 사용 → 최신 거래일 기준 정렬)
    L = min(len(s) for s in series.values())
    L = min(L, 500)
    print(f"공통 구간 길이: {L}일 (최근 기준 절단)")

    mat = {code: s[-L:] for code, s in series.items()}
    price_df = pd.DataFrame(mat)  # columns=code, rows=시간(과거→최신)

    # 로그수익률
    log_ret = np.log(price_df / price_df.shift(1)).iloc[1:]

    # 상관행렬 (열=종목)
    corr = log_ret.corr()

    log_ret.to_parquet("returns.parquet")
    corr.to_parquet("corr_matrix.parquet")
    with open("meta.json", "w", encoding="utf-8") as f:
        json.dump(
            {"name_of": name_of,
             "sector_of": sector_of,
             "n_days": int(L)},
            f, ensure_ascii=False, indent=2,
        )
    print("저장 완료: returns.parquet / corr_matrix.parquet / meta.json")


if __name__ == "__main__":
    asyncio.run(main())