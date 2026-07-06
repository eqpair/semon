"""
build_corr_neutral.py — 시장중립화 잔차 상관행렬 생성
각 종목 수익률에서 KOSPI+KOSDAQ 2팩터 회귀 잔차만 남겨 상관 계산.
산출: corr_matrix.parquet(잔차) / corr_matrix_raw.parquet(원본) / returns.parquet / meta.json
"""
import asyncio
import json
import numpy as np
import pandas as pd

import config
from crawler import fetch_all_ohlcv, fetch_kospi_ohlcv, fetch_kosdaq_ohlcv

MIN_DAYS = 250


def code_maps():
    name_of, sector_of = {}, {}
    for sec, lst in config.SECTORS.items():
        for code, name in lst:
            name_of[code] = name
            sector_of.setdefault(code, []).append(sec)
    return name_of, sector_of


def log_ret(arr):
    a = np.asarray(arr, dtype=float)
    return np.log(a[1:] / a[:-1])


async def main():
    name_of, sector_of = code_maps()
    codes = list(name_of.keys())
    print(f"유니크 종목: {len(codes)}개 — 수집 시작")

    raw = await fetch_all_ohlcv(codes)
    kospi = await fetch_kospi_ohlcv()
    kosdaq = await fetch_kosdaq_ohlcv()
    if kospi is None or kosdaq is None:
        raise SystemExit("지수 fetch 실패 — KOSPI/KOSDAQ 없이는 중립화 불가")

    series = {}
    for code, d in raw.items():
        if d is None:
            continue
        closes = d["closes"]
        if len(closes) >= MIN_DAYS:
            series[code] = np.asarray(closes, dtype=float)

    dropped = [c for c in codes if c not in series]
    print(f"수집 성공: {len(series)}개 / 제외: {len(dropped)}개")

    L = min([len(s) for s in series.values()] + [len(kospi), len(kosdaq)])
    L = min(L, 500)
    print(f"공통 구간: {L}일")

    px = pd.DataFrame({c: s[-L:] for c, s in series.items()})
    R = np.log(px / px.shift(1)).iloc[1:]
    mkt = pd.DataFrame({
        "KOSPI": log_ret(kospi[-L:]),
        "KOSDAQ": log_ret(kosdaq[-L:]),
    }, index=R.index)

    X = np.column_stack([np.ones(len(mkt)), mkt["KOSPI"].values, mkt["KOSDAQ"].values])
    XtX_inv = np.linalg.pinv(X.T @ X)
    H = X @ XtX_inv @ X.T
    resid = R.values - H @ R.values
    resid_df = pd.DataFrame(resid, index=R.index, columns=R.columns)

    corr_raw = R.corr()
    corr_neutral = resid_df.corr()

    corr_neutral.to_parquet("corr_matrix.parquet")
    corr_raw.to_parquet("corr_matrix_raw.parquet")
    resid_df.to_parquet("returns.parquet")
    with open("meta.json", "w", encoding="utf-8") as f:
        json.dump({"name_of": name_of, "sector_of": sector_of, "n_days": int(L)},
                  f, ensure_ascii=False, indent=2)

    iu = np.triu_indices(len(corr_raw), k=1)
    vr = corr_raw.values[iu]; vr = vr[~np.isnan(vr)]
    vn = corr_neutral.values[iu]; vn = vn[~np.isnan(vn)]
    print("\n=== 원상관 vs 잔차상관 분포 ===")
    print(f"{'분위':>6} {'원상관':>8} {'잔차상관':>8}")
    for q in [50, 75, 90, 95, 99]:
        print(f"{q:>5}% {np.percentile(vr,q):>8.3f} {np.percentile(vn,q):>8.3f}")
    print(f"{'평균':>6} {vr.mean():>8.3f} {vn.mean():>8.3f}")
    print("\n저장 완료")


if __name__ == "__main__":
    asyncio.run(main())
