"""
sweep_params.py — 여러 컷값에서 섹터 분포 안정성 비교

목적: "형평성 있고 안정적인" 섹터 분포가 나오는 파라미터 구간을 찾는다.
      감이 아니라 곡선을 보고 결정.

build_corr_neutral.py 실행 후 (잔차 corr_matrix.parquet 사용):
    python3 sweep_params.py

각 TIGHT_CUT 에 대해 출력하는 안정성 지표:
  - 섹터수        : 채택된 그룹 개수
  - 미배정%       : 어느 그룹에도 못 든 종목 비율 (낮을수록 커버리지 좋음)
  - 평균크기      : 섹터당 평균 종목수 (4~10이 이상적)
  - 크기중앙값    : n의 중앙값 (2에 몰리면 형평성 나쁨)
  - n2비율        : 크기 2짜리 섹터 비율 (높으면 잘게 부서짐)
  - 4~10비율      : 이상적 크기 섹터 비율 (높을수록 좋음)
  - 평균응집도    : 전체 섹터 평균 내부상관
  - 응집도편차    : 섹터간 응집도 표준편차 (낮을수록 고름)

권장 판단:
  미배정 20~35%, 4~10비율 최대, 응집도편차 최소 인 컷을 고른다.
"""
import numpy as np
import pandas as pd
from collections import defaultdict
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

corr = pd.read_parquet("corr_matrix.parquet")
codes = list(corr.columns)
N = len(codes)

D = np.clip(1 - corr.values, 0, 2)
np.fill_diagonal(D, 0)
D = (D + D.T) / 2
Z = linkage(squareform(D, checks=False), method="average")


def cohesion(members):
    if len(members) < 2:
        return np.nan
    sub = corr.loc[members, members].values
    iu = np.triu_indices(len(members), k=1)
    return float(np.nanmean(sub[iu]))


print(f"전체 종목: {N}")
print("=" * 96)
print(f"{'CUT':>5} {'corr':>5} {'섹터수':>6} {'미배정%':>7} {'평균크기':>7} {'크기중앙':>7} "
      f"{'n2비율':>7} {'4-10비율':>8} {'평균응집':>7} {'응집편차':>7}")
print("-" * 96)

# MIN_COH 도 함께 스윕: 잔차 분포에 맞춰 여러 값
for MIN_COH in [0.30, 0.35, 0.40]:
    print(f"\n--- MIN_COH = {MIN_COH} ---")
    for CUT in [0.55, 0.60, 0.65, 0.70, 0.75, 0.80]:
        labels = fcluster(Z, t=CUT, criterion="distance")
        groups = defaultdict(list)
        for c, l in zip(codes, labels):
            groups[l].append(c)

        accepted = []
        unassigned = 0
        for members in groups.values():
            coh = cohesion(members)
            if len(members) < 2 or (not np.isnan(coh) and coh < MIN_COH):
                unassigned += len(members)
                continue
            accepted.append((members, coh))

        if not accepted:
            print(f"{CUT:>5.2f} {1-CUT:>5.2f} {'0':>6} {'—':>7}")
            continue

        sizes = np.array([len(m) for m, _ in accepted])
        cohs = np.array([c for _, c in accepted])
        n_sectors = len(accepted)
        unassigned_pct = 100 * unassigned / N
        mean_size = sizes.mean()
        med_size = np.median(sizes)
        n2_pct = 100 * np.mean(sizes == 2)
        ideal_pct = 100 * np.mean((sizes >= 4) & (sizes <= 10))
        mean_coh = cohs.mean()
        coh_std = cohs.std()

        print(f"{CUT:>5.2f} {1-CUT:>5.2f} {n_sectors:>6} {unassigned_pct:>6.1f}% "
              f"{mean_size:>7.1f} {med_size:>7.0f} {n2_pct:>6.1f}% {ideal_pct:>7.1f}% "
              f"{mean_coh:>7.3f} {coh_std:>7.3f}")

print("\n" + "=" * 96)
print("판단 가이드:")
print("  · 미배정% 20~35 구간에서")
print("  · 4-10비율(이상적 크기)이 가장 높고")
print("  · 응집편차(섹터간 고름)가 가장 낮은 CUT/MIN_COH 조합 선택")
print("  · n2비율이 40%↑ 면 너무 잘게 부서진 것 → CUT 높여 완화")