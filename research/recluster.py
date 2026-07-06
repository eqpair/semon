"""
recluster.py — 상관행렬 기반 계층적 재클러스터링 + 임계값 스윕

build_corr.py 실행 후:
    python3 recluster.py

출력:
  [A] 여러 컷 높이(distance)에서 클러스터 개수·응집도 스윕
  [B] 선택 높이에서의 실제 클러스터 구성 (원소속 섹터와 대조)
  [C] 다중소속 후보 수를 임계값 0.45/0.50/0.55/best비율 별로 비교
  [D] 반도체 계열만 따로 떼서 서브클러스터 구조 확대

scipy 필요: pip install scipy
"""
import json
import numpy as np
import pandas as pd
from collections import defaultdict
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

corr = pd.read_parquet("corr_matrix.parquet")
meta = json.load(open("meta.json", encoding="utf-8"))
name_of = meta["name_of"]
sector_of = meta["sector_of"]
codes = list(corr.columns)
name = lambda c: name_of.get(c, c)

# 거리행렬: 1 - corr, 음수상관은 거리 최대로 클립
D = 1.0 - corr.values
np.fill_diagonal(D, 0.0)
D = np.clip(D, 0.0, 2.0)
D = (D + D.T) / 2.0          # 대칭 보정
condensed = squareform(D, checks=False)
Z = linkage(condensed, method="average")


def cluster_cohesion(members):
    if len(members) < 2:
        return np.nan
    sub = corr.loc[members, members].values
    iu = np.triu_indices(len(members), k=1)
    return float(np.nanmean(sub[iu]))


# ── [A] 컷 높이 스윕 ─────────────────────────────────────────
print("=" * 64)
print("[A] 컷 높이별 클러스터 개수 / 평균 응집도 / 단일종목클러스터 수")
print("=" * 64)
print(f"{'height':>7} {'corr컷':>7} {'#클러스터':>9} {'평균응집도':>9} {'단일':>5} {'최대크기':>7}")
for h in [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75]:
    labels = fcluster(Z, t=h, criterion="distance")
    groups = defaultdict(list)
    for c, l in zip(codes, labels):
        groups[l].append(c)
    cohs = [cluster_cohesion(m) for m in groups.values() if len(m) >= 2]
    singles = sum(1 for m in groups.values() if len(m) == 1)
    maxsize = max(len(m) for m in groups.values())
    mean_coh = np.nanmean(cohs) if cohs else float("nan")
    print(f"{h:>7.2f} {1-h:>7.2f} {len(groups):>9} {mean_coh:>9.3f} {singles:>5} {maxsize:>7}")

# ── [B] 선택 높이 클러스터 구성 ──────────────────────────────
CUT = 0.55   # 필요시 [A] 보고 조정 (corr≈0.45 지점)
print("\n" + "=" * 64)
print(f"[B] height={CUT} (corr≈{1-CUT:.2f}) 클러스터 구성 — 크기 2 이상만")
print("=" * 64)
labels = fcluster(Z, t=CUT, criterion="distance")
groups = defaultdict(list)
for c, l in zip(codes, labels):
    groups[l].append(c)

for l in sorted(groups, key=lambda x: -len(groups[x])):
    m = groups[l]
    if len(m) < 2:
        continue
    # 이 클러스터 구성원의 원소속 섹터 분포
    origin = defaultdict(int)
    for c in m:
        for s in sector_of.get(c, []):
            origin[s] += 1
    origin_str = ", ".join(f"{s}×{n}" for s, n in sorted(origin.items(), key=lambda x: -x[1])[:4])
    print(f"\n  [클러스터 {l}] n={len(m)} 응집도={cluster_cohesion(m):.3f}")
    print(f"    원소속분포: {origin_str}")
    print(f"    종목: {', '.join(name(c) for c in m[:20])}" + (" ..." if len(m) > 20 else ""))

# ── [C] 다중소속 기준 스윕 ───────────────────────────────────
print("\n" + "=" * 64)
print("[C] 다중소속 후보 수 — 기준별 비교")
print("=" * 64)
all_secs = list({s for ss in sector_of.values() for s in ss})
codeset = set(codes)
sec_members = defaultdict(list)
for c in codes:
    for s in sector_of.get(c, []):
        sec_members[s].append(c)

def avg_to_sec(code, sec):
    mem = [m for m in sec_members[sec] if m in codeset and m != code]
    return corr.loc[code, mem].mean() if mem else np.nan

# 각 종목의 (본인섹터 제외) 섹터별 평균상관 미리 계산
sec_scores = {}
for c in codes:
    own = set(sector_of.get(c, []))
    row = {}
    for s in all_secs:
        v = avg_to_sec(c, s)
        if not np.isnan(v):
            row[s] = v
    sec_scores[c] = (own, row)

for label, absmin, ratio in [
    ("절대 0.45",       0.45, 0.0),
    ("절대 0.50",       0.50, 0.0),
    ("절대 0.55",       0.55, 0.0),
    ("best의 95%+0.50", 0.50, 0.95),
]:
    n_codes = 0
    n_slots = 0
    for c, (own, row) in sec_scores.items():
        ext = {s: v for s, v in row.items() if s not in own}
        if not ext:
            continue
        best = max(row.values())
        hit = [s for s, v in ext.items() if v >= absmin and v >= best * ratio]
        if hit:
            n_codes += 1
            n_slots += len(hit)
    print(f"  {label:<16}: 다중소속 종목 {n_codes:>3}개 / 추가슬롯 {n_slots:>4}개")

# ── [D] 반도체 계열 확대 ─────────────────────────────────────
print("\n" + "=" * 64)
print("[D] 반도체 계열 서브클러스터 (반도체_* 종목만 재클러스터링)")
print("=" * 64)
semi = [c for c in codes if any("반도체" in s for s in sector_of.get(c, []))]
print(f"  반도체 계열 종목 수: {len(semi)}")
sub = corr.loc[semi, semi]
Dsub = np.clip(1 - sub.values, 0, 2)
np.fill_diagonal(Dsub, 0)
Dsub = (Dsub + Dsub.T) / 2
Zsub = linkage(squareform(Dsub, checks=False), method="average")
for h in [0.45, 0.50, 0.55]:
    lab = fcluster(Zsub, t=h, criterion="distance")
    g = defaultdict(list)
    for c, l in zip(semi, lab):
        g[l].append(c)
    big = [len(m) for m in g.values() if len(m) >= 2]
    print(f"  height={h} (corr≈{1-h:.2f}): {len(g)}개 클러스터, 크기2+ {len(big)}개 {sorted(big, reverse=True)}")