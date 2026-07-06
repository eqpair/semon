"""
compare_outlier_criteria.py — '겉도는 종목' 판정 기준 비교

문제: 절대 0.20 커트라인은 잔차 스케일·섹터별 응집도 차이를 무시 → 34% 걸림

세 가지 기준을 나란히 비교:
  A) 절대값: 섹터내상관 < 0.20 (현재)
  B) 상대값: 섹터내상관 < 섹터평균응집도 × RATIO
            (그 섹터 기준으로 유독 낮은 종목만 = 공정)
  C) 순위값: 섹터내상관이 '자기가 그 섹터의 최하위 X%'
  D) z-score: 섹터 내에서 표준편차 몇 배 아래인가

build_corr_neutral.py 실행 후:
    python3 compare_outlier_criteria.py
"""
import json
import numpy as np
import pandas as pd
from collections import defaultdict

corr = pd.read_parquet("corr_matrix.parquet")
meta = json.load(open("meta.json", encoding="utf-8"))
name_of = meta["name_of"]
sector_of = meta["sector_of"]
codes = list(corr.columns)
codeset = set(codes)
name = lambda c: name_of.get(c, c)

sec_members = defaultdict(list)
for c in codes:
    for s in sector_of.get(c, []):
        sec_members[s].append(c)


def avg_in_sec(code, sec):
    m = [x for x in sec_members[sec] if x in codeset and x != code]
    return corr.loc[code, m].mean() if m else np.nan


# 섹터별 멤버 상관 프로파일
sec_coh = {}
sec_member_avgs = {}
for s, members in sec_members.items():
    m = [x for x in members if x in codeset]
    if len(m) < 2:
        continue
    avgs = {c: avg_in_sec(c, s) for c in m}
    sec_member_avgs[s] = avgs
    sub = corr.loc[m, m].values
    iu = np.triu_indices(len(m), k=1)
    sec_coh[s] = float(np.nanmean(sub[iu]))


def count_outliers(method, **kw):
    flagged = []
    for s, avgs in sec_member_avgs.items():
        vals = np.array(list(avgs.values()))
        coh = sec_coh[s]
        for c, v in avgs.items():
            if np.isnan(v):
                continue
            if method == "A" and v < kw["cut"]:
                flagged.append((c, s, v, coh))
            elif method == "B" and v < coh * kw["ratio"]:
                flagged.append((c, s, v, coh))
            elif method == "C":
                pct = (vals < v).mean()  # 이 종목이 섹터 내 하위 몇%
                if pct <= kw["bottom"]:
                    flagged.append((c, s, v, coh))
            elif method == "D":
                mu, sd = vals.mean(), vals.std()
                if sd > 0 and (v - mu) / sd < -kw["z"]:
                    flagged.append((c, s, v, coh))
    return flagged


print("=" * 64)
print("겉도는 종목 판정 기준 비교 (총 종목 600)")
print("=" * 64)

configs = [
    ("A: 절대 0.20 (현재)", "A", {"cut": 0.20}),
    ("A: 절대 0.12", "A", {"cut": 0.12}),
    ("B: 섹터평균의 50% 미만", "B", {"ratio": 0.50}),
    ("B: 섹터평균의 40% 미만", "B", {"ratio": 0.40}),
    ("C: 섹터 내 하위 20%", "C", {"bottom": 0.20}),
    ("D: 섹터평균 -1.5σ 이하", "D", {"z": 1.5}),
    ("D: 섹터평균 -2.0σ 이하", "D", {"z": 2.0}),
]
results = {}
for label, m, kw in configs:
    fl = count_outliers(m, **kw)
    results[label] = fl
    print(f"  {label:<24}: {len(fl):>3}개 ({100*len(fl)/600:.1f}%)")

# 기준 B(섹터평균 50%)로 실제 리스트 — 가장 균형적일 가능성
print("\n" + "=" * 64)
print("[기준 B: 섹터평균의 50% 미만] 실제 플래그 종목")
print("  (그 섹터 기준으로 유독 낮은 = 진짜 오분류 의심)")
print("=" * 64)
flB = sorted(results["B: 섹터평균의 50% 미만"], key=lambda x: x[2] - x[3])
for c, s, v, coh in flB:
    print(f"  {name(c):<14} [{s}] 상관 {v:.3f} (섹터평균 {coh:.3f})")
print(f"\n  총 {len(flB)}개")

print("\n" + "=" * 64)
print("해석 가이드:")
print("  · A(절대)는 응집도 낮은 섹터 종목을 과다 플래그 (불공정)")
print("  · B(상대)는 '그 섹터 안에서 유독 뒤처진' 종목만 → 오분류 탐지에 적합")
print("  · 목적이 '오분류 찾기'면 B, '순수도 최대화'면 C 추천")