"""
score_sectors.py — 의미기반 분류를 잔차상관으로 채점 (경계 재설정 X, 채점만)

전제: config.py = config_semantic.py (의미기반 뼈대)
      build_corr_neutral.py 실행 완료 (잔차 corr_matrix.parquet)

실행:
    python3 score_sectors.py

출력:
  [1] 섹터 응집도 랭킹 — 내 분류가 데이터와 얼마나 맞는지 (낮으면 재검토)
  [2] 겉도는 종목 — 자기섹터 평균상관이 유독 낮은 종목 (오분류 의심)
  [3] 이사 후보 — 자기섹터보다 다른섹터와 훨씬 잘 맞는 종목 (이동 플래그)
  [4] 다중소속 후보 — 자기섹터 유지하되 다른섹터와도 강하게 동조

이 리포트는 '플래그'일 뿐. 자동 이동 안 함. 사람이 보고 판단.
"""
import json
import numpy as np
import pandas as pd
from collections import defaultdict

corr = pd.read_parquet("corr_matrix.parquet")
meta = json.load(open("meta.json", encoding="utf-8"))
name_of = meta["name_of"]
sector_of = meta["sector_of"]   # code -> [주섹터, (다중...)]
codes = list(corr.columns)
codeset = set(codes)
name = lambda c: name_of.get(c, c)

# 섹터 -> 멤버 (데이터 있는 것만)
sec_members = defaultdict(list)
for c in codes:
    for s in sector_of.get(c, []):
        sec_members[s].append(c)
all_secs = list(sec_members.keys())


def cohesion(members):
    m = [x for x in members if x in codeset]
    if len(m) < 2:
        return np.nan
    sub = corr.loc[m, m].values
    iu = np.triu_indices(len(m), k=1)
    return float(np.nanmean(sub[iu]))


def avg_to_sec(code, sec, exclude_self=True):
    m = [x for x in sec_members[sec] if x in codeset]
    if exclude_self:
        m = [x for x in m if x != code]
    return corr.loc[code, m].mean() if m else np.nan


def member_avg(code):
    """자기 주섹터 내 평균상관"""
    home = sector_of.get(code, [None])[0]
    return avg_to_sec(code, home) if home else np.nan


# ── [1] 섹터 응집도 랭킹 ──────────────────────────────────────
print("=" * 66)
print("[1] 섹터 응집도 (내 분류가 데이터와 맞는 정도, 낮은 순)")
print("=" * 66)
cohs = {s: cohesion(m) for s, m in sec_members.items()}
for s in sorted(cohs, key=lambda x: (np.isnan(cohs[x]), cohs[x])):
    n = len([c for c in sec_members[s] if c in codeset])
    bar = "█" * int(max(cohs[s], 0) * 30) if not np.isnan(cohs[s]) else ""
    print(f"  {cohs[s]:.3f} {bar:<15} {s} (n={n})")

mean_coh = np.nanmean(list(cohs.values()))
print(f"\n  전체 평균 응집도: {mean_coh:.3f}")

# ── [2] 겉도는 종목 ──────────────────────────────────────────
print("\n" + "=" * 66)
print("[2] 겉도는 종목 — 자기섹터 평균상관 < 0.20 (오분류 의심)")
print("=" * 66)
outliers = []
for c in codes:
    home = sector_of.get(c, [None])[0]
    if not home:
        continue
    ma = member_avg(c)
    if not np.isnan(ma) and ma < 0.20:
        outliers.append((c, home, ma))
for c, home, ma in sorted(outliers, key=lambda x: x[2]):
    print(f"  {name(c):<14} [{home}] 섹터내상관 {ma:.3f}")
print(f"\n  겉도는 종목: {len(outliers)}개")

# ── [3] 이사 후보 ────────────────────────────────────────────
print("\n" + "=" * 66)
print("[3] 이사 후보 — 자기섹터보다 다른섹터와 0.10+ 더 잘 맞음")
print("=" * 66)
movers = []
for c in codes:
    home = sector_of.get(c, [None])[0]
    if not home:
        continue
    home_v = avg_to_sec(c, home)
    best_s, best_v = None, -1
    for s in all_secs:
        if s in sector_of.get(c, []):
            continue
        v = avg_to_sec(c, s, exclude_self=False)
        if not np.isnan(v) and v > best_v:
            best_s, best_v = s, v
    if best_s and not np.isnan(home_v) and best_v > home_v + 0.10:
        movers.append((c, home, home_v, best_s, best_v))
for c, home, hv, bs, bv in sorted(movers, key=lambda x: -(x[4] - x[2])):
    print(f"  {name(c):<14} [{home}]{hv:.2f} → [{bs}]{bv:.2f}")
print(f"\n  이사 후보: {len(movers)}개")

# ── [4] 다중소속 후보 ────────────────────────────────────────
print("\n" + "=" * 66)
print("[4] 다중소속 후보 — 자기섹터 유지하되 다른섹터와도 0.35+ 동조")
print("=" * 66)
multi = defaultdict(list)
for c in codes:
    own = set(sector_of.get(c, []))
    home_v = member_avg(c)
    for s in all_secs:
        if s in own:
            continue
        v = avg_to_sec(c, s, exclude_self=False)
        # 다른섹터와 0.35+ 이고, 자기섹터와도 어느정도(0.25+) 유지
        if not np.isnan(v) and v >= 0.35 and not np.isnan(home_v) and home_v >= 0.25:
            multi[c].append((s, v))
for c in sorted(multi, key=lambda x: -max(v for _, v in multi[x])):
    home = sector_of.get(c, ["?"])[0]
    extra = ", ".join(f"{s}({v:.2f})" for s, v in sorted(multi[c], key=lambda x: -x[1]))
    print(f"  {name(c):<14} [{home}] + {extra}")
print(f"\n  다중소속 후보: {len(multi)}개")