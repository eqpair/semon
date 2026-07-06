"""
analyze_sectors.py — 상관행렬로 (1) 현행 섹터 검증 (2) 다중소속 후보 추출

build_corr.py 실행 후:
    python3 analyze_sectors.py

핵심 지표
  - sector_cohesion : 각 섹터 내부 평균 상관 (높을수록 동질)
  - misplaced       : 본인섹터 평균상관 < 외부 best섹터 평균상관 인 종목 (오배치 의심)
  - multi_candidate : best 섹터 외에, best의 RATIO 이상으로 잘 맞는 추가 섹터

임계값(THRESH, RATIO)은 출력되는 분포를 보고 조정하세요.
"""
import json
import numpy as np
import pandas as pd

# ── 튜닝 파라미터 (분포 보고 조정) ───────────────────────────
RATIO = 0.90      # best 섹터 평균상관 대비 이 비율 이상이면 다중소속 후보
MIN_ABS = 0.45    # 그래도 절대 상관이 이보다 낮으면 후보에서 제외
# ───────────────────────────────────────────────────────────

corr = pd.read_parquet("corr_matrix.parquet")
meta = json.load(open("meta.json", encoding="utf-8"))
name_of = meta["name_of"]
sector_of = meta["sector_of"]   # code -> [원소속섹터,...]

codes = list(corr.columns)
codeset = set(codes)

# 섹터 -> 실제로 데이터가 있는 종목들
from collections import defaultdict
sec_members = defaultdict(list)
for code in codes:
    for sec in sector_of.get(code, []):
        sec_members[sec].append(code)


def avg_corr_to_sector(code, sec, exclude_self=True):
    """code와 sec 소속 종목들 간 평균 상관 (자기 자신 제외)"""
    members = [m for m in sec_members[sec] if m in codeset]
    if exclude_self:
        members = [m for m in members if m != code]
    if not members:
        return np.nan
    return corr.loc[code, members].mean()


# ── (1) 섹터 응집도 ──────────────────────────────────────────
print("=" * 60)
print("[1] 섹터 응집도 (내부 평균 상관, 낮은 순)")
print("=" * 60)
cohesion = {}
for sec, members in sec_members.items():
    if len(members) < 2:
        cohesion[sec] = np.nan
        continue
    sub = corr.loc[members, members].values
    iu = np.triu_indices(len(members), k=1)
    cohesion[sec] = float(np.nanmean(sub[iu]))

for sec in sorted(cohesion, key=lambda s: (np.isnan(cohesion[s]), cohesion[s])):
    print(f"  {cohesion[sec]:.3f}  {sec}  (n={len(sec_members[sec])})")

# ── (2) 오배치 의심 종목 ─────────────────────────────────────
print("\n" + "=" * 60)
print("[2] 오배치 의심: 본인섹터 < 외부 best섹터")
print("=" * 60)
all_secs = list(sec_members.keys())
misplaced = []
multi_rows = []

for code in codes:
    own = sector_of.get(code, [])
    # 각 섹터에 대한 평균상관
    scores = {}
    for sec in all_secs:
        # 본인 소속 섹터면 self 제외하고 계산
        scores[sec] = avg_corr_to_sector(code, sec, exclude_self=(sec in own))
    ranked = sorted(
        ((s, v) for s, v in scores.items() if not np.isnan(v)),
        key=lambda x: -x[1],
    )
    if not ranked:
        continue
    best_sec, best_val = ranked[0]

    own_best = max((scores[s] for s in own if not np.isnan(scores.get(s, np.nan))),
                   default=np.nan)

    if best_sec not in own and best_val > (own_best if not np.isnan(own_best) else -1) + 0.05:
        misplaced.append((code, own, own_best, best_sec, best_val))

    # 다중소속 후보: best 대비 RATIO 이상 & 절대값 충족, 본인 소속 아닌 섹터
    for s, v in ranked:
        if s in own:
            continue
        if v >= best_val * RATIO and v >= MIN_ABS:
            multi_rows.append((code, s, v, best_val))

for code, own, own_best, best_sec, best_val in sorted(misplaced, key=lambda x: -(x[4] - (x[2] if not np.isnan(x[2]) else 0)))[:40]:
    ob = f"{own_best:.3f}" if not np.isnan(own_best) else " NA "
    print(f"  {name_of[code]:<12} 원소속={','.join(own):<14} ({ob}) → {best_sec} ({best_val:.3f})")
print(f"\n  총 오배치 의심: {len(misplaced)}개")

# ── (3) 다중소속 후보 ────────────────────────────────────────
print("\n" + "=" * 60)
print(f"[3] 다중소속 후보 (best의 {RATIO:.0%} 이상, 절대 {MIN_ABS} 이상)")
print("=" * 60)
by_code = defaultdict(list)
for code, sec, v, best in multi_rows:
    by_code[code].append((sec, v))
for code in sorted(by_code, key=lambda c: -len(by_code[c]))[:50]:
    extra = ", ".join(f"{s}({v:.2f})" for s, v in sorted(by_code[code], key=lambda x: -x[1]))
    print(f"  {name_of[code]:<12} [{','.join(sector_of.get(code,[]))}] + {extra}")
print(f"\n  다중소속 후보 종목: {len(by_code)}개")

# ── (4) 상관 분포 (임계값 결정 근거) ─────────────────────────
print("\n" + "=" * 60)
print("[4] 전체 상관 분포 (임계값 결정 참고)")
print("=" * 60)
iu = np.triu_indices(len(codes), k=1)
vals = corr.values[iu]
vals = vals[~np.isnan(vals)]
for q in [50, 75, 90, 95, 99]:
    print(f"  {q}분위: {np.percentile(vals, q):.3f}")
print(f"  평균: {vals.mean():.3f}  최대: {vals.max():.3f}")