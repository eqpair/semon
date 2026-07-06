"""
fine_sectors.py — 응집도 높은 소그룹 자동 추출 + 다중소속 + config emit

목적: "같은 섹터인데 이것만 아직 안 갔다" 신호가 살아나도록
      크고 느슨한 덩어리 대신 작고 단단한 미세섹터로 재편성.

build_corr.py 실행 후:
    python3 fine_sectors.py

산출:
    config_fine.py   — 재편성된 SECTORS (다중소속 반영)
    fine_report.txt  — 소그룹 구성 + 미배정 종목 리스트

방식:
  1) 계층적 클러스터링을 TIGHT_CUT(조인 컷)으로 잘라 소그룹 추출
  2) 크기 MIN_SIZE~MAX_SIZE 이고 응집도 MIN_COH 이상인 그룹만 채택
  3) 그룹 이름 = 구성원 원소속 최빈값 (동률이면 병기)
  4) 다중소속: 각 종목이 자기 그룹 외에 MULTI_ABS 이상 평균상관 갖는
     다른 그룹에도 추가 소속
"""
import json
import numpy as np
import pandas as pd
from collections import defaultdict, Counter
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

# ── 튜닝 (잘게 나누는 방향) ─────────────────────────────────
TIGHT_CUT = 0.70     # distance 컷 (corr≈0.55). 작을수록 더 잘게
MIN_COH   = 0.35     # 소그룹 최소 내부 응집도
MIN_SIZE  = 2        # 최소 종목 수
MAX_SIZE  = 15       # 이보다 크면 재분할 대상(경고)
MULTI_ABS = 0.30     # 다중소속: 다른 그룹 평균상관 이 값 이상이면 추가
# ───────────────────────────────────────────────────────────

corr = pd.read_parquet("corr_matrix.parquet")
meta = json.load(open("meta.json", encoding="utf-8"))
name_of = meta["name_of"]
sector_of = meta["sector_of"]
codes = list(corr.columns)
name = lambda c: name_of.get(c, c)

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

labels = fcluster(Z, t=TIGHT_CUT, criterion="distance")
raw_groups = defaultdict(list)
for c, l in zip(codes, labels):
    raw_groups[l].append(c)

# 채택 그룹 선별 + 이름 부여
def group_name(members):
    cnt = Counter()
    for c in members:
        for s in sector_of.get(c, []):
            cnt[s] += 1
    top = cnt.most_common()
    if not top:
        return "기타"
    maxn = top[0][1]
    tied = [s for s, n in top if n == maxn]
    base = "·".join(tied[:2])
    return base

accepted = []      # (name, members, coh)
unassigned = []    # 소그룹에 못 든 종목
for l, members in raw_groups.items():
    coh = cohesion(members)
    if len(members) < MIN_SIZE or (not np.isnan(coh) and coh < MIN_COH):
        unassigned.extend(members)
        continue
    accepted.append([group_name(members), members, coh])

# 이름 충돌 시 번호 부여 (members 확정 후 이름만 변경)
name_count = Counter(g[0] for g in accepted)
seen = defaultdict(int)
for g in accepted:
    if name_count[g[0]] > 1:
        seen[g[0]] += 1
        g[0] = f"{g[0]}_{seen[g[0]]}"

# 그룹 멤버 인덱스 (이름 확정 후 생성 → 어긋남 없음)
group_members = {g[0]: set(g[1]) for g in accepted}
assert len(group_members) == len(accepted), "그룹 이름 중복 잔존"

def avg_to_group(code, members):
    mem = [m for m in members if m != code]
    return corr.loc[code, mem].mean() if mem else np.nan

# 다중소속 부여
multi = defaultdict(set)   # gname -> extra codes
for code in codes:
    home = {g[0] for g in accepted if code in group_members[g[0]]}
    for gname, mem in group_members.items():
        if gname in home:
            continue
        if avg_to_group(code, mem) >= MULTI_ABS:
            multi[gname].add(code)

# 최종 섹터 조립
final = {}
for gname, members, coh in accepted:
    allc = list(members) + [c for c in multi[gname] if c not in set(members)]
    final[gname] = [(c, name(c)) for c in allc]

# ── 리포트 ───────────────────────────────────────────────────
lines = []
lines.append(f"채택 소그룹: {len(accepted)}개 / 미배정: {len(set(unassigned))}종목")
lines.append(f"파라미터: TIGHT_CUT={TIGHT_CUT}(corr≈{1-TIGHT_CUT:.2f}) MIN_COH={MIN_COH} MULTI_ABS={MULTI_ABS}")
lines.append("=" * 64)
for gname, members, coh in sorted(accepted, key=lambda x: -len(x[1])):
    extra = [c for c in multi[gname] if c not in set(members)]
    warn = "  ⚠️크기초과" if len(members) > MAX_SIZE else ""
    lines.append(f"\n[{gname}] n={len(members)} 응집도={coh:.3f}{warn}")
    lines.append(f"  코어: {', '.join(name(c) for c in members)}")
    if extra:
        lines.append(f"  +다중: {', '.join(name(c) for c in extra)}")

lines.append("\n" + "=" * 64)
lines.append(f"미배정 종목 {len(set(unassigned))}개 (단독/저응집 — 수동 검토 필요):")
lines.append("  " + ", ".join(name(c) for c in sorted(set(unassigned))))

report = "\n".join(lines)
with open("fine_report.txt", "w", encoding="utf-8") as f:
    f.write(report)
print(report[:3000])
print("\n... (전체는 fine_report.txt)")

# ── config emit ──────────────────────────────────────────────
out = ["SECTORS = {\n"]
for gname, lst in final.items():
    safe = gname.replace('"', "'")
    out.append(f'    "{safe}": [\n')
    for code, nm in lst:
        out.append(f'        ("{code}", "{nm}"),\n')
    out.append("    ],\n")
out.append("}\n\nWAIT_TIME = 5\nMARKET_OPEN = (9, 0)\nMARKET_CLOSE = (15, 30)\n")
with open("config_fine.py", "w", encoding="utf-8") as f:
    f.writelines(out)

slots = sum(len(v) for v in final.values())
uniq = len({c for v in final.values() for (c, n) in v})
print(f"\nconfig_fine.py: {len(final)}섹터 / {slots}슬롯 / {uniq}유니크 / 다중분(슬롯-유니크)={slots-uniq}")