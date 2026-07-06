"""
finalize_sectors.py — 오분류 종목 처리 + 밸류체인 다중소속 반영 → 최종 config

전제: config.py=의미분류, build_corr_neutral.py 실행 완료

로직:
  1) 기준 B(섹터평균 50% 미만)로 '겉도는 종목' 추출
  2) 각 겉도는 종목에 대해 '더 잘 맞는 다른 섹터' 탐색:
     - 다른 섹터 평균상관이 MOVE_GAP 이상 높으면 → 이동(MOVE)
     - 아니면(갈 곳 없음) → 미분류(UNCLASSIFY)
     - 단, KEEP_SECTORS 소속은 맥락상 유지(반도체_소자 등 사용자 지정)
  3) [4] 밸류체인 다중소속(MULTI_ABS 이상) 자동 반영
  4) 최종 config_final.py 생성 + 처리내역 리포트

실행:
    python3 finalize_sectors.py
"""
import json
import numpy as np
import pandas as pd
from collections import defaultdict

# ── 파라미터 ────────────────────────────────────────────────
OUTLIER_RATIO = 0.50   # 섹터평균의 이 비율 미만이면 겉도는 종목 (기준 B)
MOVE_GAP      = 0.10   # 다른 섹터가 자기섹터보다 이만큼 높으면 이동
MULTI_ABS     = 0.35   # 밸류체인 다중소속 하한
MIN_HOME      = 0.30   # 이동하려면 목적섹터 상관이 최소 이 값 이상 (갈 곳 있음 판정)
# 맥락상 유지할 섹터 (데이터상 안 뭉쳐도 의미적으로 묶음)
KEEP_SECTORS  = {"반도체_소자", "지주"}
# 사업상 어느 순수섹터에도 안 맞아 강제 미분류 (사람 판단)
FORCE_UNCLASSIFY = {"047050"}  # 포스코인터내셔널: 상사+에너지, 순수섹터 없음
# ───────────────────────────────────────────────────────────

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
all_secs = list(sec_members.keys())


def avg_to_sec(code, sec, exclude_self=True):
    m = [x for x in sec_members[sec] if x in codeset]
    if exclude_self:
        m = [x for x in m if x != code]
    return corr.loc[code, m].mean() if m else np.nan


sec_coh = {}
for s, members in sec_members.items():
    m = [x for x in members if x in codeset]
    if len(m) >= 2:
        sub = corr.loc[m, m].values
        iu = np.triu_indices(len(m), k=1)
        sec_coh[s] = float(np.nanmean(sub[iu]))

# ── 1) 겉도는 종목 + 처리 판정 ───────────────────────────────
MOVE = {}         # code -> 새 섹터
UNCLASSIFY = []   # code (갈 곳 없음)
KEEP = []         # code (맥락 유지)

for c in codes:
    home = sector_of.get(c, [None])[0]
    if not home or home not in sec_coh:
        continue
    if c in FORCE_UNCLASSIFY:            # 사람이 지정한 강제 미분류
        UNCLASSIFY.append((c, home, avg_to_sec(c, home)))
        continue
    home_v = avg_to_sec(c, home)
    if np.isnan(home_v) or home_v >= sec_coh[home] * OUTLIER_RATIO:
        continue  # 겉돌지 않음

    # 겉도는 종목 → 처리 결정
    if home in KEEP_SECTORS:
        KEEP.append((c, home, home_v))
        continue

    # 더 잘 맞는 섹터 탐색
    best_s, best_v = None, -1
    for s in all_secs:
        if s in sector_of.get(c, []) or s == "종합상사":
            continue
        v = avg_to_sec(c, s, exclude_self=False)
        if not np.isnan(v) and v > best_v:
            best_s, best_v = s, v

    if best_s and best_v >= max(MIN_HOME, home_v + MOVE_GAP):
        MOVE[c] = (home, best_s, home_v, best_v)
    else:
        UNCLASSIFY.append((c, home, home_v))

# ── 2) 밸류체인 다중소속 ─────────────────────────────────────
MULTI = defaultdict(list)
for c in codes:
    if c in MOVE or c in [x[0] for x in UNCLASSIFY]:
        continue
    own = set(sector_of.get(c, []))
    home = sector_of.get(c, [None])[0]
    home_v = avg_to_sec(c, home) if home else np.nan
    if np.isnan(home_v) or home_v < 0.25:  # 자기섹터 유지 조건
        continue
    for s in all_secs:
        if s in own:
            continue
        if s == "종합상사":  # 매크로 그림자 → 사업분류 아님, 제외
            continue
        v = avg_to_sec(c, s, exclude_self=False)
        if not np.isnan(v) and v >= MULTI_ABS:
            MULTI[c].append((s, v))

# ── 리포트 ───────────────────────────────────────────────────
L = []
L.append("=" * 60)
L.append(f"[이동 MOVE] {len(MOVE)}개 — 더 맞는 섹터로")
L.append("=" * 60)
for c, (h, b, hv, bv) in sorted(MOVE.items(), key=lambda x: -(x[1][3]-x[1][2])):
    L.append(f"  {name(c):<14} [{h}]{hv:.2f} → [{b}]{bv:.2f}")

L.append("\n" + "=" * 60)
L.append(f"[미분류 UNCLASSIFY] {len(UNCLASSIFY)}개 — 갈 곳 없음(고유 움직임)")
L.append("=" * 60)
for c, h, hv in sorted(UNCLASSIFY, key=lambda x: x[2]):
    L.append(f"  {name(c):<14} [{h}]에서 이탈 (상관 {hv:.2f})")

L.append("\n" + "=" * 60)
L.append(f"[유지 KEEP] {len(KEEP)}개 — 맥락상 유지({', '.join(KEEP_SECTORS)})")
L.append("=" * 60)
for c, h, hv in KEEP:
    L.append(f"  {name(c):<14} [{h}] (상관 {hv:.2f})")

L.append("\n" + "=" * 60)
L.append(f"[다중소속 MULTI] {len(MULTI)}개")
L.append("=" * 60)
for c in sorted(MULTI, key=lambda x: -max(v for _, v in MULTI[x])):
    home = sector_of.get(c, ["?"])[0]
    extra = ", ".join(f"{s}({v:.2f})" for s, v in sorted(MULTI[c], key=lambda x: -x[1]))
    L.append(f"  {name(c):<14} [{home}] + {extra}")

report = "\n".join(L)
open("finalize_report.txt", "w", encoding="utf-8").write(report)
print(report[:4000])
print("\n... (전체는 finalize_report.txt)")

# ── 최종 config 조립 ─────────────────────────────────────────
final = defaultdict(list)
unclassified_codes = {x[0] for x in UNCLASSIFY}
for c in codes:
    if c in unclassified_codes:
        continue
    home = MOVE[c][1] if c in MOVE else sector_of.get(c, [None])[0]
    if home:
        final[home].append(c)
# 다중소속 추가
for c, extras in MULTI.items():
    for s, v in extras:
        if c not in final[s]:
            final[s].append(c)

out = ["SECTORS = {\n"]
for s, clist in final.items():
    out.append(f'    "{s}": [\n')
    for c in clist:
        out.append(f'        ("{c}", "{name(c)}"),\n')
    out.append("    ],\n")
out.append("}\n\n")
if unclassified_codes:
    out.append("# 미분류 (고유 움직임 — 섹터 신호 대상 제외)\n")
    out.append("UNCLASSIFIED = [\n")
    for c in sorted(unclassified_codes):
        out.append(f'    ("{c}", "{name(c)}"),\n')
    out.append("]\n\n")
out.append("WAIT_TIME = 5\nMARKET_OPEN = (9, 0)\nMARKET_CLOSE = (15, 30)\n")
open("config_final.py", "w", encoding="utf-8").writelines(out)

slots = sum(len(v) for v in final.values())
uniq = len({c for v in final.values() for c in v})
print(f"\nconfig_final.py: {len(final)}섹터 / {slots}슬롯 / {uniq}유니크 / "
      f"다중분={slots-uniq} / 미분류={len(unclassified_codes)}")