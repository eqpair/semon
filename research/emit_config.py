"""
emit_config.py — analyze_sectors 결과를 받아 다중소속 반영 config 생성

사용 흐름:
  1) analyze_sectors.py 출력에서 채택할 다중소속을 ADD 에 적는다
     (또는 multi_candidate 자동 채택 모드 사용)
  2) python3 emit_config.py  →  config_new.py 생성
  3) 검토 후 config.py 로 교체

ADD 형식: { "종목코드": ["추가할섹터1", "추가할섹터2"] }
  - 기존 원소속은 유지되고, 여기 적은 섹터에 '추가로' 들어감
  - 같은 (code,name) 튜플이 여러 섹터에 중복 등장 → 다중소속

MOVE 형식: { "종목코드": "옮길섹터" }  (오배치 교정: 원소속 제거 후 이동)
"""
import json
import config

# ── 검증 결과 기반으로 채워넣기 ──────────────────────────────
ADD = {
    # "017670": ["우주·위성"],   # 예: SK텔레콤을 통신+우주위성 양쪽에
}
MOVE = {
    # "092300": "반도체_장비",   # 예: 현우산업 소재부품→장비 교정
}
# ───────────────────────────────────────────────────────────

name_of = {}
for sec, lst in config.SECTORS.items():
    for code, name in lst:
        name_of[code] = name

new = {sec: list(lst) for sec, lst in config.SECTORS.items()}

# MOVE 먼저 (원소속 제거)
for code, dst in MOVE.items():
    name = name_of[code]
    for sec in list(new):
        new[sec] = [(c, n) for (c, n) in new[sec] if c != code]
    new[dst].append((code, name))

# ADD (중복 소속 추가)
for code, secs in ADD.items():
    name = name_of[code]
    for dst in secs:
        if not any(c == code for (c, n) in new[dst]):
            new[dst].append((code, name))

# 직렬화
lines = ["SECTORS = {\n"]
for sec, lst in new.items():
    lines.append(f'    "{sec}": [\n')
    for code, name in lst:
        lines.append(f'        ("{code}", "{name}"),\n')
    lines.append("    ],\n")
lines.append("}\n\n")
lines.append("WAIT_TIME = 5\n")
lines.append("MARKET_OPEN = (9, 0)\n")
lines.append("MARKET_CLOSE = (15, 30)\n")

with open("config_new.py", "w", encoding="utf-8") as f:
    f.writelines(lines)

# 요약
slots = sum(len(v) for v in new.values())
uniq = len({c for v in new.values() for (c, n) in v})
print(f"config_new.py 생성: {len(new)}섹터 / {slots}슬롯 / {uniq}유니크 종목")
print(f"다중소속으로 늘어난 슬롯: {slots - uniq}개")