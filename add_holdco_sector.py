"""
add_holdco_sector.py
────────────────────
config.py에 지주·복합기업 섹터 추가
"""

import shutil
from pathlib import Path
from datetime import datetime

HOLDCO_ENTRY = """
    # ─────────────────────────────────────────
    # 지주·복합기업
    # ─────────────────────────────────────────

    "지주·복합기업": [
        ("402340", "SK스퀘어"),
        ("028260", "삼성물산"),
        ("001040", "CJ"),
        ("000880", "한화"),
        ("004990", "롯데지주"),
    ],

"""

config_path = Path("config.py")

# 백업
bak = config_path.with_suffix(f".py.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
shutil.copy2(config_path, bak)
print(f"백업: {bak}")

src = config_path.read_text(encoding="utf-8")

# }\n\nWAIT_TIME 패턴 앞에 삽입 (SECTORS 딕셔너리 닫히는 } 바로 앞)
target = "}\n\nWAIT_TIME = 300"
assert target in src, "삽입 위치를 찾을 수 없습니다"
src = src.replace(target, HOLDCO_ENTRY + "}\n\nWAIT_TIME = 300")

config_path.write_text(src, encoding="utf-8")
print("지주·복합기업 섹터 추가 완료")

# 검증
import sys
if "config" in sys.modules:
    del sys.modules["config"]
from config import SECTORS
total = sum(len(v) for v in SECTORS.values())
print(f"검증: {len(SECTORS)}개 섹터, {total}개 종목")
print(f"지주·복합기업: {[n for _,n in SECTORS.get('지주·복합기업', [])]}")