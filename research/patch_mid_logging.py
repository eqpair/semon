#!/usr/bin/env python3
"""
patch_mid_logging.py — MID radar 알림 종목을 mid_log.json에 기록

적용 내용 (radar.py):
  - MID_LOG_PATH 상수 + _mid_log_append() 함수 추가
  - 알림 전송 성공 시 mid_log.json에 종목 기록 (알림 시점 가격 포함)

실행:
    cd ~/semon
    ~/semon/venv/bin/python3 patch_mid_logging.py
"""
import shutil
import sys
from datetime import datetime
from pathlib import Path

RADAR = Path.home() / "semon" / "radar.py"
TS = datetime.now().strftime("%Y%m%d_%H%M%S")

A1 = 'MID_EXCLUDE_SECTOR_KEYWORDS = ("바이오", "제약")\n'
N1 = A1 + r'''MID_LOG_PATH = "/home/ubuntu/semon/docs/data/mid_log.json"


def _mid_log_append(hits: list, today: str) -> None:
    """MID 알림 종목을 mid_log.json에 기록. 진입 채점은 mid_track.py가 수행."""
    try:
        try:
            with open(MID_LOG_PATH, encoding="utf-8") as f:
                log = json.load(f)
        except Exception:
            log = []
        seen = {(e.get("signal_date"), e.get("code")) for e in log}
        for sec_name, sq, s in hits:
            if (today, s.get("code")) in seen:
                continue
            log.append({
                "signal_date":     today,
                "code":            s.get("code"),
                "name":            s.get("name"),
                "sector":          sec_name,
                "sector_quadrant": sq,
                "price_at_signal": s.get("price"),
                "rs_ratio":        s.get("rs_ratio"),
                "rs_momentum":     s.get("rs_momentum"),
                "value":           s.get("value"),
                "status":          "open",
                "entry_close":     None,
            })
        Path(MID_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(MID_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"mid_log 기록 실패: {e}")


'''

A2 = '        logger.info(f"MID radar 알림: {len(hits)}개 종목")\n'
N2 = A2 + '        _mid_log_append(hits, today)\n'


def main():
    s = RADAR.read_text(encoding="utf-8")
    for a in (A1, A2):
        n = s.count(a)
        assert n == 1, f"[ABORT] anchor {n}회 매칭:\n{a[:100]}"
    shutil.copy2(RADAR, f"{RADAR}.bak_{TS}")
    s = s.replace(A1, N1, 1).replace(A2, N2, 1)
    RADAR.write_text(s, encoding="utf-8")
    print(f"[OK] radar.py 패치 완료 (백업: radar.py.bak_{TS})")
    print("다음: py_compile 확인 후 semon 재시작")


if __name__ == "__main__":
    main()