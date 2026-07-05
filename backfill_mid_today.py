#!/usr/bin/env python3
"""
backfill_mid_today.py — 이미 발송된 MID 알림을 mid_log.json에 소급 등록 (1회성)

radar_sent.json의 "{날짜}:MID:{코드}" 키를 읽어 mid_log.json 엔트리 생성.
알림 시점 가격은 기록이 없으므로 null — 채점 기준가(entry_close)는
mid_track.py가 해당일 공식 종가로 채움.

실행:
    ~/semon/venv/bin/python3 backfill_mid_today.py
"""
import json
from pathlib import Path

SENT_PATH = Path("/home/ubuntu/semon/data/radar_sent.json")
SIG_PATH = Path("/home/ubuntu/semon/docs/data/signals.json")
LOG_PATH = Path("/home/ubuntu/semon/docs/data/mid_log.json")


def main():
    sent = json.loads(SENT_PATH.read_text(encoding="utf-8"))
    sig = json.loads(SIG_PATH.read_text(encoding="utf-8"))

    # code → (name, sector, sq, rs...) 룩업 (복수 섹터 시 첫 매칭)
    lookup = {}
    for sec_name, sd in sig["sectors"].items():
        sq = (sig.get("sector_rrg", {}).get(sec_name) or {}).get("quadrant", "")
        for s in sd.get("candidates", []):
            lookup.setdefault(s["code"], (sec_name, sq, s))

    try:
        log = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    except Exception:
        log = []
    seen = {(e.get("signal_date"), e.get("code")) for e in log}

    added = 0
    for key, date_val in sent.items():
        parts = key.split(":")
        # 키 형식: "YYYY-MM-DD:MID:code"
        if len(parts) != 3 or parts[1] != "MID":
            continue
        sig_date, _, code = parts
        if (sig_date, code) in seen:
            continue
        sec_name, sq, s = lookup.get(code, (None, None, {}))
        log.append({
            "signal_date":     sig_date,
            "code":            code,
            "name":            s.get("name", code),
            "sector":          sec_name,
            "sector_quadrant": sq,
            "price_at_signal": None,   # 소급분: 알림 시점 가격 기록 없음
            "rs_ratio":        s.get("rs_ratio"),
            "rs_momentum":     s.get("rs_momentum"),
            "value":           s.get("value"),
            "status":          "open",
            "entry_close":     None,
        })
        seen.add((sig_date, code))
        added += 1
        print(f"  + {sig_date} {s.get('name', code)}({code}) [{sec_name}]")

    log.sort(key=lambda e: (e["signal_date"], e["code"]))
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text(json.dumps(log, ensure_ascii=False), encoding="utf-8")
    print(f"\n등록 {added}건, 총 {len(log)}건 → {LOG_PATH}")


if __name__ == "__main__":
    main()