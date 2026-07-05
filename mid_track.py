#!/usr/bin/env python3
"""
mid_track.py — MID 시그널 일일 채점 (백테스트 label_outcomes.py와 동일 규칙)

규칙:
    진입가 = 시그널일 종가 (entry_close, 네이버 fchart 공식 종가로 확정)
    이후 매 거래일: 저가 ≤ 진입가×0.93 → LOSS (-7%)   ※ TP 동시 터치 시 LOSS 우선
                   고가 ≥ 진입가×1.10 → WIN (+10%)
    30거래일 경과 → TIMEOUT (해당일 종가 수익률)
    미확정 → open (현재수익/MAX/MIN/보유일 갱신)

실행: 장 마감 후 1회 (systemd timer 권장, 15:50 KST)
    ~/semon/venv/bin/python3 mid_track.py
"""
import json
import re
import time
import urllib.request
import subprocess
from datetime import datetime
from pathlib import Path

LOG_PATH = Path("/home/ubuntu/semon/docs/data/mid_log.json")
TP, SL, HOLD_DAYS = 0.10, 0.07, 30
FCHART = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=80&requestType=0"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"

_cache: dict[str, list] = {}


def fetch_daily(code: str) -> list:
    """[(YYYY-MM-DD, high, low, close), ...] 오래된 순"""
    if code in _cache:
        return _cache[code]
    req = urllib.request.Request(FCHART.format(code=code), headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=10) as r:
        xml = r.read().decode("euc-kr", errors="replace")
    rows = []
    for m in re.finditer(r'<item data="([^"]+)"', xml):
        p = m.group(1).split("|")
        if len(p) == 6:
            d = f"{p[0][:4]}-{p[0][4:6]}-{p[0][6:]}"
            rows.append((d, float(p[2]), float(p[3]), float(p[4])))
    _cache[code] = rows
    time.sleep(0.25)
    return rows


def score(entry: dict) -> bool:
    """엔트리 채점. 변경 있으면 True"""
    rows = fetch_daily(entry["code"])
    dates = [r[0] for r in rows]
    if entry["signal_date"] not in dates:
        return False  # 시그널일 봉 아직 없음/휴장 — 다음 실행에서 재시도
    i0 = dates.index(entry["signal_date"])
    entry_close = rows[i0][3]

    changed = entry.get("entry_close") != entry_close
    entry["entry_close"] = entry_close
    tp_px, sl_px = entry_close * (1 + TP), entry_close * (1 - SL)

    max_ret, min_ret = 0.0, 0.0
    for k in range(i0 + 1, len(rows)):
        d, hi, lo, cl = rows[k]
        held = k - i0
        max_ret = max(max_ret, hi / entry_close - 1)
        min_ret = min(min_ret, lo / entry_close - 1)
        if lo <= sl_px:                     # 동시 터치 포함 stop-first
            entry.update(status="loss", ret_exit=round(-SL * 100, 2), exit_date=d)
            entry.update(days_held=held, max_ret=round(max_ret * 100, 2),
                         min_ret=round(min_ret * 100, 2))
            return True
        if hi >= tp_px:
            entry.update(status="win", ret_exit=round(TP * 100, 2), exit_date=d)
            entry.update(days_held=held, max_ret=round(max_ret * 100, 2),
                         min_ret=round(min_ret * 100, 2))
            return True
        if held >= HOLD_DAYS:
            entry.update(status="timeout", ret_exit=round((cl / entry_close - 1) * 100, 2),
                         exit_date=d, days_held=held,
                         max_ret=round(max_ret * 100, 2), min_ret=round(min_ret * 100, 2))
            return True

    # 미확정 (open) — 현재 상태 갱신
    last_close = rows[-1][3]
    entry.update(status="open",
                 ret_current=round((last_close / entry_close - 1) * 100, 2),
                 days_held=len(rows) - 1 - i0,
                 max_ret=round(max_ret * 100, 2), min_ret=round(min_ret * 100, 2))
    return True


def main():
    if not LOG_PATH.exists():
        print("mid_log.json 없음 — 채점 대상 없음")
        return
    log = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    n_scored = n_fail = 0
    for e in log:
        if e.get("status") in ("win", "loss", "timeout"):
            continue  # 확정건은 재채점 불필요
        try:
            if score(e):
                n_scored += 1
        except Exception as ex:
            n_fail += 1
            print(f"  [FAIL] {e.get('code')}: {ex}")

    meta_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for e in log:
        e["updated_at"] = meta_ts
    LOG_PATH.write_text(json.dumps(log, ensure_ascii=False), encoding="utf-8")

    # GitHub Pages 배송 (signals.json과 동일한 git 경로)
    try:
        repo = "/home/ubuntu/semon"
        subprocess.run(["git", "-C", repo, "add", "docs/data/mid_log.json"], check=True)
        r = subprocess.run(["git", "-C", repo, "commit", "-m",
                            f"mid_log update {meta_ts[:10]}"], capture_output=True, text=True)
        if r.returncode == 0:
            subprocess.run(["git", "-C", repo, "pull", "--rebase", "--autostash"], check=True)
            subprocess.run(["git", "-C", repo, "push", "origin", "main"], check=True)
            print("git push 완료")
        else:
            print("변경 없음 — push 생략")
    except Exception as ex:
        print(f"git push 실패 (채점 데이터는 저장됨): {ex}")

    # 요약
    done = [e for e in log if e.get("status") in ("win", "loss", "timeout")]
    wins = sum(1 for e in done if e["status"] == "win")
    opens = sum(1 for e in log if e.get("status") == "open")
    print(f"채점 {n_scored}건 (실패 {n_fail}) | 진행중 {opens} | "
          f"확정 {len(done)} (WIN {wins}"
          + (f", 적중률 {wins/len(done):.1%} vs 벤치마크 52.3%" if done else "")
          + ")")


if __name__ == "__main__":
    main()