#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Update macro prices only into docs/data/macro_live.json and push."""
import json
import os
import subprocess
from datetime import datetime, timezone, timedelta
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

KST = timezone(timedelta(hours=9))
REPO = os.path.dirname(os.path.abspath(__file__))
REL_PATH = os.path.join("docs", "data", "macro_live.json")
OUT_PATH = os.path.join(REPO, REL_PATH)


def in_market_window() -> bool:
    # Skip weekends; run 07-23 KST only
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    return 7 <= now.hour <= 20


def git_push():
    try:
        subprocess.run(["git", "-C", REPO, "add", REL_PATH], check=True)
        # nothing to commit?
        diff = subprocess.run(["git", "-C", REPO, "diff", "--cached", "--quiet"])
        if diff.returncode == 0:
            print("no change to push")
            return
        msg = "macro live " + datetime.now(KST).strftime("%H:%M")
        subprocess.run(["git", "-C", REPO, "commit", "-m", msg], check=True)
        subprocess.run(["git", "-C", REPO, "pull", "--rebase", "--autostash", "origin", "main"], check=False)
        subprocess.run(["git", "-C", REPO, "push", "origin", "main"], check=True)
        print("pushed")
    except Exception as e:
        print("git push failed:", e)


def main():
    if not in_market_window():
        print("outside market window - skip")
        return
    from eqai_news import fetch_macro
    macro = fetch_macro()
    if not macro:
        print("macro data empty - skip")
        return
    # 대량 실패 방어: 절반 미만만 수집되면 파이프라인 일시 장애로 보고 건너뜀
    # (merge가 old값 전체를 새 데이터인 양 저장해 전체 frozen 되는 것을 방지)
    if len(macro) < 10:
        print(f"macro 수집 부족({len(macro)}/20) - 일시 장애 의심, 이번 회차 건너뜀")
        return
    # 이번 회차에 빠진 지표(소스 깨짐 등)는 기존 파일의 직전 정상값 유지
    old_macro = {}
    try:
        with open(OUT_PATH, encoding="utf-8") as f:
            old_macro = json.load(f).get("macro", {})
    except Exception:
        pass
    # frozen-feed 감지: 한국 지수가 직전과 소수점까지 동일하면 경고 (KIS 피드 멈춤 가능성)
    for _k in ("코스피", "코스닥"):
        _n, _o = macro.get(_k, {}).get("price"), old_macro.get(_k, {}).get("price")
        if _n is not None and _o is not None and _n == _o:
            print(f"[WARN] {_k} 가격 직전과 동일({_n}) — KIS 피드 멈춤 의심")
    # fetch_macro 티커에서 빠진(=삭제된) 키는 old에서도 버려 부활 방지
    old_macro = {k: v for k, v in old_macro.items() if k in macro}
    payload = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "macro": {**old_macro, **macro},
    }
    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)
    os.replace(tmp, OUT_PATH)
    print(f"OK {len(macro)} indices saved - {payload['generated_at']}")
    # git_push()  # nginx 직접 서빙으로 전환 — Pages 배포 불필요 (2026-06-11)


if __name__ == "__main__":
    main()
