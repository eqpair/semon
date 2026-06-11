#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Update macro prices only into docs/data/macro_live.json and push."""
import json
import os
import subprocess
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
REPO = os.path.dirname(os.path.abspath(__file__))
REL_PATH = os.path.join("docs", "data", "macro_live.json")
OUT_PATH = os.path.join(REPO, REL_PATH)


def in_market_window() -> bool:
    # Skip weekends; run 07-23 KST only
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    return 7 <= now.hour <= 23


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
    # 이번 회차에 빠진 지표(소스 깨짐 등)는 기존 파일의 직전 정상값 유지
    old_macro = {}
    try:
        with open(OUT_PATH, encoding="utf-8") as f:
            old_macro = json.load(f).get("macro", {})
    except Exception:
        pass
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
