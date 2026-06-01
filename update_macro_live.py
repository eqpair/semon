#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Update macro prices only into docs/data/macro_live.json."""
import json
import os
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
OUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "data", "macro_live.json")


def in_market_window() -> bool:
    # Skip weekends; run 07-23 KST only
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return False
    return 7 <= now.hour <= 23


def main():
    if not in_market_window():
        print("outside market window - skip")
        return
    from eqai_news import fetch_macro
    macro = fetch_macro()
    if not macro:
        print("macro data empty - skip")
        return
    payload = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "macro": macro,
    }
    tmp = OUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, OUT_PATH)
    print(f"OK {len(macro)} indices saved - {payload['generated_at']}")


if __name__ == "__main__":
    main()
