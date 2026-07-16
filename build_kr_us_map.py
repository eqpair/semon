#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_kr_us_map.py — KR 섹터 → US 섹터 ETF 매핑 초안 생성 (1회성 헬퍼)
========================================================================
docs/data/signals.json 의 실제 섹터명을 읽어 키워드 규칙으로
docs/data/kr_us_sector_map.json 초안을 생성한다.

- 매핑 형식: {"섹터명": {"us": ["XLK", ...], "confidence": "high|low"}}
  us 리스트의 첫 번째가 primary 카운터파트.
- 매핑 불가 섹터는 us: [] 로 남기고 stdout에 출력 → 수동 보완 대상.
- 이미 kr_us_sector_map.json 이 존재하면 기존 수동 수정을 보존하고
  새 섹터만 추가한다 (merge 방식).

실행: ~/semon/venv/bin/python3 build_kr_us_map.py
"""

import json
import os
import re
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIGNALS = os.path.join(BASE_DIR, "docs", "data", "signals.json")
OUT_MAP = os.path.join(BASE_DIR, "docs", "data", "kr_us_sector_map.json")
KST = timezone(timedelta(hours=9))

# 키워드 → (ETF 리스트, confidence)
# 위에서부터 첫 매칭 적용. 복수 ETF는 primary 우선 순서.
RULES = [
    # ── XLK 기술 ──
    (r"반도체",              (["XLK"], "high")),
    (r"소프트웨어|SW|클라우드|보안|AI(?!지)|인공지능", (["XLK"], "high")),
    (r"IT|디스플레이|전자부품|PCB|기판|컴퓨터|테크|핸드셋|스마트폰|통신장비|네트워크장비", (["XLK"], "high")),
    # ── XLC 커뮤니케이션 ──
    (r"게임",                (["XLC"], "high")),
    (r"엔터|미디어|콘텐츠|방송|광고|플랫폼|포털", (["XLC"], "high")),
    (r"통신(?!장비)",        (["XLC"], "high")),
    # ── XLF 금융 ──
    (r"은행|증권|보험|카드|캐피탈|금융", (["XLF"], "high")),
    # ── XLV 헬스케어 ──
    (r"바이오|제약|의료|헬스|진단|임상|신약", (["XLV"], "high")),
    # ── XLE 에너지 ──
    (r"정유|석유|가스전|원유|에너지(?!저장)", (["XLE"], "high")),
    # ── XLU 유틸리티 ──
    (r"전력(?!기기|설비)|유틸|도시가스|발전(?!기)", (["XLU"], "high")),
    # ── XLB 소재 ──
    (r"2차전지|이차전지|배터리|양극재|음극재|전해질", (["XLB", "XLY"], "low")),  # 소재 중심이나 EV체인 성격 겸유
    (r"화학|철강|비철|금속|소재|시멘트|제지|유리|태양광(?!발전)", (["XLB"], "high")),
    # ── XLI 산업재 ──
    (r"조선|기계|방산|항공(?!사)|우주|로봇|전력기기|변압기|건설기계|중공업", (["XLI"], "high")),
    (r"운송|물류|해운|항공사|택배", (["XLI"], "high")),
    (r"건설|건자재",          (["XLI", "XLRE"], "low")),  # 한국 건설은 산업재 성격이 강함
    # ── XLY 경기소비재 ──
    (r"자동차|모빌리티|타이어|부품(?!전자)", (["XLY"], "high")),
    (r"유통|리테일|이커머스|백화점|편의점", (["XLY", "XLP"], "low")),
    (r"의류|패션|화장품|뷰티|여행|레저|호텔|카지노|교육", (["XLY"], "high")),
    # ── XLP 필수소비재 ──
    (r"음식료|식품|음료|주류|담배|생활용품|농업|사료", (["XLP"], "high")),
    # ── XLRE 부동산 ──
    (r"리츠|부동산",          (["XLRE"], "high")),
]

EXCLUDE = {"미분류"}  # 매핑 대상 제외


def classify(name: str):
    for pat, (etfs, conf) in RULES:
        if re.search(pat, name):
            return etfs, conf
    return [], "none"


def main():
    with open(SIGNALS, encoding="utf-8") as f:
        data = json.load(f)
    sectors = sorted(data.get("sectors", {}).keys())
    if not sectors:
        raise RuntimeError("signals.json에서 섹터를 찾지 못함")

    # 기존 맵이 있으면 수동 수정 보존
    existing = {}
    if os.path.exists(OUT_MAP):
        with open(OUT_MAP, encoding="utf-8") as f:
            existing = json.load(f).get("map", {})

    result, unmapped, lowconf = {}, [], []
    for name in sectors:
        if name in EXCLUDE:
            continue
        if name in existing:                     # 기존 항목 보존
            result[name] = existing[name]
            continue
        etfs, conf = classify(name)
        result[name] = {"us": etfs, "confidence": conf}
        if not etfs:
            unmapped.append(name)
        elif conf == "low":
            lowconf.append(name)

    out = {
        "version": 1,
        "updated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST"),
        "etf_names": {
            "XLK": "Technology", "XLF": "Financials", "XLE": "Energy",
            "XLV": "Health Care", "XLI": "Industrials", "XLY": "Cons. Discretionary",
            "XLP": "Cons. Staples", "XLU": "Utilities", "XLB": "Materials",
            "XLRE": "Real Estate", "XLC": "Communication",
        },
        "map": result,
    }
    tmp = OUT_MAP + ".tmp"
    os.makedirs(os.path.dirname(OUT_MAP), exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=1)
    os.replace(tmp, OUT_MAP)

    print(f"[map] 총 {len(result)}개 섹터 매핑 완료 → {OUT_MAP}")
    print(f"\n── 전체 매핑 결과 ──")
    for name, m in result.items():
        mark = "  " if m["confidence"] == "high" else ("?? " if m["confidence"] == "low" else "!! ")
        print(f"{mark}{name:20} → {','.join(m['us']) or '(미매핑)'}")
    if lowconf:
        print(f"\n?? 저신뢰 (수동 확인 권장): {lowconf}")
    if unmapped:
        print(f"!! 미매핑 (수동 보완 필요): {unmapped}")


if __name__ == "__main__":
    main()