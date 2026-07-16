#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
us_context.py — US 섹터 로테이션 연동 모듈 (eqai / radar 공용)
================================================================
signals_us.json + kr_us_sector_map.json 을 읽어:

  1) us_rotation_block()      : eqai 프롬프트에 넣을 압축 텍스트 블록
  2) us_quadrant(kr_sector)   : KR 섹터의 US 카운터파트 사분면 조회
                                → radar 섀도우 로깅용

설계 원칙: 완전 fail-open. 파일이 없거나 손상돼도 예외를 밖으로
던지지 않고 빈 값("" / None)을 반환한다 — radar의 EQAI caution
섹터 연동과 동일한 패턴. 호출부는 None 체크만 하면 된다.

데이터 신선도: as_of가 3일(주말 감안 최대 4일) 이상 오래되면
stale로 간주하고 빈 값을 반환해 낡은 로테이션 정보가 판단에
섞이는 것을 방지한다.
"""

import json
import os
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
US_SIGNALS = os.path.join(BASE_DIR, "docs", "data", "signals_us.json")
KR_US_MAP = os.path.join(BASE_DIR, "docs", "data", "kr_us_sector_map.json")

STALE_DAYS = 4  # as_of가 이보다 오래되면 무시 (금요일 데이터 → 화요일 아침까지 유효)

_QUAD_ORDER = ["Leading", "Improving", "Weakening", "Lagging"]
_QUAD_KO = {
    "Leading": "주도", "Improving": "전환", "Weakening": "약화", "Lagging": "소외",
}


def _load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _load_us(max_stale_days=STALE_DAYS):
    """signals_us.json 로드 + 신선도 검사. 실패/stale 시 None."""
    data = _load_json(US_SIGNALS)
    if not data or not data.get("sectors"):
        return None
    try:
        as_of = datetime.strptime(data["as_of"], "%Y-%m-%d").date()
        today = datetime.now(timezone(timedelta(hours=9))).date()
        if (today - as_of).days > max_stale_days:
            return None
    except Exception:
        return None
    return data


# ────────────────────────────────────────────────────────────────
# 1) eqai 프롬프트용 압축 블록
# ────────────────────────────────────────────────────────────────
def us_rotation_block() -> str:
    """
    eqai 프롬프트에 삽입할 US 섹터 로테이션 요약.
    실패 시 빈 문자열 반환 (프롬프트에 그냥 안 들어감).

    출력 예:
    [미국 섹터 로테이션 · 2026-07-15 마감 · SPY 대비]
    Leading(주도): 금융XLF(R105.2/M101.7)
    Improving(전환): 커뮤니케이션XLC, 에너지XLE, 경기소비재XLY
    Weakening(약화): 헬스케어XLV, 산업재XLI
    Lagging(소외): 기술XLK, 필수소비재XLP, 유틸리티XLU, 소재XLB, 부동산XLRE
    """
    data = _load_us()
    if not data:
        return ""

    ko_name = {
        "XLK": "기술", "XLF": "금융", "XLE": "에너지", "XLV": "헬스케어",
        "XLI": "산업재", "XLY": "경기소비재", "XLP": "필수소비재",
        "XLU": "유틸리티", "XLB": "소재", "XLRE": "부동산", "XLC": "커뮤니케이션",
    }

    groups = {q: [] for q in _QUAD_ORDER}
    for s in data["sectors"]:
        q = s.get("quadrant")
        if q in groups:
            groups[q].append(s)
    for q in groups:  # 사분면 내 RS-Ratio 내림차순
        groups[q].sort(key=lambda s: -(s.get("ratio") or 0))

    lines = [f"[미국 섹터 로테이션 · {data['as_of']} 마감 · {data.get('benchmark','SPY')} 대비]"]
    for q in _QUAD_ORDER:
        if not groups[q]:
            continue
        if q == "Leading":  # 주도 섹터만 수치 포함 (토큰 절약)
            items = ", ".join(
                f"{ko_name.get(s['ticker'], '')}{s['ticker']}"
                f"(R{s['ratio']:.1f}/M{s['momentum']:.1f})"
                for s in groups[q])
        else:
            items = ", ".join(
                f"{ko_name.get(s['ticker'], '')}{s['ticker']}" for s in groups[q])
        lines.append(f"{q}({_QUAD_KO[q]}): {items}")
    return "\n".join(lines)


# ────────────────────────────────────────────────────────────────
# 2) radar 섀도우 로깅용 조회
# ────────────────────────────────────────────────────────────────
_cache = {"map": None, "us": None}


def _quad_by_ticker():
    if _cache["us"] is None:
        data = _load_us()
        _cache["us"] = {
            s["ticker"]: s["quadrant"] for s in (data or {}).get("sectors", [])
        } or {}
    return _cache["us"]


def us_quadrant(kr_sector: str):
    """
    KR 섹터명 → {"us": "XLF", "quadrant": "Leading"} 또는 None.
    primary 카운터파트(매핑 리스트 첫 번째) 기준.
    매핑 없음 / US 데이터 없음 / stale → None (fail-open).
    """
    if _cache["map"] is None:
        m = _load_json(KR_US_MAP)
        _cache["map"] = (m or {}).get("map", {})
    entry = _cache["map"].get(kr_sector)
    if not entry or not entry.get("us"):
        return None
    ticker = entry["us"][0]
    quad = _quad_by_ticker().get(ticker)
    if not quad:
        return None
    return {"us": ticker, "quadrant": quad}


def us_tailwind(kr_sector: str):
    """
    섀도우 로깅/백테스트용 단순화: US 카운터파트가
    Leading/Improving이면 True, Weakening/Lagging이면 False,
    판단 불가면 None.
    """
    r = us_quadrant(kr_sector)
    if r is None:
        return None
    return r["quadrant"] in ("Leading", "Improving")


if __name__ == "__main__":
    # 간단 자가 테스트
    print("── us_rotation_block ──")
    print(us_rotation_block() or "(빈 값 — 데이터 없음/stale)")
    print("\n── us_quadrant 샘플 ──")
    for sec in ["반도체_소자", "은행", "미분류", "없는섹터"]:
        print(f"{sec:12} → {us_quadrant(sec)}  tailwind={us_tailwind(sec)}")