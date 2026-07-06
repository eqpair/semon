#!/usr/bin/env python3
"""
patch_mid_radar.py — 검증 조합(트리플 - 바이오/제약 제외) radar 승격 패치

적용 내용:
  1) sector_signal.py: candidates에 "value" 필드 추가 (당일 거래대금, 억원)
  2) radar.py: _run_mid_radar() 추가 — 14:30 이후 Lag→Imp 전환 감지

실행:
    cd ~/semon
    ~/semon/venv/bin/python3 patch_mid_radar.py

각 파일은 .bak_타임스탬프 로 백업되며, anchor가 정확히 1회 매칭되지 않으면
아무것도 수정하지 않고 중단합니다.
"""
import shutil
import sys
from datetime import datetime
from pathlib import Path

BASE = Path.home() / "semon"
TS = datetime.now().strftime("%Y%m%d_%H%M%S")


def patch(path: Path, edits):
    s = path.read_text(encoding="utf-8")
    for old, _ in edits:  # 전체 anchor 검증 먼저 — 하나라도 실패 시 파일 미수정
        n = s.count(old)
        assert n == 1, f"[ABORT] {path.name}: anchor {n}회 매칭\n---\n{old[:120]}\n---"
    shutil.copy2(path, f"{path}.bak_{TS}")
    for old, new in edits:
        s = s.replace(old, new, 1)
    path.write_text(s, encoding="utf-8")
    print(f"[OK] {path.name} 패치 완료 (백업: {path.name}.bak_{TS})")


# ═══════════════════════════════════════════════════════════════
# 1) sector_signal.py — value 필드 추가
# ═══════════════════════════════════════════════════════════════
SS_ANCHOR_CALC = "        vol = _get_vol_ratio(code)\n"
SS_NEW_CALC = SS_ANCHOR_CALC + r"""
        # 당일 거래대금 (억원) — 장중 실시간 누적, 없으면 전일 거래량 폴백
        _tv = current_volume.get(code)
        if _tv is None:
            _e = ohlcv_store.get(code)
            _tv = _e["volumes"][-2] if _e and len(_e.get("volumes", [])) >= 2 else None
        value = (now_price * _tv / 1e8) if (_tv and now_price) else None
"""

SS_ANCHOR_DICT = '            "vol_ratio":      round(vol,    3)    if vol    is not None else None,\n'
SS_NEW_DICT = SS_ANCHOR_DICT + \
    '            "value":          round(value,  1)    if value  is not None else None,\n'

# ═══════════════════════════════════════════════════════════════
# 2) radar.py — MID radar 추가
# ═══════════════════════════════════════════════════════════════
RD_ANCHOR_FUNC = "async def run_radar(signals: dict) -> None:"
RD_NEW_FUNC = r'''# ── MID radar: 백테스트 검증 조합 (2026-07 검증) ──────────────
# 조건: 전일 Lagging → 당일 Improving 전환 + 섹터 Improving/Leading
#       + 당일 거래대금 크로스섹션 상위 50% + 바이오/제약 섹터 제외
# 프로필: 30일 보유, TP +10% / SL -7%, 검증 적중률 52.3% (n=614, net +2.1%)
MID_RADAR_START = "14:30"   # 종가 진입 기준 검증이므로 장 후반만 평가
MID_EXCLUDE_SECTOR_KEYWORDS = ("바이오", "제약")


def _tail_prev_quadrant(tail: list) -> str | None:
    """tail[-1](전일)의 rs_ratio/rs_momentum으로 전일 사분면 복원"""
    if not tail:
        return None
    pt = tail[-1]
    r, m = pt.get("rs_ratio"), pt.get("rs_momentum")
    if r is None or m is None:
        return None
    if r >= 100:
        return "leading" if m >= 100 else "weakening"
    return "improving" if m >= 100 else "lagging"


async def _run_mid_radar(sectors: dict, sector_rrg: dict,
                         sent: dict, today: str, eqai_caution: set) -> None:
    now = now_kst()
    if now.weekday() >= 5 or now.strftime("%H:%M") < MID_RADAR_START:
        return

    # 크로스섹션 거래대금 컷 (상위 50% = 중앙값)
    values = [s["value"] for sd in sectors.values()
              for s in sd.get("candidates", [])
              if s.get("value") is not None]
    if len(values) < 100:   # value 필드 미반영/데이터 부족 시 안전 중단
        logger.info(f"MID radar: value 표본 부족({len(values)}) — skip")
        return
    values.sort()
    median_value = values[len(values) // 2]

    hits = []
    for sec_name, sec_data in sectors.items():
        # 섹터 조건: Improving 또는 Leading
        sq = (sector_rrg.get(sec_name, {}) or {}).get("quadrant", "")
        if sq not in ("improving", "leading"):
            continue
        # 바이오/제약 제외 (백테스트: 해당군 적중률 32%, exp -0.86%)
        if any(k in sec_name for k in MID_EXCLUDE_SECTOR_KEYWORDS):
            continue
        # EQAI caution 차단 (기존 radar와 동일 정책)
        if sec_name in eqai_caution:
            continue

        for s in sec_data.get("candidates", []):
            if s.get("quadrant") != "improving":
                continue
            if _tail_prev_quadrant(s.get("tail") or []) != "lagging":
                continue
            v = s.get("value")
            if v is None or v < median_value:
                continue
            code = s.get("code", "")
            sent_key = f"{today}:MID:{code}"
            if _is_sent_today(sent, sent_key):
                continue
            hits.append((sec_name, sq, s))
            _mark_sent(sent, sent_key)

    if not hits:
        return

    lines = ["🎯 <b>MID-RADAR</b> — Lag→Imp 전환 (검증 조합)",
             "30일 · TP +10% / SL -7% · 백테스트 적중률 52%", ""]
    for sec_name, sq, s in hits:
        r, m = s.get("rs_ratio"), s.get("rs_momentum")
        v = s.get("value")
        lines.append(f"📈 <b>{s.get('name','')}</b> ({s.get('code','')}) [{sec_name}]")
        parts = []
        if r is not None and m is not None:
            parts.append(f"RS-R {r:.1f} / RS-M {m:.1f}")
        if v is not None:
            parts.append(f"대금 {v:,.0f}억")
        parts.append(f"섹터 {sq}")
        lines.append("  " + " | ".join(parts))
        lines.append("")

    ok = await _send_telegram("\n".join(lines).strip())
    if ok:
        logger.info(f"MID radar 알림: {len(hits)}개 종목")
    else:
        for sec_name, sq, s in hits:
            sent.pop(f"{today}:MID:{s.get('code','')}", None)


''' + RD_ANCHOR_FUNC

RD_ANCHOR_CALL = "    for sector_name, sector_data in sectors.items():"
RD_NEW_CALL = """    # MID radar (검증 조합) — 기존 radar와 독립 실행
    await _run_mid_radar(sectors, sector_rrg, sent, today, eqai_caution)

""" + RD_ANCHOR_CALL


def main():
    ss = BASE / "sector_signal.py"
    rd = BASE / "radar.py"
    for p in (ss, rd):
        if not p.exists():
            sys.exit(f"[ABORT] {p} 없음")

    patch(ss, [(SS_ANCHOR_CALC, SS_NEW_CALC), (SS_ANCHOR_DICT, SS_NEW_DICT)])
    patch(rd, [(RD_ANCHOR_CALL, RD_NEW_CALL), (RD_ANCHOR_FUNC, RD_NEW_FUNC)])

    print("\n다음 단계:")
    print("  1. ~/semon/venv/bin/python3 -m py_compile sector_signal.py radar.py")
    print("  2. semon 서비스 재시작")
    print("  3. signals.json에 value 필드 생성 확인")


if __name__ == "__main__":
    main()