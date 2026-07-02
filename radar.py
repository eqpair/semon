"""
radar.py — 탑다운 + 바텀업 통합 감지 및 텔레그램 알림

감지 기준:
  1단계 (탑다운) — 섹터 필터
    - quadrant == 'improving' AND sector_ret_1d >= 0 (오늘 실제로 오르는 섹터)

  2단계 (바텀업) — 종목 필터 (통과 섹터 내에서만)
    - signal == 'prime'
    - 또는 short_rs_grade == 'breakout' AND vol_ratio >= 1.5

당일 중복 알림 방지: data/radar_sent.json
"""

import json
import logging
import os
import aiohttp
from pathlib import Path
from utils import now_kst

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
RADAR_SENT_PATH  = "/home/ubuntu/semon/data/radar_sent.json"

# 감지 임계값
VOL_BREAKOUT_THRESHOLD = 1.5   # Breakout 인정 최소 거래량 배수


# ── radar_sent 관리 ───────────────────────────────────────────

def _load_sent() -> dict:
    try:
        with open(RADAR_SENT_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_sent(sent: dict) -> None:
    try:
        Path(RADAR_SENT_PATH).parent.mkdir(parents=True, exist_ok=True)
        with open(RADAR_SENT_PATH, "w", encoding="utf-8") as f:
            json.dump(sent, f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"radar_sent 저장 실패: {e}")


def _is_sent_today(sent: dict, key: str) -> bool:
    today = now_kst().strftime("%Y-%m-%d")
    return sent.get(key) == today


def _mark_sent(sent: dict, key: str) -> None:
    today = now_kst().strftime("%Y-%m-%d")
    sent[key] = today


# ── 텔레그램 전송 ─────────────────────────────────────────────

async def _send_telegram(text: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("텔레그램 토큰/chat_id 미설정")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return True
                else:
                    body = await resp.text()
                    logger.warning(f"텔레그램 전송 실패: {resp.status} {body}")
                    return False
    except Exception as e:
        logger.warning(f"텔레그램 전송 오류: {e}")
        return False


# ── 선정 이유 생성 ────────────────────────────────────────────

def _make_reason(signal: str, grade: str, gap, vol, rs5, rs_ratio, rs_momentum) -> str:
    lines = []

    if signal == "prime":
        lines.append("<b>PRIME</b>: Lagging→Improving 전환 + 모멘텀 가속")
        detail = []
        if gap is not None and gap >= 3:
            detail.append("오늘 섹터 대비 단독 강세")
        if vol is not None and vol >= 1.5:
            detail.append(f"거래량 {vol:.1f}x 동반")
        if rs_ratio is not None and rs_ratio < 98:
            detail.append("장기 소외 구간 → 전환 초기")
        if detail:
            lines.append("   " + " · ".join(detail))

    elif grade == "breakout":
        reason = "<b>Breakout</b>:"
        if gap is not None and gap >= 3:
            reason += f" 섹터 대비 +{gap:.1f}%p 이탈"
        elif rs5 is not None and rs5 >= 2.0:
            reason += f" 5일 RS-5 {rs5:.1f}x (섹터 대비 강세)"
        if vol is not None and vol >= 1.5:
            reason += f" + 거래량 {vol:.1f}x 동반"
        lines.append(reason)
        if rs_ratio is not None and rs_ratio < 100:
            lines.append("   장기 소외 구간에서 단기 강세 전환 신호")

    return "\n".join(lines)


# ── 메시지 포맷 ───────────────────────────────────────────────

def _sector_tag(quadrant: str) -> str:
    return {
        "improving": "☑️ Improving",
        "leading":   "🟢 Leading",
        "lagging":   "🟠 Lagging",
        "weakening": "🔴 Weakening",
    }.get(quadrant, quadrant)


def _format_alert(sector_name: str, sector_rrg: dict,
                  sector_data: dict, stocks: list) -> str:
    quad  = sector_rrg.get("quadrant", "-")
    rs_r  = sector_rrg.get("rs_ratio")
    rs_m  = sector_rrg.get("rs_momentum")
    ret5d = sector_data.get("sector_ret_5d")
    ret1d = sector_data.get("sector_ret_1d")

    lines = []

    # 섹터 헤더
    lines.append(f"<b>[{sector_name}]</b> {_sector_tag(quad)}")
    if rs_r and rs_m:
        lines.append(f"RS-R {rs_r:.1f} / RS-M {rs_m:.1f}")
    ret_parts = []
    if ret1d is not None:
        ret_parts.append(f"당일 {'+' if ret1d >= 0 else ''}{ret1d:.1f}%")
    if ret5d is not None:
        ret_parts.append(f"5D {'+' if ret5d >= 0 else ''}{ret5d:.1f}%")
    if ret_parts:
        lines.append(" | ".join(ret_parts))

    lines.append("")

    # 종목별
    for s in stocks:
        signal = s.get("signal", "")
        grade  = s.get("short_rs_grade", "")
        name   = s.get("name", "")
        code   = s.get("code", "")
        gap    = s.get("gap_1d")
        vol    = s.get("vol_ratio")
        rs5    = s.get("rs_5d")
        rs_r_s = s.get("rs_ratio")
        rs_m_s = s.get("rs_momentum")
        ret1d_s = s.get("ret_1d")

        # 아이콘
        icon = "📈" if signal == "prime" else "⚡"
        lines.append(f"{icon} <b>{name}</b> ({code})")

        # 수치
        parts = []
        if gap is not None:
            parts.append(f"GAP {'+' if gap >= 0 else ''}{gap:.1f}%")
        if vol is not None:
            parts.append(f"VOL {vol:.1f}x")
        if rs5 is not None:
            parts.append(f"RS-5 {rs5:.1f}x")
        if ret1d_s is not None:
            parts.append(f"1D {'+' if ret1d_s >= 0 else ''}{ret1d_s:.1f}%")
        if parts:
            lines.append("  " + " | ".join(parts))

        # 선정 이유
        reason = _make_reason(signal, grade, gap, vol, rs5, rs_r_s, rs_m_s)
        if reason:
            lines.append(reason)

        lines.append("")

    return "\n".join(lines).strip()


# ── 메인 감지 함수 ────────────────────────────────────────────

# ── EQAI 회피 섹터 로드 (fail-open) ──
EQAI_REPORT_PATH = "/home/ubuntu/semon/docs/data/eqai_report.json"

def _load_eqai_caution() -> set:
    """오늘자 EQAI 리포트의 caution_sectors를 반환.
    리포트 없음/에러/날짜불일치 시 빈 set (fail-open — radar 정상 작동)."""
    try:
        with open(EQAI_REPORT_PATH, encoding="utf-8") as fp:
            r = json.load(fp)
        gen = (r.get("generated_at", "") or "")[:10]
        today = now_kst().strftime("%Y-%m-%d")
        if gen != today:
            logger.info(f"EQAI 리포트 날짜 불일치({gen} != {today}) — caution 미적용")
            return set()
        cautions = set(r.get("caution_sectors", []))
        if cautions:
            logger.info(f"EQAI caution 섹터 로드: {cautions}")
        return cautions
    except Exception as e:
        logger.info(f"EQAI 리포트 로드 실패({e}) — caution 미적용 (fail-open)")
        return set()


# ── MID radar: 백테스트 검증 조합 (2026-07 검증) ──────────────
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


async def run_radar(signals: dict) -> None:
    """
    signals: calc_all_signals() 반환값
    main.py 루프에서 매 루프마다 호출
    """
    if not signals:
        return

    sectors    = signals.get("sectors", {})
    sector_rrg = signals.get("sector_rrg", {})
    sent       = _load_sent()
    today      = now_kst().strftime("%Y-%m-%d")
    eqai_caution = _load_eqai_caution()  # EQAI 회피 섹터 (fail-open)

    # MID radar (검증 조합) — 기존 radar와 독립 실행
    await _run_mid_radar(sectors, sector_rrg, sent, today, eqai_caution)

    for sector_name, sector_data in sectors.items():
        rrg   = sector_rrg.get(sector_name, {})
        quad  = rrg.get("quadrant", "neutral")
        ret1d = sector_data.get("sector_ret_1d")

        # ── 1단계: 섹터 필터 ──────────────────────────────────
        # Improving 섹터 + 오늘 실제로 오르는 섹터만 통과
        if quad != "improving":
            continue
        if ret1d is None or ret1d < 0:
            continue
        # EQAI 회피 섹터 차단 (오늘자 리포트 있을 때만 — 중도 방식)
        if sector_name in eqai_caution:
            logger.info(f"radar 차단: {sector_name} (EQAI caution)")
            continue

        # ── 2단계: 종목 필터 ──────────────────────────────────
        alert_stocks = []
        for s in sector_data.get("candidates", []):
            signal = s.get("signal", "")
            grade  = s.get("short_rs_grade", "")
            vol    = s.get("vol_ratio") or 0
            code   = s.get("code", "")

            is_prime    = signal == "prime"
            is_breakout = grade == "breakout" and vol >= VOL_BREAKOUT_THRESHOLD

            if not (is_prime or is_breakout):
                continue

            # 당일 중복 방지
            sent_key = f"{today}:{sector_name}:{code}"
            if _is_sent_today(sent, sent_key):
                continue

            alert_stocks.append(s)
            _mark_sent(sent, sent_key)

        if not alert_stocks:
            continue

        # ── 알림 전송 ──────────────────────────────────────────
        msg = _format_alert(sector_name, rrg, sector_data, alert_stocks)
        if msg.strip():
            ok = await _send_telegram(msg)
            if ok:
                logger.info(f"radar 알림: {sector_name} {len(alert_stocks)}개 종목")
            else:
                # 전송 실패 시 sent에서 제거 → 다음 루프 재시도
                for s in alert_stocks:
                    sent_key = f"{today}:{sector_name}:{s.get('code','')}"
                    sent.pop(sent_key, None)

    _save_sent(sent)