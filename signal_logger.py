"""
signal_logger.py — PRIME/Confirm/Breakout 신호 검증 로그

기록 시점: 신호 최초 발생 시 (당일 중복 방지)
추적 주기: 매 루프마다 1d/5d/20d/60d 수익률 + 사후 사분면 채우기
저장 경로: /home/eq/semon/data/signal_log.json
"""

import json
import logging
from pathlib import Path
from utils import now_kst

logger = logging.getLogger(__name__)

SIGNAL_LOG_PATH = "/home/eq/semon/data/signal_log.json"

# 추적할 신호
TRACK_SIGNALS = {"prime", "confirm"}
TRACK_GRADES  = {"breakout"}

# 추적 기간 (거래일 기준 근사)
TRACK_DAYS = [1, 5, 20, 60]


# ── 파일 I/O ─────────────────────────────────────────────────

def _load() -> list[dict]:
    try:
        with open(SIGNAL_LOG_PATH, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"signal_log 로드 실패: {e}")
        return []


def _save(log: list[dict]) -> None:
    try:
        Path(SIGNAL_LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
        tmp = SIGNAL_LOG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)
        Path(tmp).replace(SIGNAL_LOG_PATH)
    except Exception as e:
        logger.warning(f"signal_log 저장 실패: {e}")


# ── 신호 기록 ─────────────────────────────────────────────────

def log_signals(signals: dict) -> int:
    """
    signals: calc_all_signals() 반환값
    새로 발생한 PRIME/Confirm/Breakout 기록
    반환: 새로 기록된 건수
    """
    today = now_kst().strftime("%Y-%m-%d")
    log   = _load()

    # 오늘 이미 기록된 종목 코드
    logged_today = {
        e["code"] for e in log
        if e.get("logged_at") == today
    }

    new_entries = []
    sectors     = signals.get("sectors", {})
    sector_rrg  = signals.get("sector_rrg", {})

    for sector_name, sector_data in sectors.items():
        sector_quad   = sector_rrg.get(sector_name, {}).get("quadrant", "neutral")
        sector_ret_1d = sector_data.get("sector_ret_1d")

        for s in sector_data.get("candidates", []):
            code  = s.get("code", "")
            sig   = s.get("signal", "")
            grade = s.get("short_rs_grade", "")

            # 추적 대상 신호인지 확인
            # breakout은 Improving/Lagging에서만 의미있음
            is_track = (
                sig in TRACK_SIGNALS or
                (grade in TRACK_GRADES and s.get("quadrant") in ("improving", "lagging"))
            )
            if not is_track:
                continue

            # 오늘 이미 기록된 종목 스킵
            if code in logged_today:
                continue

            entry = {
                # ── 기본 정보
                "logged_at":       today,
                "code":            code,
                "name":            s.get("name", ""),
                "sector":          sector_name,

                # ── 신호
                "signal":          sig,
                "short_rs_grade":  grade,
                "combo_score":     s.get("combo_score", 0),

                # ── 진입 기준가
                "price_at_signal": s.get("price"),

                # ── RRG 위치
                "rs_ratio":        s.get("rs_ratio"),
                "rs_momentum":     s.get("rs_momentum"),
                "quadrant":        s.get("quadrant"),

                # ── 섹터 상태
                "sector_quadrant": sector_quad,
                "sector_ret_1d":   sector_ret_1d,

                # ── 단기 지표
                "gap_1d":          s.get("gap_1d"),
                "vol_ratio":       s.get("vol_ratio"),
                "rs_5d":           s.get("rs_5d"),

                # ── 사후 추적 (나중에 채워짐)
                "ret_1d":          None,
                "ret_5d":          None,
                "ret_20d":         None,
                "ret_60d":         None,
                "max_ret_5d":      None,
                "min_ret_5d":      None,
                "quadrant_5d":     None,
                "quadrant_20d":    None,

                # ── 추적 완료 플래그
                "tracked_1d":      False,
                "tracked_5d":      False,
                "tracked_20d":     False,
                "tracked_60d":     False,
            }
            new_entries.append(entry)
            logged_today.add(code)

    if new_entries:
        log.extend(new_entries)
        _save(log)
        logger.info(f"signal_log 신규 기록: {len(new_entries)}건")

    return len(new_entries)


# ── 사후 수익률 추적 ──────────────────────────────────────────

def _calc_ret(price_then: float | None, price_now: float | None) -> float | None:
    if not price_then or not price_now:
        return None
    return round((price_now - price_then) / price_then * 100, 2)


def update_tracking(signals: dict) -> int:
    """
    기존 로그에서 아직 추적 안 된 항목 수익률 채우기
    signals: 최신 calc_all_signals() 반환값
    반환: 업데이트된 건수
    """
    today  = now_kst().strftime("%Y-%m-%d")
    log    = _load()
    if not log:
        return 0

    # 현재 가격 + 사분면 맵 구성
    price_map = {}
    quad_map  = {}
    for sector_data in signals.get("sectors", {}).values():
        for s in sector_data.get("candidates", []):
            code = s.get("code", "")
            price_map[code] = s.get("price")
            quad_map[code]  = s.get("quadrant")

    updated = 0
    for entry in log:
        logged_at   = entry.get("logged_at", "")
        price_then  = entry.get("price_at_signal")
        code        = entry.get("code", "")
        price_now   = price_map.get(code)
        quad_now    = quad_map.get(code)

        if not logged_at or not price_then or not price_now:
            continue

        try:
            from datetime import date
            d_logged = date.fromisoformat(logged_at)
            d_today  = date.fromisoformat(today)
            days_elapsed = (d_today - d_logged).days
        except Exception:
            continue

        changed = False

        # 1D 추적 (1일 이상 경과)
        if not entry.get("tracked_1d") and days_elapsed >= 1:
            entry["ret_1d"]    = _calc_ret(price_then, price_now)
            entry["tracked_1d"] = True
            changed = True

        # 5D 추적 (7일 이상 경과 — 주말 포함)
        if not entry.get("tracked_5d") and days_elapsed >= 7:
            entry["ret_5d"]      = _calc_ret(price_then, price_now)
            entry["quadrant_5d"] = quad_now
            entry["tracked_5d"]  = True
            changed = True

        # 20D 추적 (28일 이상 경과)
        if not entry.get("tracked_20d") and days_elapsed >= 28:
            entry["ret_20d"]      = _calc_ret(price_then, price_now)
            entry["quadrant_20d"] = quad_now
            entry["tracked_20d"]  = True
            changed = True

        # 60D 추적 (84일 이상 경과)
        if not entry.get("tracked_60d") and days_elapsed >= 84:
            entry["ret_60d"]      = _calc_ret(price_then, price_now)
            entry["tracked_60d"]  = True
            changed = True

        # 5D 내 최고/최저 (7일 미만이면 계속 업데이트)
        if days_elapsed < 7 and price_now:
            ret_now = _calc_ret(price_then, price_now)
            if ret_now is not None:
                if entry.get("max_ret_5d") is None or ret_now > entry["max_ret_5d"]:
                    entry["max_ret_5d"] = ret_now
                    changed = True
                if entry.get("min_ret_5d") is None or ret_now < entry["min_ret_5d"]:
                    entry["min_ret_5d"] = ret_now
                    changed = True

        if changed:
            updated += 1

    if updated:
        _save(log)
        logger.info(f"signal_log 추적 업데이트: {updated}건")

    return updated


# ── 통계 요약 ─────────────────────────────────────────────────

def get_stats() -> dict:
    """
    로그 통계 요약
    반환: {신호별 평균 수익률, 적중률 등}
    """
    log = _load()
    if not log:
        return {}

    def avg(vals):
        v = [x for x in vals if x is not None]
        return round(sum(v) / len(v), 2) if v else None

    def win_rate(vals):
        v = [x for x in vals if x is not None]
        return round(sum(1 for x in v if x > 0) / len(v) * 100, 1) if v else None

    stats = {}
    for sig in ["prime", "confirm", "breakout"]:
        if sig == "breakout":
            subset = [e for e in log if e.get("short_rs_grade") == "breakout"]
        else:
            subset = [e for e in log if e.get("signal") == sig]

        if not subset:
            continue

        stats[sig] = {
            "count":        len(subset),
            "avg_ret_5d":   avg([e.get("ret_5d")  for e in subset]),
            "avg_ret_20d":  avg([e.get("ret_20d") for e in subset]),
            "win_rate_5d":  win_rate([e.get("ret_5d")  for e in subset]),
            "win_rate_20d": win_rate([e.get("ret_20d") for e in subset]),
            "avg_max_5d":   avg([e.get("max_ret_5d") for e in subset]),
            "avg_min_5d":   avg([e.get("min_ret_5d") for e in subset]),
        }

    return stats