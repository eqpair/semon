"""
signal_logger.py — PRIME/Confirm/Breakout 신호 검증 로그

기록 시점: 신호 최초 발생 시 (조건 충족 시작)
종료 시점: 신호 조건이 사라진 날 (exit 처리)
재진입: 종료 후 다시 조건 충족하면 새 기록

기록 조건 (엄격):
  PRIME   : 섹터 Improving + signal == 'prime'
  Confirm : 섹터 Improving + signal == 'confirm' + combo_score >= 3
  Breakout: GAP >= 5%p + VOL >= 2x + 섹터 Improving/Lagging
"""

import json
import logging
from pathlib import Path
from utils import now_kst

logger = logging.getLogger(__name__)

SIGNAL_LOG_PATH = "/home/eq/semon/data/signal_log.json"


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


# ── 조건 판별 ─────────────────────────────────────────────────

def _get_signal_key(s: dict, sector_quad: str) -> str | None:
    """
    종목이 기록 조건을 충족하면 신호 키 반환, 아니면 None
    """
    sig   = s.get("signal", "")
    grade = s.get("short_rs_grade", "")
    combo = s.get("combo_score", 0)
    vol   = s.get("vol_ratio") or 0
    gap   = s.get("gap_1d") or 0

    if sig == "prime" and sector_quad == "improving":
        return "prime"
    if sig == "confirm" and sector_quad == "improving" and combo >= 3:
        return "confirm"
    return None


# ── 현재 조건 충족 종목 추출 ──────────────────────────────────

def _get_active_codes(signals: dict) -> dict[str, dict]:
    """
    현재 signals에서 조건 충족 종목 추출
    반환: {code: {signal_key, price, ...}}
    """
    active = {}
    sectors    = signals.get("sectors", {})
    sector_rrg = signals.get("sector_rrg", {})

    for sector_name, sector_data in sectors.items():
        sector_quad   = sector_rrg.get(sector_name, {}).get("quadrant", "neutral")
        sector_ret_1d = sector_data.get("sector_ret_1d")

        for s in sector_data.get("candidates", []):
            code = s.get("code", "")
            key  = _get_signal_key(s, sector_quad)
            if not key:
                continue
            active[code] = {
                "signal_key":    key,
                "sector_name":   sector_name,
                "sector_quad":   sector_quad,
                "sector_ret_1d": sector_ret_1d,
                "signal":        s.get("signal", ""),
                "grade":         s.get("short_rs_grade", ""),
                "combo_score":   s.get("combo_score", 0),
                "price":         s.get("price"),
                "rs_ratio":      s.get("rs_ratio"),
                "rs_momentum":   s.get("rs_momentum"),
                "quadrant":      s.get("quadrant"),
                "gap_1d":        s.get("gap_1d"),
                "vol_ratio":     s.get("vol_ratio"),
                "rs_5d":         s.get("rs_5d"),
            }
    return active


# ── 신호 기록 + 종료 처리 ─────────────────────────────────────

def log_signals(signals: dict) -> int:
    today  = now_kst().strftime("%Y-%m-%d")
    log    = _load()
    active = _get_active_codes(signals)

    # 현재 추적 중인 항목 (exited=False)
    tracking = {
        e["code"]: e for e in log
        if not e.get("exited", False)
    }

    new_count = 0

    # ── 1. 종료 처리: 추적 중인데 오늘 조건 미충족 ──────────
    for code, entry in tracking.items():
        if code not in active:
            entry["exited"]     = True
            entry["exit_at"]    = today
            entry["exit_price"] = None  # update_tracking에서 채워짐
            entry["ret_exit"]   = None

    # ── 2. 신규 기록: 조건 충족인데 추적 중이 아닌 것 ────────
    for code, info in active.items():
        if code in tracking:
            continue  # 이미 추적 중

        entry = {
            # 기본
            "logged_at":       today,
            "code":            code,
            "name":            _get_name(signals, code),
            "sector":          info["sector_name"],

            # 신호
            "signal":          info["signal"],
            "short_rs_grade":  info["grade"],
            "combo_score":     info["combo_score"],

            # 진입가
            "price_at_signal": info["price"],

            # RRG 위치
            "rs_ratio":        info["rs_ratio"],
            "rs_momentum":     info["rs_momentum"],
            "quadrant":        info["quadrant"],

            # 섹터
            "sector_quadrant": info["sector_quad"],
            "sector_ret_1d":   info["sector_ret_1d"],

            # 단기 지표
            "gap_1d":          info["gap_1d"],
            "vol_ratio":       info["vol_ratio"],
            "rs_5d":           info["rs_5d"],

            # 수익률 추적
            "ret_1d":          None,
            "ret_5d":          None,
            "ret_20d":         None,
            "ret_60d":         None,
            "max_ret_5d":      None,
            "min_ret_5d":      None,
            "quadrant_5d":     None,
            "quadrant_20d":    None,

            # 추적 플래그
            "tracked_1d":      False,
            "tracked_5d":      False,
            "tracked_20d":     False,
            "tracked_60d":     False,

            # 종료
            "exited":          False,
            "exit_at":         None,
            "exit_price":      None,
            "ret_exit":        None,
        }
        log.append(entry)
        new_count += 1

    if new_count > 0 or any(e.get("exit_at") == today for e in log):
        _save(log)
        if new_count > 0:
            logger.info(f"signal_log 신규 기록: {new_count}건")

    return new_count


def _get_name(signals: dict, code: str) -> str:
    for sector_data in signals.get("sectors", {}).values():
        for s in sector_data.get("candidates", []):
            if s.get("code") == code:
                return s.get("name", "")
    return ""


# ── 사후 수익률 추적 ──────────────────────────────────────────

def _calc_ret(price_then: float | None, price_now: float | None) -> float | None:
    if not price_then or not price_now:
        return None
    return round((price_now - price_then) / price_then * 100, 2)


def update_tracking(signals: dict) -> int:
    today  = now_kst().strftime("%Y-%m-%d")
    log    = _load()
    if not log:
        return 0

    price_map = {}
    quad_map  = {}
    for sector_data in signals.get("sectors", {}).values():
        for s in sector_data.get("candidates", []):
            code = s.get("code", "")
            price_map[code] = s.get("price")
            quad_map[code]  = s.get("quadrant")

    updated = 0
    for entry in log:
        logged_at  = entry.get("logged_at", "")
        price_then = entry.get("price_at_signal")
        code       = entry.get("code", "")
        price_now  = price_map.get(code)
        quad_now   = quad_map.get(code)

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

        # 종료 시점 가격 채우기
        if entry.get("exited") and entry.get("exit_at") == today and entry.get("exit_price") is None:
            entry["exit_price"] = price_now
            entry["ret_exit"]   = _calc_ret(price_then, price_now)
            changed = True

        # 기간별 수익률
        if not entry.get("tracked_1d") and days_elapsed >= 1:
            entry["ret_1d"]     = _calc_ret(price_then, price_now)
            entry["tracked_1d"] = True
            changed = True

        if not entry.get("tracked_5d") and days_elapsed >= 7:
            entry["ret_5d"]      = _calc_ret(price_then, price_now)
            entry["quadrant_5d"] = quad_now
            entry["tracked_5d"]  = True
            changed = True

        if not entry.get("tracked_20d") and days_elapsed >= 28:
            entry["ret_20d"]      = _calc_ret(price_then, price_now)
            entry["quadrant_20d"] = quad_now
            entry["tracked_20d"]  = True
            changed = True

        if not entry.get("tracked_60d") and days_elapsed >= 84:
            entry["ret_60d"]      = _calc_ret(price_then, price_now)
            entry["tracked_60d"]  = True
            changed = True

        # 5D 내 최고/최저
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