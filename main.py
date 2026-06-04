import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv("/home/ubuntu/semon/.env")

from config import SECTORS, WAIT_TIME
from crawler import fetch_kospi_ohlcv
from kis_price import fetch_all_prices_kis as fetch_all_prices, fetch_all_ohlcv_kis as fetch_all_ohlcv
from sector_signal import (update_prices, update_ohlcv, calc_all_signals,
                           load_market_caps_into_store, update_kospi,
                           save_rrg_history, load_rrg_history)
from utils import is_market_time, is_near_market_close, is_nxt_time, now_kst, save_and_push, save_closing, save_to_s3
from fetch_stocks import fetch_all_market_caps, save_market_caps, load_market_caps
from radar import run_radar
from signal_logger import log_signals, update_tracking

# ── 로깅 설정 ─────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ubuntu/semon/semon.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── 전체 종목 코드 목록 (중복 제거) ──────────────────────────

ALL_CODES = list({code for codes in SECTORS.values() for code, _ in codes})
logger.info(f"총 {len(ALL_CODES)}개 종목 로드")

# ── 경로 설정 ─────────────────────────────────────────────────

MARKET_CAP_PATH = "/home/ubuntu/semon/data/market_cap.json"

# ── 갱신 주기 ─────────────────────────────────────────────────

OHLCV_REFRESH_INTERVAL = 3600  # 1시간마다
_last_ohlcv_fetch  = None
_closing_saved: str | None = None
_market_cap_date: str | None = None
_off_market_pushed: str | None = None
_kospi_date: str | None = None


def _need_ohlcv_refresh() -> bool:
    global _last_ohlcv_fetch
    if _last_ohlcv_fetch is None:
        return True
    elapsed = (now_kst() - _last_ohlcv_fetch).total_seconds()
    _n = now_kst()
    # 마감 직후(15:31~16:10)엔 당일 KRX 봉을 빨리 확정 반영 — 2분 주기
    if (_n.hour == 15 and _n.minute >= 31) or (_n.hour == 16 and _n.minute <= 10):
        return elapsed >= 120
    return elapsed >= OHLCV_REFRESH_INTERVAL


def _is_after_market_close() -> bool:
    """15:31 이후 — 당일 봉이 확정된 이후"""
    now = now_kst()
    return now.hour > 15 or (now.hour == 15 and now.minute >= 31)


def _is_nxt_premarket() -> bool:
    """평일 08:00~08:59 — NXT 프리마켓 시간.
    이 시간엔 KIS가 미확정 오늘 봉을 closes에 미리 넣어 내려준다."""
    from datetime import time
    now = now_kst()
    if not (0 <= now.weekday() <= 4):
        return False
    return time(8, 0) <= now.time() < time(9, 0)


# ── 메인 루프 ─────────────────────────────────────────────────

async def run():
    global _last_ohlcv_fetch, _closing_saved, _market_cap_date, _off_market_pushed, _kospi_date
    logger.info("semon 시작")

    # 시작 시 기존 시총 파일 로드
    caps = load_market_caps(MARKET_CAP_PATH)
    if caps:
        load_market_caps_into_store(caps)
        logger.info(f"시총 파일 로드: {len(caps)}개 종목")
    else:
        logger.warning("시총 파일 없음 — 첫 장 시작 시 자동 갱신됩니다")

    # 시작 시 rrg_history 복원
    load_rrg_history()

    # 시작 시 OHLCV 무조건 1회 로드
    # 장중 시작이면 KIS가 당일 봉 close=현재가로 내려줘 ret_1d 오계산 발생
    # → strip_today=True 로 당일 봉 제거, current_price fetch 후 정상 계산됨
    logger.info("시작 시 OHLCV 초기 로드 시작 (약 10~15분 소요)")
    try:
        ohlcv = await fetch_all_ohlcv(ALL_CODES)
        strip = is_market_time() or _is_nxt_premarket()  # 장중 + NXT 프리마켓 (KIS가 미확정 봉 내려줌)
        if strip:
            logger.info("장중 시작 — OHLCV 당일 봉 제거 (strip_today=True)")
        update_ohlcv(ohlcv, strip_today=strip)
        _last_ohlcv_fetch = now_kst()
        success = sum(1 for v in ohlcv.values() if v is not None)
        logger.info(f"OHLCV 초기 로드 완료: {success}/{len(ALL_CODES)}개")
    except Exception as e:
        logger.error(f"OHLCV 초기 로드 실패: {e}", exc_info=True)

    while True:
        try:
            today = now_kst().strftime("%Y-%m-%d")

            # ── closing 저장 체크 ────────────────────────────────
            if is_near_market_close() and _closing_saved != today:
                logger.info("장 마감 closing 스냅샷 계산 시작")
                prices = await fetch_all_prices(ALL_CODES)
                update_prices(prices)
                signals = calc_all_signals()
                if save_closing(signals):
                    _closing_saved = today
                    logger.info(f"closing 스냅샷 저장 완료: {today}")
                else:
                    logger.error("closing 스냅샷 저장 실패")
                save_rrg_history()

            # ── 장 외 시간 ───────────────────────────────────────
            if not is_market_time():
                if is_nxt_time():
                    # NXT 시간(08:00~08:59, 15:31~20:00) — 5분마다 현재가 fetch + 신호 계산 + push
                    logger.info("NXT 시간 — 현재가 fetch 시작")
                    prices = await fetch_all_prices(ALL_CODES)
                    update_prices(prices)
                    signals = calc_all_signals()
                    save_and_push(signals)
                    save_to_s3(signals)
                    logger.info("NXT 시간 — 신호 계산 + push 완료")
                else:
                    # 야간(20:01~07:59) — 1회 tail push 후 대기
                    if _off_market_pushed != today and _closing_saved == today:
                        logger.info("야간 — tail 포함 signals.json 갱신")
                        signals = calc_all_signals()
                        save_and_push(signals)
                        save_to_s3(signals)
                        _off_market_pushed = today
                        logger.info("야간 tail push 완료")
                    else:
                        logger.info("야간 — 대기 중")
                await asyncio.sleep(WAIT_TIME)
                continue

            # 1. 시총 갱신 — 하루 1회
            if _market_cap_date != today:
                logger.info("시총 갱신 시작")
                caps = await fetch_all_market_caps(ALL_CODES)
                save_market_caps(caps, MARKET_CAP_PATH)
                load_market_caps_into_store(caps)
                _market_cap_date = today
                success = sum(1 for v in caps.values() if v > 0)
                logger.info(f"시총 갱신 완료: {success}/{len(ALL_CODES)}개")

            # 2. KOSPI 갱신 — 하루 1회
            if _kospi_date != today:
                logger.info("KOSPI 일봉 fetch 시작")
                kospi_closes = await fetch_kospi_ohlcv()
                if kospi_closes:
                    update_kospi(kospi_closes)
                    _kospi_date = today
                else:
                    logger.warning("KOSPI fetch 실패 — 섹터 평균으로 폴백")

            # 3. OHLCV 갱신 — 15:31 이후 + 1시간마다
            #    장중에 받으면 당일 봉 close=0.0 (미확정)이 들어와
            #    _get_return()에서 ret_1d = -100% 오류 발생
            #    초기 로드는 run() 시작 시 이미 완료됨
            if _need_ohlcv_refresh() and _last_ohlcv_fetch is not None:
                if _is_after_market_close() or _last_ohlcv_fetch.date() < now_kst().date():
                    logger.info("OHLCV fetch 시작")
                    ohlcv = await fetch_all_ohlcv(ALL_CODES)
                    strip = is_market_time() or _is_nxt_premarket()  # 장중 + NXT 프리마켓 (KIS가 미확정 봉 내려줌)
                    if strip:
                        logger.info("장중 갱신 — OHLCV 당일 봉 제거 (strip_today=True)")
                    update_ohlcv(ohlcv, strip_today=strip)
                    _last_ohlcv_fetch = now_kst()
                else:
                    logger.info("OHLCV fetch 스킵 — 장중 (15:31 이후 갱신, 당일 봉 미확정 방지)")

            # 4. 현재가 fetch
            logger.info("현재가 fetch 시작")
            prices = await fetch_all_prices(ALL_CODES)
            update_prices(prices)

            # 5. 신호 계산
            logger.info("신호 계산 시작")
            signals = calc_all_signals()

            # 6. 신호 검증 로그 (push 전에 먼저 처리)
            log_signals(signals)
            update_tracking(signals)

            # 7. signal_log → docs/data/ 복사 + staged
            import shutil, os, subprocess
            src = "/home/ubuntu/semon/data/signal_log.json"
            dst = "/home/ubuntu/semon/docs/data/signal_log.json"
            if os.path.exists(src):
                shutil.copy2(src, dst)
                subprocess.run(["git", "add", dst], cwd="/home/ubuntu/semon", capture_output=True)

            # 8. JSON 저장 + git push (signal_log 포함)
            save_and_push(signals)
            save_to_s3(signals)

            # 9. rrg_history 영속화
            save_rrg_history()

            # 10. radar 감지 + 텔레그램 알림
            await run_radar(signals)

            logger.info(f"완료 — {WAIT_TIME}초 후 재실행")
            await asyncio.sleep(WAIT_TIME)

        except KeyboardInterrupt:
            logger.info("수동 종료")
            break
        except Exception as e:
            logger.error(f"메인 루프 오류: {e}", exc_info=True)
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(run())