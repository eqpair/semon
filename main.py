import asyncio
import logging
import sys
from pathlib import Path

from config import SECTORS, WAIT_TIME
from crawler import fetch_all_prices, fetch_all_ohlcv
from sector_signal import (update_prices, update_ohlcv, calc_all_signals,
                           load_market_caps_into_store,
                           save_rrg_history, load_rrg_history)
from utils import is_market_time, is_near_market_close, now_kst, save_and_push, save_closing
from fetch_stocks import fetch_all_market_caps, save_market_caps, load_market_caps

# ── 로깅 설정 ─────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/eq/semon/semon.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── 전체 종목 코드 목록 (중복 제거) ──────────────────────────

ALL_CODES = list({code for codes in SECTORS.values() for code, _ in codes})
logger.info(f"총 {len(ALL_CODES)}개 종목 로드")

# ── 경로 설정 ─────────────────────────────────────────────────

MARKET_CAP_PATH = "/home/eq/semon/data/market_cap.json"

# ── 갱신 주기 ─────────────────────────────────────────────────

OHLCV_REFRESH_INTERVAL = 3600  # 1시간마다
_last_ohlcv_fetch  = None
_closing_saved: str | None = None
_market_cap_date: str | None = None  # 오늘 시총 갱신 여부 (날짜 문자열)


def _need_ohlcv_refresh() -> bool:
    global _last_ohlcv_fetch
    if _last_ohlcv_fetch is None:
        return True
    return (now_kst() - _last_ohlcv_fetch).total_seconds() >= OHLCV_REFRESH_INTERVAL


# ── 메인 루프 ─────────────────────────────────────────────────

async def run():
    global _last_ohlcv_fetch, _closing_saved, _market_cap_date
    logger.info("semon 시작")

    # 시작 시 기존 시총 파일 로드 → sector_signal에 주입
    caps = load_market_caps(MARKET_CAP_PATH)
    if caps:
        load_market_caps_into_store(caps)
        logger.info(f"시총 파일 로드: {len(caps)}개 종목")
    else:
        logger.warning("시총 파일 없음 — 첫 장 시작 시 자동 갱신됩니다")

    # 시작 시 rrg_history 복원 → 재시작 후에도 tail 즉시 사용 가능
    load_rrg_history()

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
                # closing 저장 시점에 rrg_history도 함께 영속화
                save_rrg_history()

            # ── 장 중 루프 ───────────────────────────────────────
            if not is_market_time():
                logger.info("장 외 시간 — 대기 중")
                await asyncio.sleep(60)
                continue

            # 1. 시총 갱신 — 하루 1회, 장 시작 후 첫 루프에서 실행
            if _market_cap_date != today:
                logger.info("시총 갱신 시작")
                caps = await fetch_all_market_caps(ALL_CODES)
                save_market_caps(caps, MARKET_CAP_PATH)
                load_market_caps_into_store(caps)
                _market_cap_date = today
                success = sum(1 for v in caps.values() if v > 0)
                logger.info(f"시총 갱신 완료: {success}/{len(ALL_CODES)}개")

            # 2. OHLCV 갱신 (1시간마다) — 과거 종가 + 거래량
            if _need_ohlcv_refresh():
                logger.info("OHLCV fetch 시작")
                ohlcv = await fetch_all_ohlcv(ALL_CODES)
                update_ohlcv(ohlcv)
                _last_ohlcv_fetch = now_kst()

            # 3. 현재가 fetch
            logger.info("현재가 fetch 시작")
            prices = await fetch_all_prices(ALL_CODES)
            update_prices(prices)

            # 4. 신호 계산
            logger.info("신호 계산 시작")
            signals = calc_all_signals()

            # 5. JSON 저장 + git push
            save_and_push(signals)

            # 6. rrg_history 영속화 — 재시작 후 tail 즉시 복원 보장
            save_rrg_history()

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