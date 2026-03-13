import asyncio
import logging
import sys
from datetime import datetime

from config import SECTORS, WAIT_TIME
from crawler import fetch_all_prices, fetch_all_ohlcv
from sector_signal import update_prices, update_ohlcv, calc_all_signals
from utils import is_market_time, save_and_push

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

# ── OHLCV 갱신 주기 ───────────────────────────────────────────

OHLCV_REFRESH_INTERVAL = 3600  # 1시간마다
_last_ohlcv_fetch: datetime | None = None


def _need_ohlcv_refresh() -> bool:
    global _last_ohlcv_fetch
    if _last_ohlcv_fetch is None:
        return True
    return (datetime.now() - _last_ohlcv_fetch).total_seconds() >= OHLCV_REFRESH_INTERVAL


# ── 메인 루프 ─────────────────────────────────────────────────

async def run():
    global _last_ohlcv_fetch
    logger.info("semon 시작")

    while True:
        try:
            if not is_market_time():
                logger.info("장 외 시간 — 대기 중")
                await asyncio.sleep(60)
                continue

            # 1. OHLCV 갱신 (1시간마다) — 과거 종가 + 거래량
            if _need_ohlcv_refresh():
                logger.info("OHLCV fetch 시작")
                ohlcv = await fetch_all_ohlcv(ALL_CODES)
                update_ohlcv(ohlcv)
                _last_ohlcv_fetch = datetime.now()

            # 2. 현재가 fetch (10분마다)
            logger.info("현재가 fetch 시작")
            prices = await fetch_all_prices(ALL_CODES)
            update_prices(prices)

            # 3. 신호 계산
            logger.info("신호 계산 시작")
            signals = calc_all_signals()

            # 4. JSON 저장 + git push
            save_and_push(signals)

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
