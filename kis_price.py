import aiohttp
import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from kis_auth import get_access_token

load_dotenv(Path(__file__).parent / ".env")

APP_KEY    = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
BASE_URL   = "https://openapi.koreainvestment.com:9443"
PRICE_URL  = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"

logger = logging.getLogger(__name__)


def get_market_code():
    """현재 시간에 따라 마켓 코드 반환"""
    now = datetime.now()
    h, m = now.hour, now.minute
    t = h * 60 + m
    # 09:00 ~ 15:30 → KRX 정규장
    if 9 * 60 <= t <= 15 * 60 + 30:
        return "J"
    # 08:00 ~ 08:59 또는 15:30 ~ 20:00 → NXT
    elif (8 * 60 <= t < 9 * 60) or (15 * 60 + 30 < t <= 20 * 60):
        return "NX"
    # 그 외 시간 → KRX (종가 기준)
    else:
        return "J"


async def _fetch_one(session, token, code, market_code):
    try:
        async with session.get(
            PRICE_URL,
            headers={
                "authorization": f"Bearer {token}",
                "appkey":        APP_KEY,
                "appsecret":     APP_SECRET,
                "tr_id":         "FHKST01010100",
            },
            params={
                "fid_cond_mrkt_div_code": market_code,
                "fid_input_iscd":         code,
            },
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            data   = await resp.json(content_type=None)
            output = data.get("output", {})
            price  = output.get("stck_prpr")
            volume = output.get("acml_vol")
            return code, float(price) if price else None, float(volume) if volume else None
    except Exception as e:
        logger.warning(f"KIS 현재가 오류 ({code}): {e}")
        return code, None, None


async def fetch_all_prices_kis(code_list):
    token       = await get_access_token()
    market_code = get_market_code()
    logger.info(f"마켓 코드: {market_code}")
    results    = {}
    chunk_size = 19
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk     = code_list[i:i + chunk_size]
            tasks     = [_fetch_one(session, token, code, market_code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, price, volume in responses:
                results[code] = (price, volume)
            if i + chunk_size < len(code_list):
                await asyncio.sleep(1.1)
    success = sum(1 for v in results.values() if v[0] is not None)
    logger.info(f"KIS 현재가 fetch: {success}/{len(code_list)}개 성공")
    return results