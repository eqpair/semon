import aiohttp
import asyncio
import logging
from bs4 import BeautifulSoup
from bs4 import XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logger = logging.getLogger(__name__)

# 현재가 API
PRICE_URL = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{code}"

# 일별 시세 XML (당일 거래량 포함) - 기존 pairs.py에서 검증된 URL
SISE_URL = (
    "https://fchart.stock.naver.com/sise.nhn"
    "?symbol={code}&timeframe=day&count=21&requestType=0"
)


async def fetch_price(session: aiohttp.ClientSession, code: str) -> tuple[str, float | None]:
    """현재가 fetch"""
    url = PRICE_URL.format(code=code)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return code, None
            data = await resp.json(content_type=None)
            areas = data.get("result", {}).get("areas", [])
            if not areas or not areas[0].get("datas"):
                return code, None
            price = areas[0]["datas"][0].get("nv")
            return code, float(price) if price is not None else None
    except Exception as e:
        logger.warning(f"현재가 오류 ({code}): {e}")
        return code, None


async def fetch_volume(session: aiohttp.ClientSession, code: str) -> tuple[str, list[float] | None]:
    """
    최근 21일치 거래량 fetch (당일 포함)
    반환: (code, [vol_20일전, ..., vol_오늘]) 또는 (code, None)
    """
    url = SISE_URL.format(code=code)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return code, None
            text = await resp.text()

        soup = BeautifulSoup(text, "xml")
        items = soup.find_all("item")
        if not items:
            return code, None

        volumes = []
        for item in items:
            raw = item.get("data", "")
            parts = raw.split("|")
            # data 형식: 날짜|시가|고가|저가|종가|거래량
            if len(parts) >= 6:
                try:
                    vol = float(parts[5])
                    volumes.append(vol)
                except ValueError:
                    continue

        return code, volumes if volumes else None

    except Exception as e:
        logger.warning(f"거래량 오류 ({code}): {e}")
        return code, None


async def fetch_all_prices(code_list: list[str]) -> dict[str, float | None]:
    """전체 종목 현재가 비동기 fetch"""
    chunk_size = 50
    results = {}

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk = code_list[i:i + chunk_size]
            tasks = [fetch_price(session, code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, price in responses:
                results[code] = price
            if i + chunk_size < len(code_list):
                await asyncio.sleep(0.3)

    success = sum(1 for v in results.values() if v is not None)
    logger.info(f"현재가 fetch: {success}/{len(code_list)}개 성공")
    return results


async def fetch_all_volumes(code_list: list[str]) -> dict[str, list[float] | None]:
    """
    전체 종목 거래량 이력 비동기 fetch
    장 시작 시 1회 + 이후 1시간마다 갱신으로 충분
    """
    chunk_size = 30  # 거래량은 더 보수적으로
    results = {}

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk = code_list[i:i + chunk_size]
            tasks = [fetch_volume(session, code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, vols in responses:
                results[code] = vols
            if i + chunk_size < len(code_list):
                await asyncio.sleep(0.5)

    success = sum(1 for v in results.values() if v is not None)
    logger.info(f"거래량 fetch: {success}/{len(code_list)}개 성공")
    return results
