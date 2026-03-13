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

# 일별 시세 XML — 62일치 (60일 수익률 계산용)
SISE_URL = (
    "https://fchart.stock.naver.com/sise.nhn"
    "?symbol={code}&timeframe=day&count=62&requestType=0"
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


async def fetch_ohlcv(
    session: aiohttp.ClientSession, code: str
) -> tuple[str, list[float] | None, list[float] | None]:
    """
    최근 62일치 일봉 fetch
    반환: (code, closes, volumes)  — 오래된 순 → 최신 순
    data 형식: 날짜|시가|고가|저가|종가|거래량
    """
    url = SISE_URL.format(code=code)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return code, None, None
            text = await resp.text(encoding="euc-kr")

        soup = BeautifulSoup(text, "html.parser")
        items = soup.find_all("item")
        if not items:
            return code, None, None

        closes, volumes = [], []
        for item in items:
            raw = item.get("data", "")
            parts = raw.split("|")
            if len(parts) >= 6:
                try:
                    closes.append(float(parts[4]))   # 종가
                    volumes.append(float(parts[5]))  # 거래량
                except ValueError:
                    continue

        if not closes:
            return code, None, None

        return code, closes, volumes

    except Exception as e:
        logger.warning(f"OHLCV 오류 ({code}): {e.__class__.__name__}: {e}")
        return code, None, None


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


async def fetch_all_ohlcv(code_list: list[str]) -> dict[str, dict | None]:
    """
    전체 종목 일봉(종가+거래량) 비동기 fetch
    반환: {code: {"closes": [...], "volumes": [...]}} or {code: None}
    """
    chunk_size = 10
    results = {}

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk = code_list[i:i + chunk_size]
            tasks = [fetch_ohlcv(session, code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, closes, volumes in responses:
                if closes and volumes:
                    results[code] = {"closes": closes, "volumes": volumes}
                else:
                    results[code] = None
            if i + chunk_size < len(code_list):
                await asyncio.sleep(1.0)

    success = sum(1 for v in results.values() if v is not None)
    logger.info(f"OHLCV fetch: {success}/{len(code_list)}개 성공")
    return results
