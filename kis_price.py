import aiohttp
import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kis_auth import get_access_token

load_dotenv(Path(__file__).parent / ".env")

APP_KEY    = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
BASE_URL   = "https://openapi.koreainvestment.com:9443"
PRICE_URL  = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
OHLCV_URL  = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"

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


# ── 현재가 fetch ──────────────────────────────────────────────

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


# ── 일봉 OHLCV fetch ──────────────────────────────────────────

async def _fetch_ohlcv_chunk(session, token, code, start_dt: str, end_dt: str) -> list[dict]:
    """
    KIS 일봉 API 1회 호출 — 최대 100일치
    start_dt, end_dt: "YYYYMMDD"
    반환: [{"date": "YYYYMMDD", "close": float, "volume": float}, ...]  오래된 순
    """
    try:
        async with session.get(
            OHLCV_URL,
            headers={
                "authorization": f"Bearer {token}",
                "appkey":        APP_KEY,
                "appsecret":     APP_SECRET,
                "tr_id":         "FHKST03010100",
            },
            params={
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd":         code,
                "fid_input_date_1":       start_dt,
                "fid_input_date_2":       end_dt,
                "fid_period_div_code":    "D",  # 일봉
                "fid_org_adj_prc":        "0",  # 수정주가
            },
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json(content_type=None)

        output2 = data.get("output2", [])
        if not output2:
            return []

        result = []
        for row in output2:
            date  = row.get("stck_bsop_date", "")
            close = row.get("stck_clpr", "")
            vol   = row.get("acml_vol", "")
            if not date or not close:
                continue
            try:
                c = float(close)
                v = float(vol) if vol else 0.0
                if c > 0:          # 0이면 거래정지 — 제외
                    result.append({"date": date, "close": c, "volume": v})
            except ValueError:
                continue

        # KIS는 최신 순으로 내려줌 → 뒤집어서 오래된 순으로
        result.reverse()
        return result

    except Exception as e:
        logger.warning(f"KIS 일봉 오류 ({code} {start_dt}~{end_dt}): {e}")
        return []


async def _fetch_ohlcv_500(session, token, code) -> tuple[str, dict | None]:
    """
    종목 1개의 500거래일치 일봉 fetch
    KIS는 1회 최대 100거래일 → end를 뒤로 당기며 5번 호출

    날짜 계산 방식:
      - 달력일 140일 ≈ 거래일 100일 (주말+공휴일 약 40일 제외)
      - end를 140일씩 뒤로 당기며 구간 생성 → 겹침/누락 없음
      - 중복 제거 + 날짜 정렬로 최종 정합성 보장
    반환: (code, {"closes": [...], "volumes": [...]})  오래된 순
    """
    all_rows = []
    end = datetime.now()

    for _ in range(5):
        start     = end - timedelta(days=140)   # 달력 140일 ≈ 거래일 100일
        start_str = start.strftime("%Y%m%d")
        end_str   = end.strftime("%Y%m%d")

        rows = await _fetch_ohlcv_chunk(session, token, code, start_str, end_str)
        all_rows.extend(rows)

        end = start - timedelta(days=1)         # 다음 구간 끝 = 이번 구간 시작 하루 전
        await asyncio.sleep(0.1)

    if not all_rows:
        return code, None

    # 날짜 중복 제거 후 오래된 순 정렬
    seen  = set()
    dedup = []
    for row in all_rows:
        if row["date"] not in seen:
            seen.add(row["date"])
            dedup.append(row)
    dedup.sort(key=lambda x: x["date"])

    closes  = [r["close"]  for r in dedup]
    volumes = [r["volume"] for r in dedup]

    return code, {"closes": closes, "volumes": volumes}


async def fetch_all_ohlcv_kis(code_list: list[str]) -> dict[str, dict | None]:
    """
    전체 종목 500일치 일봉 KIS fetch
    반환: {code: {"closes": [...], "volumes": [...]}} or {code: None}

    KIS 제한:
      - 초당 20건 → 종목당 5회 호출이므로 동시 실행 종목 수를 제한
      - chunk_size=3: 3종목 × 5회 = 15회/초 → 안전
    """
    token   = await get_access_token()
    results = {}
    chunk_size = 3   # 동시 처리 종목 수 (KIS rate limit 대응)

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk = code_list[i:i + chunk_size]
            tasks = [_fetch_ohlcv_500(session, token, code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, ohlcv in responses:
                results[code] = ohlcv
            if i + chunk_size < len(code_list):
                await asyncio.sleep(1.0)  # chunk 간 추가 대기

    success = sum(1 for v in results.values() if v is not None)
    logger.info(f"KIS OHLCV fetch: {success}/{len(code_list)}개 성공")
    return results