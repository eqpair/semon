import aiohttp
import asyncio
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from kis_auth import get_access_token

load_dotenv(Path(__file__).parent / ".env")

APP_KEY    = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
BASE_URL   = "https://openapi.koreainvestment.com:9443"
PRICE_URL  = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
OHLCV_URL  = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"

logger = logging.getLogger(__name__)

# KST 타임존 (utils.now_kst 의존성 제거)
KST = timezone(timedelta(hours=9))


def now_kst():
    return datetime.now(KST)


# ── 시장 코드 결정 ────────────────────────────────────────────
#
# KIS API의 fid_cond_mrkt_div_code 값:
#   J  = KRX (한국거래소)
#   NX = NXT (넥스트레이드)
#   UN = 통합 (KRX + NXT, best price)
#
# NXT 운영시간 (2025-03-04 오픈):
#   - 프리마켓:   08:00 ~ 08:50
#   - 메인마켓:   09:00 ~ 15:30 (KRX 정규장과 동시 운영)
#   - 애프터마켓: 15:30 ~ 20:00
#
# 정규장 시간에도 NXT가 동시에 돌아가므로,
# "J"만 호출하면 NXT의 더 빠른 가격/체결을 놓침 → 실시간성 손실.
# → 정규장+NXT 시간대에는 "UN" (통합)을 써서 두 시장 best price를 받아옴.

def get_market_code() -> str:
    """현재 시간에 따라 시장 코드 반환"""
    now = now_kst()
    h, m = now.hour, now.minute
    t = h * 60 + m

    # 08:00 ~ 20:00 → NXT 운영시간 → 통합 시세 (UN)
    if 8 * 60 <= t <= 20 * 60:
        return "UN"
    # 그 외 시간 (야간) → KRX 종가 기준
    return "J"


# ── 현재가 fetch ──────────────────────────────────────────────

async def _fetch_one_market(session, token, code, market_code):
    """특정 시장 코드로 1회 fetch. (price, volume, raw_output) 반환"""
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
            output = data.get("output", {}) or {}
            price  = output.get("stck_prpr")
            volume = output.get("acml_vol")
            sdpr   = output.get("stck_sdpr")  # 전일 정규장 확정 종가 (HTS 기준가)
            p = float(price) if price else None
            v = float(volume) if volume else None
            sd = float(sdpr) if sdpr else None
            # KIS는 종목 미상장/오류 시 price=0 또는 빈 문자열을 줌
            if p is not None and p <= 0:
                p = None
            if sd is not None and sd <= 0:
                sd = None
            return p, v, sd, data.get("rt_cd")
    except Exception as e:
        logger.warning(f"KIS 현재가 오류 ({code} mkt={market_code}): {e}")
        return None, None, None, None


async def _fetch_one(session, token, code, market_code):
    """
    종목 1개 현재가 fetch.
    market_code='UN'으로 먼저 시도하고, 응답이 비면 'J'로 폴백.
    """
    # 1차: 요청된 시장 코드 (보통 UN)
    price, volume, sdpr, rt_cd = await _fetch_one_market(session, token, code, market_code)
    if price is not None:
        return code, price, volume, sdpr

    # 2차 폴백: NXT 미상장 종목 등은 UN으로 안 잡힐 수 있음 → J로 재시도
    if market_code != "J":
        price2, volume2, sdpr2, _ = await _fetch_one_market(session, token, code, "J")
        if price2 is not None:
            return code, price2, volume2, sdpr2

    return code, None, None, None


async def fetch_all_prices_kis(code_list):
    token       = await get_access_token()
    market_code = get_market_code()
    logger.info(f"마켓 코드: {market_code} (시간: {now_kst().strftime('%H:%M')})")
    results    = {}
    chunk_size = 19
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk     = code_list[i:i + chunk_size]
            tasks     = [_fetch_one(session, token, code, market_code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, price, volume, sdpr in responses:
                results[code] = (price, volume, sdpr)
            if i + chunk_size < len(code_list):
                await asyncio.sleep(1.1)
    success = sum(1 for v in results.values() if v[0] is not None)
    fail = len(code_list) - success
    if fail > 0:
        failed_codes = [c for c, v in results.items() if v[0] is None][:5]
        logger.info(f"KIS 현재가 fetch: {success}/{len(code_list)}개 성공 (실패 샘플: {failed_codes})")
    else:
        logger.info(f"KIS 현재가 fetch: {success}/{len(code_list)}개 성공")
    return results


# ── 일봉 OHLCV fetch ──────────────────────────────────────────
# 일봉은 KRX 기준만 사용 (NXT는 KRX 종가와 동일하게 정산됨)

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
                "fid_cond_mrkt_div_code": "J",  # 일봉은 KRX 통합 정산
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
    """
    all_rows = []
    end = now_kst().replace(tzinfo=None)

    for _ in range(5):
        start     = end - timedelta(days=140)
        start_str = start.strftime("%Y%m%d")
        end_str   = end.strftime("%Y%m%d")

        rows = await _fetch_ohlcv_chunk(session, token, code, start_str, end_str)
        all_rows.extend(rows)

        end = start - timedelta(days=1)
        await asyncio.sleep(0.1)

    if not all_rows:
        return code, None

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
    """전체 종목 500일치 일봉 KIS fetch"""
    token   = await get_access_token()
    results = {}
    chunk_size = 3

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(code_list), chunk_size):
            chunk = code_list[i:i + chunk_size]
            tasks = [_fetch_ohlcv_500(session, token, code) for code in chunk]
            responses = await asyncio.gather(*tasks)
            for code, ohlcv in responses:
                results[code] = ohlcv
            if i + chunk_size < len(code_list):
                await asyncio.sleep(1.0)

    success = sum(1 for v in results.values() if v is not None)
    logger.info(f"KIS OHLCV fetch: {success}/{len(code_list)}개 성공")
    return results