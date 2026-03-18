"""
fetch_stocks.py
---------------
네이버 증권 업종별 전체 종목 수집 후 필터링

필터 기준:
  - 주가 1,000원 미만 제외
  - 시가총액 500억 미만 제외
  - 일평균 거래대금 1억 미만 제외

실행: python3 fetch_stocks.py
결과: stocks_filtered.csv

시총 출처:
  https://m.stock.naver.com/api/stock/{code}/basic
  → marketValue 필드 (억원 단위)
  폴링 API(polling.finance.naver.com)에는 시총 필드가 없어
  별도 API를 병렬로 호출한다.
"""

import asyncio
import aiohttp
import csv
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

MIN_PRICE      = 1_000   # 원
MIN_MARKET_CAP = 500     # 억원
MIN_TRADE_AMT  = 1       # 억원

BASE        = "https://finance.naver.com"
GROUP_URL   = BASE + "/sise/sise_group.naver?type=upjong"
DETAIL_URL  = BASE + "/sise/sise_group_detail.naver?type=upjong&no={no}"
PRICE_URL   = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{code}"
MSTOCK_URL  = "https://m.stock.naver.com/api/stock/{code}/basic"
HEADERS     = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
M_HEADERS   = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 11; SM-G991B) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Mobile Safari/537.36",
    "Referer": "https://m.stock.naver.com/",
}


async def fetch_group_nos(session):
    async with session.get(GROUP_URL, headers=HEADERS) as resp:
        html = await resp.text(encoding="euc-kr")
    soup   = BeautifulSoup(html, "html.parser")
    links  = soup.find_all("a", href=lambda h: h and "upjong" in h and "no=" in h)
    result = []
    for l in links:
        no   = l["href"].split("no=")[-1]
        name = l.text.strip()
        if no and name:
            result.append((no, name))
    return result


async def fetch_group_stocks(session, no, group_name):
    url = DETAIL_URL.format(no=no)
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            html = await resp.text(encoding="euc-kr")
    except Exception:
        return []
    soup   = BeautifulSoup(html, "html.parser")
    stocks = []
    for a in soup.find_all("a", href=lambda h: h and "item/main" in str(h)):
        href = a.get("href", "")
        code = href.split("code=")[-1].strip() if "code=" in href else ""
        name = a.text.strip()
        if code and len(code) == 6 and name:
            stocks.append({"code": code, "name": name, "group_name": group_name})
    return stocks


async def fetch_price_info(session, code):
    """폴링 API: 현재가 + 거래대금 + 시장구분"""
    url = PRICE_URL.format(code=code)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return {}
            data  = await resp.json(content_type=None)
            areas = data.get("result", {}).get("areas", [])
            if not areas or not areas[0].get("datas"):
                return {}
            d         = areas[0]["datas"][0]
            price     = float(d.get("nv", 0) or 0)
            # aa: 당일 누적 거래대금(원). 장중에는 누적치이므로
            # 전일 거래대금이 필요하면 별도 일봉 API를 써야 하나,
            # 필터 목적(유동성 최소 기준)으로는 당일 누적치로 충분하다.
            trade_amt = float(d.get("aa", 0) or 0) / 100_000_000  # 원 → 억원
            ms        = d.get("mt", "")
            return {
                "price":    price,
                "trade_amt": trade_amt,
                "market":   "KOSPI" if ms == "1" else "KOSDAQ",
            }
    except Exception:
        return {}


async def fetch_market_cap(session, code) -> int:
    """
    모바일 API: 시가총액 (억원)
    m.stock.naver.com/api/stock/{code}/basic
    → marketValue: 억원 단위 정수
    폴링 API에 시총 필드가 없어 별도 호출한다.
    """
    url = MSTOCK_URL.format(code=code)
    try:
        async with session.get(url, headers=M_HEADERS,
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return 0
            data = await resp.json(content_type=None)
            # marketValue 는 억원 단위 문자열 또는 정수로 반환됨
            val = data.get("marketValue") or data.get("marketCap") or 0
            return int(str(val).replace(",", "") or 0)
    except Exception:
        return 0


async def main():
    async with aiohttp.ClientSession() as session:

        print("업종 목록 수집 중...")
        groups = await fetch_group_nos(session)
        print(f"총 {len(groups)}개 업종")

        print("\n종목 수집 중...")
        all_stocks = {}
        for i, (no, name) in enumerate(groups):
            stocks = await fetch_group_stocks(session, no, name)
            for s in stocks:
                if s["code"] not in all_stocks:
                    all_stocks[s["code"]] = s
            print(f"\r  {i+1}/{len(groups)} 업종 완료 ({len(all_stocks)}개 종목)", end="", flush=True)
            await asyncio.sleep(0.2)
        print()
        print(f"전체 종목: {len(all_stocks)}개")

        # ── 현재가 + 거래대금 수집 ────────────────────────────
        print("\n현재가·거래대금 수집 중...")
        codes   = list(all_stocks.keys())
        results = []
        chunk   = 50
        for i in range(0, len(codes), chunk):
            batch = codes[i:i+chunk]
            tasks = [fetch_price_info(session, c) for c in batch]
            infos = await asyncio.gather(*tasks)
            for code, info in zip(batch, infos):
                s = all_stocks[code].copy()
                s.update(info)
                s.setdefault("market_cap", 0)  # 아직 0, 아래에서 채움
                results.append(s)
            print(f"\r  {min(i+chunk, len(codes))}/{len(codes)}개", end="", flush=True)
            await asyncio.sleep(0.3)
        print()

        # ── 1차 필터: 주가·거래대금 (시총 fetch 대상을 줄이기 위해 먼저 적용) ──
        pre_filtered = [
            s for s in results
            if s.get("price", 0)     >= MIN_PRICE
            and s.get("trade_amt", 0) >= MIN_TRADE_AMT
        ]
        print(f"1차 필터(주가·거래대금) 후: {len(pre_filtered)}개 (시총 수집 대상)")

        # ── 시가총액 수집 (1차 통과 종목만) ──────────────────
        print("\n시가총액 수집 중...")
        cap_chunk = 20  # 모바일 API는 더 보수적으로
        for i in range(0, len(pre_filtered), cap_chunk):
            batch = pre_filtered[i:i+cap_chunk]
            tasks = [fetch_market_cap(session, s["code"]) for s in batch]
            caps  = await asyncio.gather(*tasks)
            for s, cap in zip(batch, caps):
                s["market_cap"] = cap
            print(f"\r  {min(i+cap_chunk, len(pre_filtered))}/{len(pre_filtered)}개", end="", flush=True)
            await asyncio.sleep(0.5)
        print()

    # ── 2차 필터: 시총 ────────────────────────────────────────
    filtered = [
        s for s in pre_filtered
        if s.get("market_cap", 0) >= MIN_MARKET_CAP
    ]
    cap_zero = sum(1 for s in pre_filtered if s.get("market_cap", 0) == 0)
    print(f"\n1차 통과: {len(pre_filtered)}개")
    print(f"  시총 0 (API 실패): {cap_zero}개 — 필터에서 제외됨")
    print(f"  시총 {MIN_MARKET_CAP}억 미만 제외 후: {len(filtered)}개")
    print(f"\n전체 원본: {len(results)}개  →  최종 필터 후: {len(filtered)}개")

    # ── CSV 저장 ──────────────────────────────────────────────
    out = "stocks_filtered.csv"
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["code","name","market","price","market_cap","trade_amt","group_name"])
        writer.writeheader()
        for s in sorted(filtered, key=lambda x: -x.get("market_cap", 0)):
            writer.writerow({
                "code":       s.get("code", ""),
                "name":       s.get("name", ""),
                "market":     s.get("market", ""),
                "price":      int(s.get("price", 0)),
                "market_cap": int(s.get("market_cap", 0)),
                "trade_amt":  round(s.get("trade_amt", 0), 1),
                "group_name": s.get("group_name", ""),
            })

    print(f"\nCSV 저장: {out}")
    print("\n시총 상위 20개:")
    for s in sorted(filtered, key=lambda x: -x.get("market_cap", 0))[:20]:
        print(f"  {s['code']} {s['name']:15s} {s.get('market',''):6s} "
              f"{int(s.get('price', 0)):>10,}원  "
              f"시총:{int(s.get('market_cap', 0)):>7,}억  "
              f"거래대금:{s.get('trade_amt', 0):>6.1f}억  "
              f"{s.get('group_name', '')}")


if __name__ == "__main__":
    asyncio.run(main())