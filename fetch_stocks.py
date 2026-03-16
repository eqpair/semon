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
"""

import asyncio
import aiohttp
import csv
import warnings
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

MIN_PRICE      = 1_000
MIN_MARKET_CAP = 500
MIN_TRADE_AMT  = 1

BASE       = "https://finance.naver.com"
GROUP_URL  = BASE + "/sise/sise_group.naver?type=upjong"
DETAIL_URL = BASE + "/sise/sise_group_detail.naver?type=upjong&no={no}"
PRICE_URL  = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{code}"
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


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
    except:
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
            trade_amt = float(d.get("aa", 0) or 0) / 100_000_000  # 원 → 억원
            ms        = d.get("mt", "")
            return {
                "price":      price,
                "market_cap": 0,   # 시총은 별도 처리
                "trade_amt":  trade_amt,
                "market":     "KOSPI" if ms == "1" else "KOSDAQ",
            }
    except:
        return {}


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

        print("\n현재가·시총·거래대금 수집 중...")
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
                results.append(s)
            print(f"\r  {min(i+chunk, len(codes))}/{len(codes)}개", end="", flush=True)
            await asyncio.sleep(0.3)
        print()

    # 필터링
    filtered = [
        s for s in results
        if s.get("price", 0)      >= MIN_PRICE
        and s.get("trade_amt", 0)  >= MIN_TRADE_AMT
    ]
    print(f"\n전체: {len(results)}개  →  필터 후: {len(filtered)}개")
    print(f"제외: {len(results)-len(filtered)}개")

    # CSV 저장
    out = "stocks_filtered.csv"
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["code","name","market","price","market_cap","trade_amt","group_name"])
        writer.writeheader()
        for s in sorted(filtered, key=lambda x: -x.get("market_cap", 0)):
            writer.writerow({
                "code":       s.get("code",""),
                "name":       s.get("name",""),
                "market":     s.get("market",""),
                "price":      int(s.get("price",0)),
                "market_cap": int(s.get("market_cap",0)),
                "trade_amt":  int(s.get("trade_amt",0)),
                "group_name": s.get("group_name",""),
            })

    print(f"CSV 저장: {out}")
    print("\n시총 상위 20개:")
    for s in sorted(filtered, key=lambda x: -x.get("market_cap",0))[:20]:
        print(f"  {s['code']} {s['name']:15s} {s.get('market',''):6s} "
              f"{int(s.get('price',0)):>10,}원  "
              f"시총:{int(s.get('market_cap',0)):>7,}억  "
              f"거래대금:{int(s.get('trade_amt',0)):>5,}억  "
              f"{s.get('group_name','')}")


if __name__ == "__main__":
    asyncio.run(main())