import feedparser
import trafilatura
import yfinance as yf
import json
import logging
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)
KST = ZoneInfo("Asia/Seoul")

# ── RSS 소스 정의 ─────────────────────────────────────────────

RSS_SOURCES = {
    # 글로벌
    "reuters_world":    "https://feeds.reuters.com/reuters/worldNews",
    "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
    "cnbc_world":       "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "cnbc_finance":     "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "bloomberg":        "https://feeds.bloomberg.com/markets/news.rss",
    "ft":               "https://www.ft.com/rss/home",
    "wsj_markets":      "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "investing_com":    "https://www.investing.com/rss/news.rss",
    "marketwatch":      "https://feeds.marketwatch.com/marketwatch/topstories/",
    # 국내
    "yonhap":           "https://www.yna.co.kr/rss/economy.xml",
    "hankyung":         "https://www.hankyung.com/feed/economy",
    "maeil":            "https://www.mk.co.kr/rss/40300001/",
}

# ── 매크로 지수 수집 ──────────────────────────────────────────
#
# 수정 이력:
#   금    GLD  → GC=F  (COMEX 금 선물 — 실제 $/oz 반영)
#   WTI   USO  → CL=F  (NYMEX WTI 선물 — 실제 $/bbl 반영)
#   GLD/USO는 ETF로 실제 가격과 큰 괴리 존재:
#     GLD ≈ 금값 × 0.093 (1/10oz 기준), USO ≈ 선물 롤오버 ETF
#
# 추가 지표:
#   필라델피아반도체  ^SOX   (Philadelphia Semiconductor Index)
#   러셀2000   ^RUT   (Russell 2000 Small Cap)
#   영국FTSE   ^FTSE  (FTSE 100)
#   독일DAX    ^GDAXI (DAX)

MACRO_TICKERS = {
    # 미국 주요 지수
    "S&P500":    "^GSPC",
    "NASDAQ":    "^IXIC",
    "DOW":       "^DJI",
    "VIX":       "^VIX",
    "필라델피아반도체": "^SOX",    # 추가: 필라델피아 반도체지수
    "러셀2000":  "^RUT",    # 추가: Russell 2000

    # 글로벌 지수
    "영국FTSE":  "^FTSE",   # 추가: FTSE 100
    "독일DAX":   "^GDAXI",  # 추가: DAX
    "니케이":    "^N225",   # 추가: 일본 닛케이225
    "항셍":      "^HSI",    # 추가: 홍콩 항셍

    # 채권 / 환율 / 원자재
    "달러인덱스": "DX-Y.NYB",
    "국채10Y":   "^TNX",
    "금":        "GC=F",    # 수정: COMEX 금 선물 ($/oz)  ← GLD에서 변경
    "WTI유가":   "CL=F",    # 수정: NYMEX WTI 선물 ($/bbl) ← USO에서 변경
    "원달러":    "KRW=X",
    "구리":      "HG=F",
}

def fetch_macro() -> dict:
    result = {}
    for name, symbol in MACRO_TICKERS.items():
        try:
            hist = yf.Ticker(symbol).history(period="5d")
            if len(hist) >= 2:
                prev  = hist["Close"].iloc[-2]
                curr  = hist["Close"].iloc[-1]
                chg   = (curr - prev) / prev * 100
                result[name] = {
                    "price":  round(float(curr), 2),
                    "change": round(float(chg), 2),
                }
            else:
                logger.warning(f"매크로 데이터 부족 ({name}): {len(hist)}일치")
        except Exception as e:
            logger.warning(f"매크로 fetch 실패 ({name}): {e}")
    return result

# ── RSS 수집 ─────────────────────────────────────────────────

def fetch_rss(hours: int = 12) -> list[dict]:
    """최근 N시간 이내 기사만 수집"""
    cutoff = datetime.now(KST) - timedelta(hours=hours)
    articles = []
    seen_titles = set()

    for source, url in RSS_SOURCES.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:
                title = entry.get("title", "").strip()
                link  = entry.get("link", "").strip()

                if not title or not link:
                    continue
                if title in seen_titles:
                    continue

                seen_titles.add(title)
                articles.append({
                    "source": source,
                    "title":  title,
                    "link":   link,
                    "published": entry.get("published", ""),
                })
        except Exception as e:
            logger.warning(f"RSS fetch 실패 ({source}): {e}")

    logger.info(f"RSS 수집 완료: {len(articles)}개")
    return articles

# ── 본문 스크래핑 ─────────────────────────────────────────────

def fetch_article_body(url: str, max_chars: int = 1500) -> str:
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ""
        text = trafilatura.extract(downloaded, include_comments=False,
                                   include_tables=False)
        if not text:
            return ""
        return text[:max_chars]
    except Exception as e:
        logger.warning(f"본문 스크래핑 실패 ({url}): {e}")
        return ""

# ── 뉴스 + 매크로 통합 수집 ───────────────────────────────────

def collect_all(hours: int = 12) -> dict:
    logger.info("EQAI 뉴스 수집 시작")

    macro    = fetch_macro()
    articles = fetch_rss(hours=hours)

    # 본문 수집 (상위 15개만)
    for article in articles[:20]:
        body = fetch_article_body(article["link"])
        article["body"] = body

    result = {
        "collected_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "macro":        macro,
        "articles":     articles,
    }

    logger.info(f"수집 완료 — 매크로: {len(macro)}개, 기사: {len(articles)}개")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = collect_all(hours=12)
    print(json.dumps(data, ensure_ascii=False, indent=2)[:2000])