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
    "reuters_world": "https://feeds.reuters.com/reuters/worldNews",
    "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
    "cnbc_world": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "cnbc_finance": "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    "yonhap": "https://www.yna.co.kr/rss/economy.xml",
    "hankyung": "https://www.hankyung.com/feed/economy",
}

# ── 매크로 지수 수집 ──────────────────────────────────────────

MACRO_TICKERS = {
    "S&P500":    "^GSPC",
    "NASDAQ":    "^IXIC",
    "DOW":       "^DJI",
    "VIX":       "^VIX",
    "달러인덱스": "DX-Y.NYB",
    "국채10Y":   "^TNX",
    "금":        "GLD",
    "WTI유가":   "USO",
    "니케이":    "^N225",
    "항셍":      "^HSI",
    "원달러":    "KRW=X",
    "구리":      "HG=F",
}

def fetch_macro() -> dict:
    result = {}
    try:
        tickers = yf.Tickers(" ".join(MACRO_TICKERS.values()))
        for name, symbol in MACRO_TICKERS.items():
            try:
                hist = tickers.tickers[symbol].history(period="2d")
                if len(hist) >= 2:
                    prev  = hist["Close"].iloc[-2]
                    curr  = hist["Close"].iloc[-1]
                    chg   = (curr - prev) / prev * 100
                    result[name] = {
                        "price":  round(float(curr), 2),
                        "change": round(float(chg), 2),
                    }
            except Exception as e:
                logger.warning(f"매크로 fetch 실패 ({name}): {e}")
    except Exception as e:
        logger.error(f"매크로 전체 fetch 실패: {e}")
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
    for article in articles[:15]:
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
