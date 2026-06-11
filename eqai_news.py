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
    "hankyung_finance": "https://www.hankyung.com/feed/finance",
    "maeil":            "https://www.mk.co.kr/rss/40300001/",
    "fnnews":           "https://www.fnnews.com/rss/r20/fn_realnews_economy.xml",
    # 글로벌 추가
    "yahoo_finance":    "https://finance.yahoo.com/news/rssindex",
    "seeking_alpha":    "https://seekingalpha.com/market_currents.xml",
    "cnbc_tech":        "https://www.cnbc.com/id/19854910/device/rss/rss.html",
    "benzinga":         "https://www.benzinga.com/feed",
    "investors":        "https://www.investors.com/feed/",
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
    "국채30Y":   "^TYX",
    "코스피":     "^KS11",
    "코스닥":     "^KQ11",
    "금":        "GC=F",    # 수정: COMEX 금 선물 ($/oz)  ← GLD에서 변경
    "WTI유가":   "CL=F",    # 수정: NYMEX WTI 선물 ($/bbl) ← USO에서 변경
    "원달러":    "KRW=X",
    "구리":      "HG=F",
}

# ── 한국 지수: KIS 업종지수 API (yfinance ^KS11/^KQ11 데이터 오염 대응) ──
#
# yfinance는 한국 지수의 일봉 누락(06-10 누락 사례)과 오염된 봉이 빈번해
# "iloc[-2] = 전일 종가" 가정이 깨짐. 코스피/코스닥은 KIS 공식 시세로 대체.
#
# inquire-index-price (tr_id FHPUP02100000) 응답:
#   bstp_nmix_prpr      현재지수
#   bstp_nmix_prdy_ctrt 전일대비율(%) — 부호 포함, 거래소 공식 기준가 기반

KIS_INDEX_URL = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-index-price"
KIS_INDEX_CODES = {"코스피": "0001", "코스닥": "1001"}


def fetch_kr_index_kis() -> dict:
    """코스피/코스닥 현재지수·전일대비율을 KIS API로 조회. 실패 시 빈 dict."""
    import os
    import asyncio
    from pathlib import Path
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent / ".env")
    result = {}
    try:
        from kis_auth import get_access_token
        token = asyncio.run(get_access_token())
        headers = {
            "authorization": f"Bearer {token}",
            "appkey":        os.getenv("KIS_APP_KEY"),
            "appsecret":     os.getenv("KIS_APP_SECRET"),
            "tr_id":         "FHPUP02100000",
        }
        for name, iscd in KIS_INDEX_CODES.items():
            try:
                r = requests.get(
                    KIS_INDEX_URL,
                    headers=headers,
                    params={
                        "FID_COND_MRKT_DIV_CODE": "U",
                        "FID_INPUT_ISCD":         iscd,
                    },
                    timeout=5,
                )
                out = r.json().get("output", {}) or {}
                price = float(out.get("bstp_nmix_prpr") or 0)
                ctrt  = float(out.get("bstp_nmix_prdy_ctrt") or 0)
                if price > 0:
                    result[name] = {"price": round(price, 2), "change": round(ctrt, 2)}
            except Exception as e:
                logger.warning(f"KIS 지수 조회 실패 ({name}): {e}")
    except Exception as e:
        logger.warning(f"KIS 지수 조회 불가 (토큰/모듈): {e}")
    return result


def fetch_macro() -> dict:
    result = {}
    kis_kr = fetch_kr_index_kis()   # 코스피/코스닥은 KIS 공식 시세 우선
    for name, symbol in MACRO_TICKERS.items():
        if name in kis_kr:
            result[name] = kis_kr[name]
            continue
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
    """최근 N시간 이내 기사 수집 + 시간순 정렬 + 중요도 우선순위 부여"""
    from email.utils import parsedate_to_datetime

    # 빅네임 키워드 (높을수록 가중치 큼)
    PRIORITY_KEYWORDS = {
        # 글로벌 빅네임 — 가중치 3
        "nvidia": 3, "엔비디아": 3, "nvda": 3,
        "micron": 3, "마이크론": 3, "mu ": 3,
        "tsmc": 3, "tsm ": 3,
        "삼성전자": 3, "sk하이닉스": 3, "hynix": 3,
        "apple": 3, "애플": 3, "aapl": 3,
        "tesla": 3, "테슬라": 3, "tsla": 3,
        "openai": 3, "오픈ai": 3, "오픈에이아이": 3,
        "amd ": 3, "broadcom": 3, "avgo": 3,
        "google": 3, "구글": 3, "alphabet": 3,
        "microsoft": 3, "msft": 3, "마이크로소프트": 3,
        "meta": 3, "amazon": 3, "amzn": 3,
        "spacex": 3, "스페이스x": 3,
        # 매크로 키워드 — 가중치 2
        "fed ": 2, "fomc": 2, "powell": 2, "연준": 2, "파월": 2,
        "ecb": 2, "boj": 2, "한국은행": 2,
        "cpi": 2, "ppi": 2, "인플레": 2, "고용지표": 2,
        "금리": 2, "interest rate": 2,
        # 시그널성 키워드 — 가중치 2
        "price target": 2, "목표가": 2, "tp ": 2,
        "upgrade": 2, "downgrade": 2, "상향": 2, "하향": 2,
        "earnings": 2, "실적": 2, "어닝": 2,
        # 기타 중요 — 가중치 1
        "ai ": 1, "chip": 1, "반도체": 1,
    }

    # 한글 RSS 소스는 paywall 없음 → 본문 확보 확률 100% → 가중치 보너스
    KOREAN_SOURCES = {"yonhap", "hankyung", "hankyung_finance", "maeil", "fnnews"}

    def score(title: str, source: str = "") -> int:
        t = title.lower()
        base = sum(w for k, w in PRIORITY_KEYWORDS.items() if k in t)
        # 한글 소스 보너스: 빅네임이 언급되면 +2, 일반은 +0
        # (한글 소스라도 무관 뉴스가 위로 오는 건 막기 위해 빅네임 매칭시에만 보너스)
        if source in KOREAN_SOURCES and base >= 2:
            base += 2
        return base

    def parse_pub(s: str):
        if not s:
            return datetime.min.replace(tzinfo=KST)
        try:
            dt = parsedate_to_datetime(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            return dt.astimezone(KST)
        except Exception:
            return datetime.min.replace(tzinfo=KST)

    cutoff = datetime.now(KST) - timedelta(hours=hours)
    articles = []
    seen_titles = set()

    for source, url in RSS_SOURCES.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:30]:  # 소스당 30개로 확대
                title = entry.get("title", "").strip()
                link  = entry.get("link", "").strip()
                if not title or not link:
                    continue
                if title in seen_titles:
                    continue
                seen_titles.add(title)
                pub_str = entry.get("published", "")
                articles.append({
                    "source":    source,
                    "title":     title,
                    "link":      link,
                    "published": pub_str,
                    "_pub_dt":   parse_pub(pub_str),
                    "_score":    score(title, source),
                })
        except Exception as e:
            logger.warning(f"RSS fetch 실패 ({source}): {e}")

    # ── 시간 필터: cutoff 이전 기사 제거 ────────────────────
    # parse 실패한 기사(datetime.min)는 자동으로 컷오프 미달로 제거됨
    before = len(articles)
    articles = [a for a in articles if a["_pub_dt"] >= cutoff]
    filtered = before - len(articles)
    logger.info(f"시간 필터: {before}개 → {len(articles)}개 ({filtered}개 제거, cutoff={cutoff.strftime('%Y-%m-%d %H:%M')})")

    # 정렬: 점수 내림차순 → 최신순
    articles.sort(key=lambda a: (-a["_score"], -a["_pub_dt"].timestamp()))

    # 내부 필드 제거
    for a in articles:
        a.pop("_pub_dt", None)
        a.pop("_score", None)

    logger.info(f"RSS 수집 완료: {len(articles)}개 (점수+최신순 정렬, 최근 {(datetime.now(KST) - cutoff).total_seconds() / 3600:.0f}시간)")
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

    # 본문 수집: 상위 30개 후보 중 실제로 본문 확보된 20개까지 (실패 시 다음 후보)
    BODY_TARGET = 20   # 목표 본문 확보 개수
    BODY_MAX_TRY = 30  # 최대 시도 개수
    success_count = 0
    fail_count = 0
    for i, article in enumerate(articles[:BODY_MAX_TRY]):
        if success_count >= BODY_TARGET:
            break
        body = fetch_article_body(article["link"])
        article["body"] = body
        if body and len(body) >= 100:  # 100자 이상이면 성공 간주
            success_count += 1
        else:
            fail_count += 1
    logger.info(f"본문 수집: 성공 {success_count}개 / 실패 {fail_count}개 (총 {success_count+fail_count}개 시도)")

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