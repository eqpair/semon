#!/usr/bin/env python3
"""
eqai.py — EQAI 분석 파이프라인

흐름:
    1. 뉴스 + 매크로 수집 (eqai_news.py)
    2. S3에서 RRG 데이터 fetch
    3. Claude 3단계 분석 (API 키 설정 후 활성화)
    4. eqai_report.json 생성
    5. GitHub Pages push
"""

import asyncio
import json
import logging
import os
import sys
import boto3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv("/home/ubuntu/semon/.env")

# eqai_news 모듈 import
sys.path.insert(0, "/home/ubuntu/semon")
from eqai_news import collect_all

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/ubuntu/semon/eqai.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

KST           = ZoneInfo("Asia/Seoul")
S3_BUCKET     = "semon-eqai-data"
S3_KEY        = "rrg_latest.json"
REPORT_PATH   = Path("/home/ubuntu/semon/docs/data/eqai_report.json")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")


# ── S3에서 RRG 데이터 fetch ────────────────────────────────────

def fetch_rrg_from_s3() -> dict:
    try:
        s3  = boto3.client("s3", region_name="ap-northeast-2")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        data = json.loads(obj["Body"].read())
        logger.info(f"RRG 데이터 fetch 완료: {len(data.get('sector_rrg', {}))}개 섹터")
        return data
    except Exception as e:
        logger.error(f"RRG fetch 실패: {e}")
        return {}


# ── Claude 분석 (API 키 설정 후 활성화) ──────────────────────────

def _extract_json(text: str) -> dict:
    """Claude 응답에서 JSON 추출 — 코드블록 제거 후 첫번째 JSON 파싱"""
    import re
    # 코드블록 제거
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()
    # 첫번째 { 부터 매칭되는 } 까지만 추출
    start = text.find("{")
    if start == -1:
        raise ValueError(f"JSON 없음: {text[:200]}")
    depth = 0
    end = start
    for i, c in enumerate(text[start:], start):
        if c == "{": depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i
                break
    json_str = text[start:end+1]
    return json.loads(json_str)


def claude_analyze(news_data: dict, rrg_data: dict) -> dict:
    """
    Claude 3단계 분석
    API 키가 없으면 플레이스홀더 반환
    """
    if not os.getenv("BEDROCK_API_KEY", ""):
        logger.warning("ANTHROPIC_API_KEY 미설정 — 플레이스홀더 반환")
        return _placeholder_analysis(news_data, rrg_data)

    try:
        import boto3
        import re
        bedrock = boto3.client(
            service_name="bedrock-runtime",
            region_name="ap-northeast-2",
        )

        # ── 1차 호출: 뉴스 분석 ───────────────────────────────
        macro_text = _format_macro(news_data.get("macro", {}))
        articles_text = _format_articles(news_data.get("articles", []))

        prompt_1 = f"""한국주식 애널리스트. 마크다운없이 순수JSON만 출력. 모든 문자열값에 큰따옴표 사용금지.
매크로:{macro_text[:400]}
뉴스:{articles_text[:800]}
출력형식(이 구조 그대로):
{{"global_sentiment":"risk_on","sentiment_reason":"한줄요약","key_factors":["요인1","요인2","요인3"],"korea_market_impact":{{"summary":"한국증시전반영향한줄","positive_factors":["긍정요인1","긍정요인2"],"negative_factors":["부정요인1","부정요인2"],"key_watch":"오늘주목포인트"}},"sector_impact":{{"반도체":{{"direction":"positive","reason":"이유"}},"금융":{{"direction":"neutral","reason":"이유"}},"자동차":{{"direction":"neutral","reason":"이유"}},"2차전지":{{"direction":"neutral","reason":"이유"}},"조선":{{"direction":"neutral","reason":"이유"}}}},"news_summary":"뉴스1\\n뉴스2\\n뉴스3"}}"""

        resp_1 = bedrock.invoke_model(
            modelId="global.anthropic.claude-sonnet-4-6",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt_1}]
            })
        )
        body_1 = json.loads(resp_1["body"].read())
        analysis_1 = _extract_json(body_1["content"][0]["text"])
        logger.info("1차 분석 완료")

        # ── 2차 호출: RRG 교차 분석 ───────────────────────────
        rrg_text = _format_rrg(rrg_data)

        # 2차: RRG 교차 분석 — 텍스트로 받아서 구조화
        sector_rrg = rrg_data.get("sector_rrg", {})
        sentiment = analysis_1.get("global_sentiment", "neutral")

        strong_buy = []
        watch = []
        for sector, d in sector_rrg.items():
            quad = d.get("quadrant", "neutral")
            sector_impact = analysis_1.get("sector_impact", {})
            # 섹터 카테고리 매핑
            impact = "neutral"
            for key in sector_impact:
                if key in sector or sector in key:
                    impact = sector_impact[key].get("direction", "neutral")
                    break

            if quad in ("improving", "leading") and (impact == "positive" or sentiment == "risk_on"):
                strong_buy.append({
                    "sector": sector,
                    "reason": f"RRG {quad} + {sentiment} 환경 일치",
                    "rrg_quadrant": quad
                })
            elif quad == "improving":
                watch.append({
                    "sector": sector,
                    "reason": f"RRG {quad} 진입 — 뉴스 확인 필요"
                })

        strong_buy = strong_buy[:4]
        watch = watch[:3]

        analysis_2 = {
            "strong_buy": strong_buy,
            "watch": watch,
            "avoid": [],
            "summary": f"{sentiment} 환경 — {', '.join([s['sector'] for s in strong_buy[:2]])} 주목"
        }
        logger.info("2차 분석 완료")

        # ── 3차 호출: 종목 확정 ───────────────────────────────
        candidates_text = _format_candidates(rrg_data, analysis_2.get("strong_buy", []))

        prompt_3 = f"""당신은 한국 주식시장 전문 애널리스트입니다.

[진입 가능 섹터]
{json.dumps(analysis_2.get("strong_buy", []), ensure_ascii=False)}

[각 섹터의 prime/confirm 종목]
{candidates_text}

오늘 진입 가능한 최종 섹터와 종목을 선정하여 아래 JSON 형식으로만 응답하세요. 마크다운 코드블록 없이 순수 JSON만 출력하세요.

{{
  "top_picks": [
    {{
      "sector": "섹터명",
      "rrg_quadrant": "quadrant",
      "reason": "섹터 선정 근거",
      "stocks": [
        {{"code": "종목코드", "name": "종목명", "signal": "prime 또는 confirm", "reason": "종목 선정 근거"}}
      ]
    }}
  ],
  "caution_sectors": ["주의 섹터1", "주의 섹터2"]
}}"""

        resp_3 = bedrock.invoke_model(
            modelId="global.anthropic.claude-sonnet-4-6",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt_3}]
            })
        )
        body_3 = json.loads(resp_3["body"].read())
        analysis_3 = _extract_json(body_3["content"][0]["text"])
        logger.info("3차 분석 완료")

        return {
            **analysis_1,
            **analysis_2,
            **analysis_3,
        }

    except Exception as e:
        logger.error(f"Claude 분석 실패: {e}")
        return _placeholder_analysis(news_data, rrg_data)


# ── 플레이스홀더 (API 키 없을 때) ─────────────────────────────

def _placeholder_analysis(news_data: dict, rrg_data: dict) -> dict:
    """API 키 없을 때 RRG 데이터만으로 기본 분석 생성"""
    sector_rrg = rrg_data.get("sector_rrg", {})

    improving = [s for s, d in sector_rrg.items() if d.get("quadrant") == "improving"]
    leading   = [s for s, d in sector_rrg.items() if d.get("quadrant") == "leading"]

    strong_buy = [
        {"sector": s, "reason": "RRG Improving 진입 — 모멘텀 전환 신호",
         "rrg_quadrant": "improving"}
        for s in improving[:3]
    ]

    top_picks = []
    candidates = rrg_data.get("top_candidates", {})
    for item in strong_buy:
        sector = item["sector"]
        stocks = candidates.get(sector, [])[:3]
        if stocks:
            top_picks.append({
                "sector":       sector,
                "rrg_quadrant": "improving",
                "reason":       "RRG Improving + prime/confirm 신호 종목 보유",
                "stocks": [
                    {"code": s["code"], "name": s["name"],
                     "signal": s["signal"], "reason": "RRG prime/confirm 신호"}
                    for s in stocks
                ]
            })

    macro = news_data.get("macro", {})
    sp500 = macro.get("S&P500", {})
    vix   = macro.get("VIX", {})

    sentiment = "neutral"
    if sp500.get("change", 0) > 0.5 and (vix.get("change", 0) or 0) < 0:
        sentiment = "risk_on"
    elif sp500.get("change", 0) < -0.5:
        sentiment = "risk_off"

    return {
        "global_sentiment":  sentiment,
        "sentiment_reason":  "매크로 지표 기반 자동 판단 (Claude API 미연결)",
        "key_factors":       ["Claude API 연결 후 상세 분석 제공됩니다"],
        "news_summary":      f"수집된 기사 {len(news_data.get('articles', []))}개 | Claude API 연결 후 뉴스 분석이 제공됩니다.",
        "strong_buy":        strong_buy,
        "watch":             [{"sector": s, "reason": "RRG Leading — 모멘텀 확인 필요"} for s in leading[:2]],
        "avoid":             [],
        "summary":           "Claude API 연결 후 종합 판단이 제공됩니다.",
        "top_picks":         top_picks,
        "caution_sectors":   [],
    }


# ── 포맷 헬퍼 ─────────────────────────────────────────────────

def _format_macro(macro: dict) -> str:
    lines = []
    for name, d in macro.items():
        chg = d.get("change", 0)
        lines.append(f"  {name}: {d.get('price')} ({'+' if chg >= 0 else ''}{chg:.2f}%)")
    return "\n".join(lines)

def _format_articles(articles: list) -> str:
    lines = []
    for a in articles[:20]:
        body = a.get("body", "")[:300]
        lines.append(f"[{a.get('source')}] {a.get('title')}\n{body}")
    return "\n\n".join(lines)

def _format_rrg(rrg_data: dict) -> str:
    lines = []
    for sector, d in rrg_data.get("sector_rrg", {}).items():
        lines.append(
            f"  {sector}: {d.get('quadrant')} | RS-R={d.get('rs_ratio')} RS-M={d.get('rs_momentum')} | 1D={d.get('sector_ret_1d')}%"
        )
    return "\n".join(lines)

def _format_candidates(rrg_data: dict, strong_buy: list) -> str:
    lines = []
    candidates = rrg_data.get("top_candidates", {})
    strong_sectors = {s["sector"] for s in strong_buy}
    for sector in strong_sectors:
        stocks = candidates.get(sector, [])
        if stocks:
            lines.append(f"[{sector}]")
            for s in stocks[:5]:
                lines.append(f"  {s['name']}({s['code']}) signal={s['signal']} rs_ratio={s['rs_ratio']} vol={s['vol_ratio']}")
    return "\n".join(lines)


# ── 리포트 저장 + push ─────────────────────────────────────────

def save_report(report: dict) -> bool:
    try:
        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(REPORT_PATH) + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        tmp.replace(REPORT_PATH)
        logger.info(f"리포트 저장 완료: {REPORT_PATH}")
        return True
    except Exception as e:
        logger.error(f"리포트 저장 실패: {e}")
        return False

def git_push_report() -> bool:
    try:
        import git
        repo = git.Repo("/home/ubuntu/semon")
        repo.index.add(["docs/data/eqai_report.json"])
        repo.index.commit(f"EQAI report {datetime.now(KST).strftime('%Y-%m-%d %H:%M')} KST")
        repo.remote("origin").push()
        logger.info("EQAI 리포트 push 완료")
        return True
    except Exception as e:
        logger.error(f"EQAI push 실패: {e}")
        return False


# ── 메인 ──────────────────────────────────────────────────────

def run():
    logger.info("EQAI 분석 시작")

    # 1. 뉴스 + 매크로 수집
    logger.info("뉴스 수집 중...")
    news_data = collect_all(hours=12)

    # 2. RRG 데이터 fetch
    logger.info("RRG 데이터 fetch 중...")
    rrg_data = fetch_rrg_from_s3()

    # 3. Claude 분석
    logger.info("Claude 분석 중...")
    analysis = claude_analyze(news_data, rrg_data)

    # 4. 리포트 생성
    report = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "macro":        news_data.get("macro", {}),
        "sector_rrg":   rrg_data.get("sector_rrg", {}),
        "articles":     news_data.get("articles", [])[:10],
        **analysis,
    }

    # 5. 저장 + push
    save_report(report)
    git_push_report()

    logger.info("EQAI 분석 완료")
    return report


if __name__ == "__main__":
    run()
