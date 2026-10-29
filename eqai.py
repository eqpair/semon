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
import anthropic
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
STOCK_MAP_PATH = Path("/home/ubuntu/semon/stock_map.json")

# 국내 컨텍스트가 만든 종목 후보 코드 (top_picks는 이 안에서만 허용; 비어 있으면 RRG lagging 필터로 폴백)
_CANDIDATE_CODES: set = set()

# 종목명→코드 정답표 (Claude가 생성한 코드의 hallucination 교정용)
try:
    _STOCK_MAP = json.loads(STOCK_MAP_PATH.read_text(encoding="utf-8"))
except Exception:
    _STOCK_MAP = {}

def _fix_code(name: str, claude_code: str) -> str:
    """종목명으로 stock_map에서 정확한 코드를 찾아 교정. 없으면 Claude 코드 유지."""
    name = (name or "").strip()
    real = _STOCK_MAP.get(name)
    if real:
        return real
    return (claude_code or "").strip()
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


# 코드→실제섹터 역인덱스 (로컬 signals.json 기준 — S3 축약본엔 sectors 없음)
def _load_code2rrg() -> dict:
    """코드→RRG 정보 {sector, signal, quadrant, rs_ratio} (로컬 signals.json).
    추천 종목의 실제 RRG 상태로 lagging 필터링/태깅에 사용."""
    try:
        with open("/home/ubuntu/semon/docs/data/signals.json", encoding="utf-8") as fp:
            sig = json.load(fp)
        idx = {}
        for _sec, _sd in sig.get("sectors", {}).items():
            for _c in _sd.get("candidates", []):
                idx[_c.get("code")] = {
                    "sector":    _sec,
                    "signal":    _c.get("signal", ""),
                    "quadrant":  _c.get("quadrant", ""),
                    "rs_ratio":  _c.get("rs_ratio"),
                }
        return idx
    except Exception as e:
        logger.error(f"code2rrg 로드 실패: {e}")
        return {}


def _load_code2sector() -> dict:
    """docs/data/signals.json의 sectors에서 코드→섹터 맵 생성.
    S3 rrg_latest.json은 sector_rrg/top_candidates만 담아 sectors가 없으므로
    RRG 유니버스 추적여부/실제섹터 판정은 로컬 전체 파일에서 읽는다."""
    try:
        with open("/home/ubuntu/semon/docs/data/signals.json", encoding="utf-8") as fp:
            sig = json.load(fp)
        idx = {}
        for _sec, _sd in sig.get("sectors", {}).items():
            for _c in _sd.get("candidates", []):
                idx[_c.get("code")] = _sec
        logger.info(f"코드→섹터 역인덱스 로드: {len(idx)}개 종목")
        return idx
    except Exception as e:
        logger.error(f"역인덱스 로드 실패: {e}")
        return {}


# ── Claude 분석 ──────────────────────────────────────────────────

def _extract_json(text: str) -> dict:
    import re
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()
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
    if not os.getenv("ANTHROPIC_API_KEY", ""):
        return _placeholder_analysis(news_data, rrg_data)

    try:
        from dotenv import load_dotenv
        load_dotenv("/home/ubuntu/semon/.env")
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        macro_text      = _format_macro(news_data.get("macro", {}))
        # US 섹터 로테이션 컨텍스트 (fail-open: 없으면 기존과 동일)
        try:
            from us_context import us_rotation_block
            _us_rot = us_rotation_block()
            if _us_rot:
                macro_text = macro_text + "\n\n" + _us_rot
        except Exception:
            pass
        articles        = news_data.get("articles", [])
        titles_text     = "\n".join([f"- {a.get('title','')} [{a.get('source','')}]" for a in articles[:30]])
        # 본문 확보된 기사만 (상위 30개 후보 중 본문 100자 이상) — 최대 20개
        bodies_pool = [a for a in articles[:30] if (a.get("body") or "").strip() and len(a.get("body", "")) >= 100][:20]
        bodies_text     = "\n\n".join([f"[{a.get('source')}] {a.get('title')}\n{a.get('body','')[:300]}" for a in bodies_pool])
        logger.info(f"전달: 헤드라인 {min(30,len(articles))}개 + 본문 {len(bodies_pool)}개")
        rrg_text        = _format_rrg(rrg_data)
        # 국내 컨텍스트 (국면·시장 폭·수급·섹터·종목 후보) — fail-open
        global _CANDIDATE_CODES
        try:
            from eqai_domestic import domestic_context
            domestic_text, _CANDIDATE_CODES = domestic_context()
        except Exception as _e:
            logger.warning(f"국내 컨텍스트 실패 — RRG 후보로 폴백: {_e}")
            domestic_text, _CANDIDATE_CODES = "", set()
        if not domestic_text:
            # 폴백: 전 섹터 RRG 후보 (기존엔 빈 리스트를 넘겨 후보가 항상 비어 있었음)
            _all = [{"sector": s} for s in rrg_data.get("top_candidates", {})]
            domestic_text = "=== 종목 후보 (RRG) ===\n" + _format_candidates(rrg_data, _all)

        prompt = f"""당신은 월스트리트와 여의도를 평정한 전설의 애널리스트입니다. 워런 버핏도 당신의 리포트를 읽고, 소로스도 당신의 판단을 구합니다. 30년 경력의 글로벌 매크로 전략가이자 한국 주식시장의 살아있는 전설입니다.

[기술적 분석 철학]
- 마크 미너비니의 SEPA 전략: 52주 신고가 돌파, VCP 패턴, 추세·펀더멘탈·촉매·타이밍의 4박자
- 스탠 와인스타인의 스테이지 분석: 1(바닥)→2(상승)→3(천장)→4(하락), 스테이지 2 초입 진입 원칙
- 닥터 엘더의 삼중 스크린: 주간(추세)+일간(Momentum)+단기(타이밍) 다중 시간대 분석
- RRG: 시장 대비 상대강도를 보는 참고 지표. 느리게 반응하므로 단독 근거로 쓰지 말고 최근 수익률·상승 종목 비율·거래대금·수급과 함께 판단
- 소로스 재귀성: Fundamentals(매크로·실물 환경)와 Momentum(가격·수급 반응)의 상호작용으로 붐-버스트 국면을 판단. 아래 '국내 시장 국면'이 그 결과
섹터 분석과 종목 선정 시 위 관점을 반영하되, 한 지표에만 기대지 마세요.

아래 데이터를 분석하여 반드시 아래 XML 구조로만 응답하세요.
절대로 다른 XML 태그를 추가하지 마세요. 코드블록(```)도 사용하지 마세요.
반드시 <analysis>로 시작하고 </analysis>로 끝나야 합니다.
속성값(reason="..." 등) 안에는 큰따옴표(")를 절대 쓰지 마세요. 인용이 필요하면 ‘ ’를 쓰세요.

[뉴스 요약 원칙]
- 뉴스 상세 본문을 반드시 참고하여 정확한 맥락과 수치를 포함하세요
- 헤드라인만 보고 단편적으로 요약하지 마세요
- "A가 B 대비 X배" 같은 표현은 반드시 무엇 기준인지 명시하세요
- 오해를 유발할 수 있는 축약 표현을 피하세요

[뉴스 카테고리 분류 규칙]
- macro (매크로·금리): 연준·ECB·BOJ 정책, 금리·인플레·고용, 환율, 원자재, 채권시장
- geopolitics (지정학·정책): 전쟁·중동·러우, 미중관계, 무역·관세·제재, 선거, 규제
- company (기업·실적): 개별 기업 실적·목표가·M&A·신제품·소송, 빅테크 동향, 우주·AI 등 산업 이슈
- 한국 시장 영향은 별도 섹션에서 다루므로 news_summary에서는 글로벌 이슈만 다루세요
- 단, 한국 종목 직접 뉴스(예: 삼성전자·SK하이닉스 실적, 외국인 매도 등)는 company에 포함 가능

[임팩트 등급 기준 — impact 속성]
- high: 글로벌 자산 가격에 즉각·광범위한 영향 (연준 결정, 빅테크 어닝쇼크, 전쟁 발발 등)
- mid: 특정 섹터·지역에 영향, 또는 중기적으로 의미 있는 변화

[뉴스 항목 수량]
- 글로벌 3개 카테고리 (macro / geopolitics / company)로만 작성하세요
- 각 카테고리 3~5개씩, 전체 약 10~14개 뉴스
- company 카테고리는 4~5개로 가장 풍성하게 (빅네임 시그널은 모두 포함)
- 카테고리별 high 등급은 최소 1개 포함하세요
- 빅네임 종목(엔비디아·마이크론·TSMC·애플·테슬라·삼성·SK하이닉스)의 목표가·실적·신제품 뉴스는 우선순위로 high 처리
- 같은 사건은 한 카테고리에만 분류하세요 (중복 금지)

[매크로 원인 분석 원칙 — 매우 중요]
- 아래 매크로 지수 리스트의 모든 항목에 대해 <reason> 태그로 등락 원인을 반드시 작성하세요
- 원인은 뉴스 상세 본문에서 근거를 찾아 구체적으로 작성하세요 ("실적 호조" 같은 모호한 표현 금지)
- 핵심 드라이버(개별 종목·이벤트·지표·정책 등 2~3개)를 <driver> 태그로 나열하세요
- 등락폭이 ±0.3% 미만이라도 시장 분위기·동조화 요인을 한 줄로 설명하세요
- 뉴스에 직접적 근거가 없으면 "직접 촉매 없음 — [관련 매크로 흐름] 동조" 형식으로 명시하세요
- 절대로 추측성 원인을 만들어내지 마세요. 근거가 약하면 약하다고 쓰세요

=== 글로벌 매크로 ===
{macro_text}

=== 주요 뉴스 헤드라인 ===
{titles_text}

=== 뉴스 상세 ===
{bodies_text}

{domestic_text}

=== RRG 섹터 지표 (참고) ===
{rrg_text}

[종합 판단 원칙 — 글로벌에서 국내로, 국내에서 종목으로 / 매우 중요]
- korea_impact의 positive/negative 요인은 반드시 위 글로벌 매크로·뉴스에서 직접 도출하세요. 각 요인은 "[글로벌 원인] → [국내 영향]" 형태로 인과를 드러내세요. (예: "필라델피아반도체 +2%·엔비디아 가이던스 상향 → 국내 반도체 투자심리 개선")
- top_picks는 반드시 위 '종목 후보'(Picks 이번 달 포트폴리오) 목록 안에서만 고르세요. 주식 비중 70% 이상이면 3~6종목, 30~70%면 3~4종목, 30% 이하면 2~3종목입니다. 목록 밖 종목은 시스템이 자동 제외합니다.
- 'Focus 2' 블록이 있으면 그 두 종목을 top_picks의 맨 앞에 두세요(목록 밖 제한의 예외). 실제 매매 후보는 이 두 종목입니다. 상태가 보유 중이거나 조건 충족이면 각 reason에 선정 업종 순위, Score와 업종 내 종합, 목표가 상향·하향 건수를 숫자로 쓰고 끝에 리스크: …를 붙이세요. 상태가 대기이면 reason은 대기 — 로 시작하고 끝에 전환 조건: Fundamentals 기준 이상과 Momentum 기준 이상(현재 값과 기준을 숫자로)을 쓰세요. strategy_brief에 Focus 2의 상태와 시장 조건 숫자를 한 문장으로 밝히세요.
- top_picks의 각 pick reason은 세 층을 한 문장에 엮으세요: (1) 글로벌→국내 판단(korea_impact의 어느 요인에서 파생됐는지), (2) 국내 Market Regime(Risk-On/Off)과 Sector Momentum 순위, (3) 종목 근거 두 가지 이상(Quality·Trend·Low Vol 점수, 오늘 순위, 52주 고점 대비, 수급 중). 많이 떨어져 반등 여력이 있다, 되돌림이 충분하다, 낙폭 과대처럼 하락폭 자체를 근거로 쓰지 마세요 — 검증에서 오르지 않은 종목은 이후에도 덜 올랐습니다. RRG 사분면은 근거로 쓰지 마세요.
- 종목 후보는 Picks Composite 점수 순입니다(Quality: 흑자 여부·이익수익률·EPS 증가·외국인+기관 순매수·Sector Momentum / Trend: 52주 고점 근접·최근 1개월 급등 회피 / Low Vol: 낮은 변동성). 2011~2026 검증에서 Risk-On 구간의 Quality·Trend 상위 종목이 이후 60일 가장 많이 올랐고, 최근 1~3개월 급등·거래량 급증 종목은 오히려 덜 올랐습니다. 특별한 이유가 없으면 순위 상위 종목을 우선하세요.
- Market Regime Filter(Equity Exposure, 주식 비중 0~100%) 판정을 모든 결론의 전제로 삼으세요. Risk-Off(주식 비중 30% 이하)일 때: 결론은 현금(단기채) 비중 유지와 신규 매수 보류입니다. 매수, 비중 확대, 선별 접근, 방어주로 대응 같은 표현을 쓰지 말고, top_picks는 주식 비중이 올라갈 때 먼저 늘릴 대기 명단(watchlist)으로 쓰세요. 각 pick reason은 대기 — 로 시작하고 끝에 전환 조건: …과 리스크: …를 따옴표 없이 붙이세요. Neutral(30~70%)일 때: 권장 배분 비율 안에서만 보유하고 분할로 접근한다고 쓰고, 비중을 더 늘릴 조건을 함께 쓰세요. Risk-On(70% 이상)일 때: 권장 배분(코스피 200 ETF와 Picks 종목 반반)을 전제로 Sector Momentum 상위이면서 Quality와 Trend 점수가 함께 높은 종목 위주로 쓰세요. 모든 경우 strategy_brief에 현재 주식 비중(%)과 권장 배분을 숫자로 밝히고, 하루 급반등만으로 국면 전환을 단정하지 마세요. 각 pick reason 끝에는 리스크: …를 붙이세요.
- 수치는 위에 제공된 데이터와 뉴스 본문에 있는 것만 인용하세요. 금액 단위(억·조)를 그대로 옮기고 임의로 환산하지 마세요. 종목별 외국인·기관 매매 금액은 종목 후보 표에 수급 열이 있을 때만 언급하세요.
- RRG 사분면과 최근 수익률·상승 종목 비율이 엇갈리면(예: RRG leading인데 5일 수익률이 크게 마이너스) 최근 데이터를 우선하고, 그 엇갈림을 판단에 반영하세요.
- summary는 "글로벌 환경 → 국내 시장 한 줄 판단 → 핵심 진입 아이디어" 순서의 한 흐름으로 쓰세요.
- strategy_headline은 "해외/미국 동인 → 국내 전개"의 인과가 드러나는 오늘 국내 시장의 핵심 논지를 한 줄(공백 포함 40자 이내)로 압축하세요. 예: "AI 수출 슈퍼사이클 확인 — 반도체·IT부품 Leading 집중"
- strategy_points는 투자자가 한눈에 봐야 할 요점 3~4개로 쓰세요. 각 요점은 한 줄(1문장, 짧게)이며 가능하면 각 요점을 "[해외/미국 원인] → [국내 영향·대응]" 인과 형태로 쓰고, 서로 다른 측면(글로벌 동인 / 국내 국면·시장 폭·수급 / 섹터 Momentum / 리스크)을 담으세요. 이 중 하나는 반드시 국내 시장 국면 단계와 시장 폭·수급 데이터를 근거로 쓰세요. 국내 지표는 판단의 출발점이 아니라 해외 동인의 결과·확인 수단으로만 언급하세요.
- strategy_headline·strategy_points·strategy_brief는 반드시 이 순서로 전개하세요: ① 미국·해외 시장의 현 상황 → ② 그로 인해 오늘 국내 시장이 어떻게 전개될지 → ③ 국내 전략·섹터 결론. 코스피/코스닥 등락·수급 등 국내 지표를 문두나 논리의 출발점으로 두지 마세요.
- [용어 주석 — 필수, 누락 금지] strategy_headline·strategy_points·strategy_brief·summary에서 일반 투자자가 어려워할 용어는 반드시 첫 등장 시 괄호로 쉬운 설명을 병기하세요. 대상: 경제지표 약어(PPI, CPI, ESS 등), 시장 용어(리스크오프, 밸류에이션, 커브 스티프닝, 숏커버링, 수급 등), RRG 용어(leading, improving, weakening, lagging, RS-Momentum 등), 국면 용어(Deterioration, Capitulation, 시장 폭 등). 예: "PPI(생산자물가지수)", "커브 스티프닝(장기금리가 단기금리보다 빠르게 올라 예대마진이 개선되는 현상)", "improving(상승 전환 구간)". 같은 용어의 주석은 문서 전체에서 1회만 다세요. 주석 없이 전문 용어만 쓰는 것은 작성 규칙 위반입니다.
- [표현] 형세·기세 같은 비유적 표현이나 고전 인용은 쓰지 마세요. 영어로 통용되는 금융 용어는 영어로 쓰고(Fundamentals, Momentum, Bias, Risk-On/Off, Quality, Trend, Low Vol, Sector Momentum 등), 첫 등장 시 필요하면 괄호로 한국어 설명을 다세요.
- strategy_brief는 위 모든 분석을 종합한 "오늘의 국내 시장 전략"을 서술형 한 문단(3~5문장)으로 쓰세요. 긍정/부정 요인을 나열하지 말고, 글로벌 환경이 국내에 주는 함의와 그래서 어디에 주목하는지를 자연스러운 문장으로 압축하세요. key_factors나 korea_impact 항목을 그대로 복사하지 말고 새로 종합 서술하세요.

아래 XML 구조로만 응답하세요:
<analysis>
  <sentiment>risk_on</sentiment>
  <sentiment_reason>한줄이유</sentiment_reason>
  <key_factors>
    <factor>요인1</factor>
    <factor>요인2</factor>
    <factor>요인3</factor>
  </key_factors>
  <macro_reasons>
    <macro name="S&amp;P500">
      <reason>등락의 핵심 원인 한 줄 (뉴스 근거 포함)</reason>
      <driver>드라이버1 (예: NVDA +4.2%)</driver>
      <driver>드라이버2</driver>
    </macro>
    <macro name="필라델피아반도체">
      <reason>등락 원인 (예: 엔비디아 실적 가이던스 상향 + TSMC AI 수요 재확인)</reason>
      <driver>NVDA 실적 호조</driver>
      <driver>TSM 가이던스 상향</driver>
    </macro>
    <!-- 위 매크로 리스트의 모든 지수에 대해 동일한 패턴으로 작성 -->
  </macro_reasons>
  <news_summary>
    <category name="macro" label="매크로·금리">
      <item impact="high">뉴스1 (맥락·수치 포함, 한 줄)</item>
      <item impact="mid">뉴스2</item>
    </category>
    <category name="geopolitics" label="지정학·정책">
      <item impact="high">뉴스1</item>
      <item impact="mid">뉴스2</item>
    </category>
    <category name="company" label="기업·실적">
      <item impact="high">뉴스1 (예: Micron UBS 목표가 인상 등 빅네임 시그널)</item>
      <item impact="high">뉴스2</item>
      <item impact="mid">뉴스3</item>
      <item impact="mid">뉴스4</item>
    </category>
  </news_summary>
  <korea_impact>
    <summary>코스피 영향 한줄</summary>
    <positive>긍정요인1</positive>
    <positive>긍정요인2</positive>
    <negative>부정요인1</negative>
    <negative>부정요인2</negative>
    <watch_point>오늘 주목 포인트</watch_point>
  </korea_impact>
  <strong_buy>
    <sector quadrant="leading" reason="선정근거">반도체_소자</sector>
    <sector quadrant="improving" reason="선정근거">2차전지_셀</sector>
  </strong_buy>
  <watch_sectors>
    <sector reason="관망이유">조선</sector>
  </watch_sectors>
  <top_picks>
    <pick sector="반도체_소자" name="삼성전자" code="005930" reason="선정근거"/>
    <pick sector="반도체_소자" name="SK하이닉스" code="000660" reason="선정근거"/>
  </top_picks>
  <strategy_headline>오늘 국내 시장을 한 줄로 압축한 핵심 논지 (간결한 헤드라인)</strategy_headline>
  <strategy_points>
    <point>요점1 (짧고 명료하게, 글로벌→국내 함의 또는 핵심 판단)</point>
    <point>요점2</point>
    <point>요점3</point>
  </strategy_points>
  <strategy_brief>글로벌 환경부터 국내 시장 판단, 핵심 진입 아이디어까지 하나의 흐름으로 엮은 3~5문장 종합 판단 (서술형 한 문단)</strategy_brief>
  <summary>오늘 시장 한줄 판단</summary>
</analysis>"""

        resp = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=16000,
            messages=[{"role": "user", "content": prompt}]
        )
        text = resp.content[0].text
        logger.info(f"분석 완료 ({len(text)}자)")
        return _parse_xml_response(text, rrg_data)

    except Exception as e:
        logger.error(f"분석 실패: {e}")
        return _placeholder_analysis(news_data, rrg_data)


def _parse_xml_response(text: str, rrg_data: dict) -> dict:
    import xml.etree.ElementTree as ET
    import re

    text = text.strip()
    text = re.sub(r"```xml\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    m = re.search(r"<analysis>.*?</analysis>", text, re.DOTALL)
    if not m:
        # 닫는 태그 누락 여부 진단
        has_open  = "<analysis>" in text
        has_close = "</analysis>" in text
        tail = text[-300:] if len(text) > 300 else text
        raise ValueError(
            f"XML analysis 매칭 실패 (열림={has_open}, 닫힘={has_close}, "
            f"전체길이={len(text)}자). 응답 끝부분 300자:\n{tail}"
        )

    xml_text = m.group()
    # reason 속성값 안의 큰따옴표·꺾쇠 교정 (속성이 중간에 끊겨 이후 내용이 유실되는 것 방지)
    def _fix_attr(mm):
        inner = mm.group(2).replace('"', '”').replace("<", "‹").replace(">", "›")
        return mm.group(1) + inner + mm.group(3)
    xml_text = re.sub(r'(reason=")(.*?)("\s*/?>)', _fix_attr, xml_text, flags=re.DOTALL)
    xml_text = re.sub(r"&(?!amp;|lt;|gt;|quot;|apos;)", "&amp;", xml_text)
    # 태그가 아닌 위치의 < 를 이스케이프 (본문 내 "<예상", "<3%" 등으로 파싱 깨짐 방지)
    xml_text = re.sub(r"<(?![a-zA-Z/!?])", "&lt;", xml_text)
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as pe:
        import time
        fail_path = f"/tmp/claude_fail_{int(time.time())}.xml"
        open(fail_path, "w", encoding="utf-8").write(xml_text)
        logger.error(f"ET 파싱 실패({pe}) — lxml recover 시도, raw 저장: {fail_path}")
        from lxml import etree as _lxml_etree
        root = _lxml_etree.fromstring(xml_text.encode("utf-8"), _lxml_etree.XMLParser(recover=True))

    def get(tag, default=""):
        el = root.find(tag)
        return el.text.strip() if el is not None and el.text else default

    def get_list(tag):
        return [el.text.strip() for el in root.findall(tag) if el.text]

    sentiment        = get("sentiment", "neutral")
    sentiment_reason = get("sentiment_reason", "")
    key_factors      = get_list("key_factors/factor")
    # 새 구조: 카테고리별 뉴스 + 임팩트 등급
    news_categories = []
    flat_items = []
    for cat in root.findall("news_summary/category"):
        cat_name  = cat.get("name", "")
        cat_label = cat.get("label", cat_name)
        items = []
        for it in cat.findall("item"):
            if it.text:
                items.append({
                    "text":   it.text.strip(),
                    "impact": (it.get("impact", "mid") or "mid").lower(),
                })
                flat_items.append(it.text.strip())
        if items:
            news_categories.append({
                "name":  cat_name,
                "label": cat_label,
                "items": items,
            })
    # 하위호환: news_summary 문자열도 계속 채워둠 (구 클라이언트 대비)
    news_summary = "\n".join(flat_items)
    summary          = get("summary", "")
    strategy_brief   = get("strategy_brief", "")
    strategy_headline = get("strategy_headline", "")
    strategy_points   = get_list("strategy_points/point")

    # ── 매크로 원인 파싱 ───────────────────────────────────────
    macro_reasons = {}
    for el in root.findall("macro_reasons/macro"):
        name = el.get("name", "").strip()
        if not name:
            continue
        reason_el = el.find("reason")
        reason = reason_el.text.strip() if reason_el is not None and reason_el.text else ""
        drivers = [d.text.strip() for d in el.findall("driver") if d.text]
        macro_reasons[name] = {"reason": reason, "drivers": drivers}

    # 한국 시장 영향
    ki = root.find("korea_impact")
    korea_impact = {
        "summary":          ki.find("summary").text.strip() if ki is not None and ki.find("summary") is not None else "",
        "positive_factors": [el.text.strip() for el in ki.findall("positive") if el.text] if ki is not None else [],
        "negative_factors": [el.text.strip() for el in ki.findall("negative") if el.text] if ki is not None else [],
        "key_watch":        ki.find("watch_point").text.strip() if ki is not None and ki.find("watch_point") is not None else "",
    }

    # RRG 실측 quadrant 조회용 (Claude가 쓴 값 대신 signals.json 신뢰)
    _sector_rrg = rrg_data.get("sector_rrg", {})
    # 코드→실제섹터 역인덱스 (로컬 signals.json — S3엔 sectors 없음)
    _code2rrg = _load_code2rrg()

    strong_buy = []
    for el in root.findall("strong_buy/sector"):
        if el.text:
            _sec = el.text.strip()
            # RRG 실측 quadrant로 강제 (없으면 Claude 값 폴백)
            _rrg_q = _sector_rrg.get(_sec, {}).get("quadrant", "")
            strong_buy.append({
                "sector":       _sec,
                "rrg_quadrant": _rrg_q or el.get("quadrant", ""),
                "claude_quadrant": el.get("quadrant", ""),  # Claude 원판단 보존(감사용)
                "reason":       el.get("reason", ""),
            })

    watch = []
    for el in root.findall("watch_sectors/sector"):
        if el.text:
            watch.append({
                "sector": el.text.strip(),
                "reason": el.get("reason", ""),
            })

    top_picks_raw = {}
    for el in root.findall("top_picks/pick"):
        sector = el.get("sector", "")
        if not sector:
            continue
        if sector not in top_picks_raw:
            sq = next((s.get("rrg_quadrant","") for s in strong_buy if s["sector"]==sector), "")
            sr = next((s.get("reason","") for s in strong_buy if s["sector"]==sector), "")
            top_picks_raw[sector] = {"sector": sector, "rrg_quadrant": sq, "reason": sr, "stocks": []}
        _nm = el.get("name", "")
        _cd = _fix_code(_nm, el.get("code", ""))
        _rrg = _code2rrg.get(_cd)  # RRG 정보 {sector, signal, quadrant, rs_ratio}
        _actual_sec = _rrg.get("sector") if _rrg else None
        _rrg_signal = _rrg.get("signal", "") if _rrg else ""
        # 데이터 근거 없는 추천 방지: 국내 컨텍스트의 종목 후보 목록 밖이면 제외.
        # 후보 목록을 못 만든 경우에만 기존 RRG lagging 필터로 폴백.
        if _CANDIDATE_CODES:
            if _cd not in _CANDIDATE_CODES:
                logger.info(f"top_pick 제외: {_nm}({_cd}) — 종목 후보 목록 밖")
                continue
        elif _rrg_signal == "lagging":
            logger.info(f"top_pick 제외: {_nm}({_cd}) — RRG lagging (실제섹터={_actual_sec}, rs_ratio={_rrg.get('rs_ratio')})")
            continue
        top_picks_raw[sector]["stocks"].append({
            "name":   _nm,
            "code":   _cd,
            "signal": "prime",
            "reason": el.get("reason", ""),
            "rrg_tracked":       _actual_sec is not None,
            "rrg_actual_sector": _actual_sec or "",
            "rrg_sector_match":  (_actual_sec == sector),
            "rrg_signal":        _rrg_signal,                  # 실제 RRG 신호(투명성)
        })
    # lagging 제외로 종목이 0개가 된 섹터는 top_picks에서 제거
    top_picks = [tp for tp in top_picks_raw.values() if tp.get("stocks")]
    # Focus 2(focus.py) 종목은 Focus가 고른 업종으로 묶어 Focus 업종 순위대로 맨 앞에 — 실제 매매 후보 (_focus_v2)
    _focus = _focus_codes()            # {code: (업종, 순서)}
    fgroups, rest = {}, []
    for tp in top_picks:
        keep = []
        for st in tp["stocks"]:
            st["focus"] = st.get("code") in _focus
            if st["focus"]:
                sec, order = _focus[st["code"]]
                g = fgroups.setdefault(sec, {"sector": sec, "rrg_quadrant": "", "reason": "", "stocks": [], "_o": order})
                if tp.get("sector") == sec:
                    g["rrg_quadrant"], g["reason"] = tp.get("rrg_quadrant", ""), tp.get("reason", "")
                g["stocks"].append(st)
            else:
                keep.append(st)
        if keep:
            rest.append(dict(tp, stocks=keep))
    for tp in list(rest):              # 일반 종목이 Focus 업종과 같으면 그 묶음 뒤에
        if tp.get("sector") in fgroups:
            fgroups[tp["sector"]]["stocks"] += tp["stocks"]
            rest.remove(tp)
    top_picks = [{k: v for k, v in g.items() if k != "_o"} for g in sorted(fgroups.values(), key=lambda g: g["_o"])] + rest

    return {
        "global_sentiment":    sentiment,
        "sentiment_reason":    sentiment_reason,
        "key_factors":         key_factors,
        "news_summary":        news_summary,
        "news_categories":     news_categories,
        "macro_reasons":       macro_reasons,
        "korea_market_impact": korea_impact,
        "sector_impact":       {},
        "strong_buy":          strong_buy,
        "watch":               watch,
        "avoid":               [],
        "summary":             summary,
        "strategy_brief":      strategy_brief,
        "strategy_headline":   strategy_headline,
        "strategy_points":     strategy_points,
        "top_picks":           top_picks,
        "caution_sectors":     [w["sector"] for w in watch[:2]],
    }



def _focus_codes() -> dict:
    """docs/data/focus.json의 Focus 2 종목 {코드: (업종, 순서)} (보유 중이면 보유 종목, 아니면 후보)."""
    try:
        j = json.loads((Path(__file__).resolve().parent / "docs" / "data" / "focus.json").read_text(encoding="utf-8"))
        lst = j.get("holdings") if j.get("status") == "hold" else j.get("picks")
        return {x["t"]: (x.get("s") or "Focus", i) for i, x in enumerate(lst or []) if x.get("t")}
    except Exception as e:
        logger.warning(f"focus.json 읽기 실패(Focus 표시 생략): {e}")
        return {}


def _placeholder_analysis(news_data: dict, rrg_data: dict) -> dict:
    sector_rrg = rrg_data.get("sector_rrg", {})
    improving = [s for s, d in sector_rrg.items() if d.get("quadrant") == "improving"]
    leading   = [s for s, d in sector_rrg.items() if d.get("quadrant") == "leading"]

    # 코드→실제섹터 역인덱스 (로컬 signals.json — 정상 경로와 동일)
    _code2rrg = _load_code2rrg()

    strong_buy = [
        {"sector": s, "reason": "RRG Improving 진입 — Momentum 전환 신호",
         "rrg_quadrant": sector_rrg.get(s, {}).get("quadrant", "improving"),
         "claude_quadrant": ""}  # 폴백은 Claude 판단 없음
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
                "rrg_quadrant": sector_rrg.get(sector, {}).get("quadrant", "improving"),
                "reason":       "RRG Improving + prime/confirm 신호 종목 보유",
                "stocks": [
                    {"code": s["code"], "name": s["name"],
                     "signal": s["signal"], "reason": "RRG prime/confirm 신호",
                     "rrg_tracked":       _code2rrg.get(s["code"]) is not None,
                     "rrg_actual_sector": (_code2rrg.get(s["code"]) or {}).get("sector", ""),
                     "rrg_sector_match":  ((_code2rrg.get(s["code"]) or {}).get("sector") == sector),
                     "rrg_signal":        (_code2rrg.get(s["code"]) or {}).get("signal", "")}
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
        "sentiment_reason":  "매크로 지표 기반 자동 판단 (API 미연결)",
        "key_factors":       ["API 연결 후 상세 분석 제공됩니다"],
        "news_summary":      f"수집된 기사 {len(news_data.get('articles', []))}개 | API 연결 후 뉴스 분석이 제공됩니다.",
        "news_categories":   [],
        "macro_reasons":     {},
        "strong_buy":        strong_buy,
        "watch":             [{"sector": s, "reason": "RRG Leading — Momentum 확인 필요"} for s in leading[:2]],
        "avoid":             [],
        "summary":           "API 연결 후 종합 판단이 제공됩니다.",
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
    if os.getenv("EQAI_NO_PUSH"):
        logger.info("EQAI_NO_PUSH 설정됨 - git push 건너뜀 (테스트 모드)")
        return False
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

    logger.info("뉴스 수집 중...")
    news_data = collect_all(hours=12)

    logger.info("RRG 데이터 fetch 중...")
    rrg_data = fetch_rrg_from_s3()

    logger.info("분석 중...")
    analysis = claude_analyze(news_data, rrg_data)
    if not analysis.get("strategy_headline") or not analysis.get("strategy_brief"):
        logger.warning("핵심 필드(headline/brief) 누락 — 1회 재시도")
        analysis = claude_analyze(news_data, rrg_data)

    # ── 매크로 데이터에 원인 병합 ─────────────────────────────
    macro_with_reasons = {}
    macro_reasons = analysis.get("macro_reasons", {})
    for name, d in news_data.get("macro", {}).items():
        merged = dict(d)
        mr = macro_reasons.get(name, {})
        reason  = mr.get("reason", "")
        drivers = mr.get("drivers", [])
        # 폴백: AI가 reason을 누락한 경우 등락 기반 기본 문구 (특히 BTC 등 신규 지표)
        if not reason:
            chg = d.get("change")
            if chg is not None:
                direction = "상승" if chg >= 0 else "하락"
                reason = f"{name} {abs(chg):.2f}% {direction} — 상세 코멘트 생성 전"
        merged["reason"]  = reason
        merged["drivers"] = drivers
        macro_with_reasons[name] = merged

    # 공개 리포트에는 원문 본문(body)을 저장하지 않는다 (저작권).
    # 가공된 요약은 news_categories가 담당하고, 여기서는 출처 링크 목록만 보존한다.
    sources = [
        {
            "source":    a.get("source", ""),
            "title":     a.get("title", ""),
            "link":      a.get("link", ""),
            "published": a.get("published", ""),
        }
        for a in news_data.get("articles", [])[:15]
        if a.get("link")
    ]

    report = {
        "generated_at": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "macro":        macro_with_reasons,
        "sector_rrg":   rrg_data.get("sector_rrg", {}),
        "sources":      sources,
        **analysis,
    }

    if not report.get("strategy_headline") or not report.get("strategy_brief"):
        logger.error("빈 리포트 감지(headline·top_picks 모두 없음) — 저장·푸시 스킵, 기존 리포트 보존")
        raise SystemExit(1)
    save_report(report)
    git_push_report()

    logger.info("EQAI 분석 완료")
    return report


if __name__ == "__main__":
    run()
