#!/usr/bin/env python3
"""
eqai_chat.py — EQAI 채팅 API 서버
"""
import json, logging, boto3, requests
from flask import Flask, request, jsonify
from flask_cors import CORS
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import StringIO

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
REPORT_PATH   = Path("/home/ubuntu/semon/docs/data/eqai_report.json")
STOCK_MAP_PATH = Path("/home/ubuntu/semon/stock_map.json")
KST = ZoneInfo("Asia/Seoul")

# ── 종목 매핑 로드 ────────────────────────────────────────────────
def load_stock_map() -> dict:
    try:
        with open(STOCK_MAP_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"stock_map 로드 실패: {e}")
        return {}

def refresh_stock_map():
    """KRX에서 전체 종목 매핑 갱신 (주 1회 권장)"""
    try:
        import pandas as pd
        res = requests.post(
            "https://kind.krx.co.kr/corpgeneral/corpList.do",
            data={"method": "download", "searchType": "13"},
            headers={"User-Agent": "Mozilla/5.0"}, timeout=30
        )
        content = res.content.decode("euc-kr", errors="ignore")
        df = pd.read_html(StringIO(content))[0]
        stock_map = {}
        for _, row in df.iterrows():
            name = str(row["회사명"]).strip()
            code = str(row["종목코드"]).zfill(6)
            stock_map[name] = code
        with open(STOCK_MAP_PATH, "w", encoding="utf-8") as f:
            json.dump(stock_map, f, ensure_ascii=False)
        logger.info(f"종목 매핑 갱신 완료: {len(stock_map)}개")
        return stock_map
    except Exception as e:
        logger.error(f"종목 매핑 갱신 실패: {e}")
        return load_stock_map()

_stock_map = load_stock_map()
logger.info(f"종목 매핑 로드: {len(_stock_map)}개")

# ── 현재가 조회 ───────────────────────────────────────────────────
def get_stock_price(code: str, name: str) -> dict | None:
    """네이버 금융 API로 실시간 현재가 조회"""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, timeout=5)
        if res.status_code != 200:
            return None
        data = res.json()
        price     = int(data["closePrice"].replace(",", ""))
        change    = float(data["fluctuationsRatio"])
        status    = data.get("marketStatus", "")
        return {"price": price, "change": change, "status": status, "code": code}
    except Exception as e:
        logger.warning(f"현재가 조회 실패 ({name}/{code}): {e}")
        return None

def extract_stocks_from_messages(messages: list) -> dict:
    """대화에서 종목명 찾아 실시간 현재가 조회"""
    if not _stock_map:
        return {}
    full_text = " ".join([m.get("content", "") for m in messages])
    found = {}
    # 긴 이름 우선 매칭
    for name in sorted(_stock_map.keys(), key=len, reverse=True):
        if len(name) < 2:
            continue
        if name in full_text and name not in found:
            code = _stock_map[name]
            if not code.isdigit():
                continue
            price_info = get_stock_price(code, name)
            if price_info:
                found[name] = price_info
                logger.info(f"종목 감지: {name}({code}) = {price_info['price']:,}원 ({price_info['change']:+.2f}%)")
            if len(found) >= 10:
                break
    return found

def load_report() -> dict:
    try:
        with open(REPORT_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def build_system_prompt(report: dict, current_prices: dict) -> str:
    macro = report.get("macro", {})
    macro_lines = "\n".join([
        f"  - {k}: {v.get('price')} ({'+' if v.get('change',0)>=0 else ''}{v.get('change',0):.2f}%)"
        for k, v in macro.items()
    ])

    rrg = report.get("sector_rrg", {})
    leading   = [s for s,d in rrg.items() if d.get("quadrant")=="leading"]
    improving = [s for s,d in rrg.items() if d.get("quadrant")=="improving"]
    weakening = [s for s,d in rrg.items() if d.get("quadrant")=="weakening"]
    lagging   = [s for s,d in rrg.items() if d.get("quadrant")=="lagging"]

    picks = report.get("top_picks", [])
    picks_text = ""
    for p in picks:
        stocks = ", ".join([f"{s['name']}({s['code']})" for s in p.get("stocks", [])])
        picks_text += f"\n  - {p['sector']} [{p.get('rrg_quadrant','').upper()}]: {p.get('reason','')} → {stocks}"

    ki = report.get("korea_market_impact", {})

    price_section = ""
    if current_prices:
        price_lines = "\n".join([
            f"  - {name}({d['code']}): {d['price']:,}원 ({d['change']:+.2f}%) [{d.get('status','')}]"
            for name, d in current_prices.items()
        ])
        price_section = f"""
## ⚡ 실시간 종목 현재가 (방금 네이버 금융에서 조회)
반드시 아래 가격을 기준으로 분석하세요. 이 가격이 실제 현재가입니다.
{price_lines}
"""

    return f"""당신은 글로벌 시장과 한국 주식시장에 정통한 올라운드 투자 전문가입니다.
중장기 투자 철학과 단기 트레이딩 기술을 모두 갖추고 있습니다.

## 투자 철학 (중장기)
워런 버핏의 내재가치·경제적 해자, 하워드 막스의 시장 사이클과 리스크 인식,
레이 달리오의 부채 사이클과 자본 흐름, 피터 린치의 바텀업 종목 발굴,
조지 소로스의 재귀이론과 시장 심리 독해를 체화하고 있습니다.

## 단기 트레이딩 기술
- **마크 미너비니**: SEPA 전략 — 추세·펀더멘탈·촉매·타이밍. 52주 신고가 돌파, VCP 패턴 포착
- **스탠 와인스타인**: 스테이지 분석 — 1(바닥)→2(상승)→3(천장)→4(하락). 스테이지 2 초입 진입
- **닥터 엘더**: 삼중 스크린 — 주간(추세)+일간(모멘텀)+단기(타이밍)
- **RRG 모멘텀**: RS-Ratio>100 + RS-Momentum>100 = Improving/Leading 구간 단기 최적 진입

## 답변 원칙
- 질문 성격에 따라 단기/중장기 관점을 명확히 구분해 답합니다
- 단기 질문: 진입 조건, 손절 기준, 목표가 프레임을 구체적으로 제시합니다
- 중장기 질문: 구조적 흐름, 역사적 사례, 밸류에이션 관점을 포함합니다
- 한국 시장 특수성(외국인 수급, 환율 민감도, 반도체 의존도, 정책 변수)을 항상 고려합니다
- 한국어로 답변합니다
{price_section}
## 현재 시장 스냅샷 ({report.get('generated_at', '')})

[글로벌 매크로]
{macro_lines}

[시장 심리] {report.get('global_sentiment', '-')} — {report.get('sentiment_reason', '-')}

[RRG 섹터 모멘텀]
- Leading: {', '.join(leading) or '없음'}
- Improving: {', '.join(improving) or '없음'}
- Weakening: {', '.join(weakening) or '없음'}
- Lagging: {', '.join(lagging) or '없음'}

[오늘 주목 섹터·종목]{picks_text}

[한국 시장 영향] {ki.get('summary', '-')}

[종합 판단] {report.get('summary', '-')}"""

@app.route("/chat", methods=["POST"])
def chat():
    try:
        data     = request.json
        messages = data.get("messages", [])
        report   = load_report()

        current_prices = extract_stocks_from_messages(messages)

        system_prompt = build_system_prompt(report, current_prices)

        bedrock = boto3.client("bedrock-runtime", region_name="ap-northeast-2")
        resp = bedrock.invoke_model(
            modelId="global.anthropic.claude-sonnet-4-6",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 8192,
                "system": system_prompt,
                "messages": messages
            })
        )
        body   = json.loads(resp["body"].read())
        answer = body["content"][0]["text"]
        return jsonify({"answer": answer})

    except Exception as e:
        logger.error(f"채팅 오류: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "stocks": len(_stock_map)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
