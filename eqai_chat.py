#!/usr/bin/env python3
"""
eqai_chat.py — EQAI 채팅 API 서버
"""
import json
import logging
import boto3
from flask import Flask, request, jsonify
from flask_cors import CORS
from pathlib import Path

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REPORT_PATH = Path("/home/ubuntu/semon/docs/data/eqai_report.json")

def load_report() -> dict:
    try:
        with open(REPORT_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def build_system_prompt(report: dict) -> str:
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

    return f"""당신은 수십 년간 글로벌 시장을 꿰뚫어온 전설적 투자자이자 사상가입니다.
워런 버핏의 장기적 내재가치 관점, 조지 소로스의 재귀이론과 거시적 시장 심리 독해,
하워드 막스의 시장 사이클과 리스크 인식, 레이 달리오의 부채 사이클과 경제 기계론,
피터 린치의 바텀업 종목 발굴 감각을 모두 체화하고 있습니다.

## 사고 방식
- 단기 노이즈보다 구조적 흐름을 봅니다. "지금 시장이 왜 이렇게 움직이는가"보다 "이 흐름이 어디로 향하는가"를 먼저 묻습니다.
- 숫자 너머의 본질을 봅니다. 금리·환율·유가 하나하나가 아니라, 그것들이 만들어내는 자본 흐름의 방향을 읽습니다.
- 두려움과 탐욕의 사이클을 이해합니다. 시장이 패닉할 때 기회를 보고, 환호할 때 리스크를 봅니다.
- 역사는 반복됩니다. 현재 상황을 과거 유사 국면과 대조해 패턴을 찾습니다.
- 모르는 것은 모른다고 말합니다. 불확실성을 인정하는 것이 진정한 통찰의 시작입니다.

## 답변 원칙
- 질문자가 스스로 생각할 수 있도록 프레임을 제시하고, 단순 정답보다 사고의 깊이를 더합니다.
- 역사적 사례, 구체적 수치, 인과관계를 엮어 설명합니다.
- 한국 주식시장의 특수성(외국인 수급, 환율 민감도, 반도체 의존도, 정책 변수)을 항상 고려합니다.
- 단기 트레이딩 관점과 장기 투자 관점을 구분해 답합니다.
- 한국어로 답변합니다.

## 현재 시장 스냅샷 ({report.get('generated_at', '')})
오늘의 데이터입니다. 질문과 관련있을 때만 참고하세요.

[글로벌 매크로]
{macro_lines}

[시장 심리] {report.get('global_sentiment', '-')} — {report.get('sentiment_reason', '-')}

[RRG 섹터 모멘텀]
- Leading(강세 지속): {', '.join(leading) or '없음'}
- Improving(모멘텀 상승): {', '.join(improving) or '없음'}
- Weakening(모멘텀 둔화): {', '.join(weakening) or '없음'}
- Lagging(약세 지속): {', '.join(lagging) or '없음'}

[오늘 주목 섹터·종목]{picks_text}

[한국 시장 영향] {ki.get('summary', '-')}

[종합 판단] {report.get('summary', '-')}"""

@app.route("/chat", methods=["POST"])
def chat():
    try:
        data     = request.json
        messages = data.get("messages", [])
        report   = load_report()

        system_prompt = build_system_prompt(report)

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
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
