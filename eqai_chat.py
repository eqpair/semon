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

    return f"""당신은 한국 주식시장과 글로벌 매크로에 정통한 시니어 트레이딩 애널리스트입니다.

## 역할과 답변 원칙
- 질문의 주제와 범위에 맞게 자유롭게 답변하세요. 오늘 데이터에만 국한하지 마세요.
- 거시경제, 섹터 분석, 종목, 매매 전략, 리스크 관리 등 어떤 주제든 전문가 수준으로 답하세요.
- 오늘의 시장 데이터는 "현재 시장 상황"의 참고자료로만 활용하세요. 질문이 오늘과 무관하면 무시해도 됩니다.
- 구체적 수치, 역사적 사례, 논리적 인과관계를 포함해 답하세요.
- 한국어로 답변하되, 실제 트레이더에게 실용적인 인사이트를 제공하세요.
- 불확실한 내용은 솔직하게 "불확실하다"고 말하세요.

## 현재 시장 스냅샷 ({report.get('generated_at','')})
참고용 데이터입니다. 질문과 관련 있을 때만 활용하세요.

[글로벌 매크로]
{macro_lines}

[시장 심리] {report.get('global_sentiment', '-')} — {report.get('sentiment_reason', '-')}

[RRG 섹터]
- Leading: {', '.join(leading) or '없음'}
- Improving: {', '.join(improving) or '없음'}
- Weakening: {', '.join(weakening) or '없음'}
- Lagging: {', '.join(lagging) or '없음'}

[오늘 진입 후보]{picks_text}

[한국 시장] {ki.get('summary', '-')}

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
                "max_tokens": 2000,
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
