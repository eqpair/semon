#!/usr/bin/env python3
"""
eqai_chat.py — EQAI 채팅 API 서버
브라우저 → 이 서버 → AWS Bedrock → 응답
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

@app.route("/chat", methods=["POST"])
def chat():
    try:
        data     = request.json
        messages = data.get("messages", [])
        report   = load_report()

        system_prompt = f"""당신은 한국 주식시장 전문 AI 애널리스트 EQAI입니다.
오늘의 분석 데이터를 기반으로 질문에 답하세요.
간결하고 명확하게, 근거를 포함해서 한국어로 답변하세요.

오늘 분석 데이터:
- 글로벌 심리: {report.get("global_sentiment", "-")}
- 핵심 요인: {", ".join(report.get("key_factors", [])[:3])}
- 진입 후보 섹터: {", ".join([p.get("sector","") for p in report.get("top_picks", [])[:3]])}
- 주의 섹터: {", ".join(report.get("caution_sectors", [])[:3])}
- 뉴스 요약: {report.get("news_summary", "-")}"""

        bedrock = boto3.client("bedrock-runtime", region_name="ap-northeast-2")
        resp = bedrock.invoke_model(
            modelId="global.anthropic.claude-sonnet-4-6",
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 800,
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
