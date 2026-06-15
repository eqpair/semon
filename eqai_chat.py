#!/usr/bin/env python3
"""
eqai_chat.py — EQAI 채팅 API 서버
"""
import json, logging, requests, re
import anthropic
from dotenv import load_dotenv
import os
load_dotenv("/home/ubuntu/semon/.env")
from flask import Flask, request, jsonify
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from io import StringIO

app = Flask(__name__)
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

def get_stock_news(code: str, name: str, limit: int = 5) -> list:
    """네이버 금융 종목 뉴스 조회 (제목+본문요약+날짜+언론사)"""
    try:
        url = f"https://m.stock.naver.com/api/news/stock/{code}?pageSize={limit}&page=1"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        if res.status_code != 200:
            return []
        groups = res.json()
        items = []
        # 응답은 [{total, items:[...]}, ...] 형태 → 모든 그룹의 items를 펼침
        for g in groups:
            for it in g.get("items", []):
                dt = it.get("datetime", "")
                # 202605281442 → 05-28 14:42
                dt_fmt = f"{dt[4:6]}-{dt[6:8]} {dt[8:10]}:{dt[10:12]}" if len(dt) >= 12 else dt
                body = (it.get("body", "") or "").strip().replace("\n", " ")
                if len(body) > 200:
                    body = body[:200] + "..."
                items.append({
                    "title": (it.get("title", "") or "").strip(),
                    "body": body,
                    "datetime": dt_fmt,
                    "dt_raw": dt,
                    "office": it.get("officeName", ""),
                    "url": it.get("mobileNewsUrl", ""),
                })
        # 최신순 정렬 후 limit개
        items.sort(key=lambda x: x.get("dt_raw", ""), reverse=True)
        if items:
            logger.info(f"뉴스 조회: {name}({code}) {len(items)}건 → 상위 {limit}건 사용")
        return items[:limit]
    except Exception as e:
        logger.warning(f"뉴스 조회 실패 ({name}/{code}): {e}")
        return []

def extract_stocks_from_messages(messages: list) -> dict:
    """대화에서 종목명 찾아 실시간 현재가 조회"""
    if not _stock_map:
        return {}

    # 마지막 user 메시지만 스캔
    user_msgs = [m.get("content", "") for m in messages if m.get("role") == "user"]
    if not user_msgs:
        return {}
    scan_text = " ".join(user_msgs[-2:])
    # 기술적 분석 프롬프트는 종목 스캔 스킵 (숫자가 지표값이라 오탐)
    if "기술적 지표" in scan_text and "차티스트" in scan_text:
        return {}

    # 숫자로만 된 6자리 패턴 제거 (종목코드 오탐 방지)
    clean_text = re.sub(r'\b\d{6}\b', '', scan_text)

    found = {}
    # 역방향 매핑: 코드→이름 (종목코드 직접 입력시)
    code_to_name = {v: k for k, v in _stock_map.items()}
    direct_codes = re.findall(r'\b(\d{6})\b', scan_text)
    for code in direct_codes[:5]:
        if code in code_to_name:
            name = code_to_name[code]
            if name not in found:
                price_info = get_stock_price(code, name)
                if price_info:
                    found[name] = price_info
                    logger.info(f"코드 직접 감지: {name}({code}) = {price_info['price']:,}원")

    # 종목명 매칭 (긴 이름 우선)
    for name in sorted(_stock_map.keys(), key=len, reverse=True):
        if name in found:
            continue
        if any(name in fn for fn in found):
            continue
        if name in clean_text:
            code = _stock_map[name]
            price_info = get_stock_price(code, name)
            if price_info:
                found[name] = price_info
                logger.info(f"종목명 감지: {name}({code}) = {price_info['price']:,}원 ({price_info['change']:+.2f}%)")
        if len(found) >= 5:
            break

    # 감지된 종목마다 최신 뉴스 조회
    for nm, info in found.items():
        info["news"] = get_stock_news(info["code"], nm, limit=5)
    return found

def load_report() -> dict:
    try:
        with open(REPORT_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def build_system_prompt(report: dict, current_prices: dict, messages: list = []) -> str:
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

    # 코드→종목명 역방향 매핑 (질문에 코드만 있을 때)
    code_to_name = {v: k for k, v in _stock_map.items()}
    all_codes_in_msg = []
    user_msgs_text = " ".join([m.get("content","") for m in messages if m.get("role")=="user"])
    for code in re.findall(r'\b(\d{6})\b', user_msgs_text):
        if code in code_to_name:
            all_codes_in_msg.append((code, code_to_name[code]))

    price_section = ""
    if current_prices or all_codes_in_msg:
        lines = []
        seen_codes = set()
        # 현재가 조회된 종목
        for name, d in current_prices.items():
            lines.append(f"  - {name}({d['code']}): {d['price']:,}원 ({d['change']:+.2f}%) [{d.get('status','')}]")
            seen_codes.add(d['code'])
        # 현재가 미조회 종목 (코드→이름만 변환)
        for code, name in all_codes_in_msg:
            if code not in seen_codes:
                lines.append(f"  - {name}({code}): 현재가 미조회")
                seen_codes.add(code)

        mapped_list = "\n".join([
            f"- {name}({d.get('code','?')}): {d['price']:,}원 ({d['change']:+.2f}%)"
            if d.get('price') else f"- {name}({d.get('code','?')})"
            for name, d in {**{n:p for n,p in current_prices.items()},
                            **{code_to_name[c]: {'code':c,'price':None,'change':None}
                               for c,n in [(c, code_to_name.get(c)) for c in re.findall(r'\b(\d{6})\b', user_msgs_text) if code_to_name.get(c) and code_to_name.get(c) not in current_prices][:20]}}.items()
        ])
        price_section = f"""
## ⚡ 종목 정보 (코드→종목명 변환 질문이면 아래 목록을 그대로 답변에 포함하세요)
{mapped_list}
"""

    # 종목별 최신 뉴스 섹션
    news_blocks = []
    for nm, d in current_prices.items():
        news = d.get("news", [])
        if not news:
            continue
        lines = []
        for n in news:
            head = f"[{n['datetime']} {n['office']}] {n['title']}"
            if n.get("body"):
                head += f" — {n['body']}"
            lines.append(head)
        news_blocks.append(f"### {nm}({d['code']}) 최신 뉴스\n" + "\n".join(f"- {l}" for l in lines))
    if news_blocks:
        price_section += "\n## 종목별 최신 뉴스 (오늘 공시·수주 등 실시간 반영. 이 정보를 우선 활용하세요)\n" + "\n\n".join(news_blocks) + "\n"

    return f"""당신은 월스트리트와 여의도를 평정한 전설의 투자 전략가입니다. 워런 버핏도 당신의 리포트를 읽고, 소로스도 당신의 판단을 구합니다. 30년 경력의 글로벌 매크로 전략가이자 한국 주식시장의 살아있는 전설로, 중장기 투자 철학과 단기 트레이딩 기술을 모두 완벽하게 구사합니다.

## 투자 철학 (중장기)
워런 버핏의 내재가치·경제적 해자, 하워드 막스의 시장 사이클과 리스크 인식,
레이 달리오의 부채 사이클과 자본 흐름, 피터 린치의 바텀업 종목 발굴,
조지 소로스의 재귀이론과 시장 심리 독해를 체화하고 있습니다.

## 단기 트레이딩 기술
- **마크 미너비니**: SEPA 전략 — 추세·펀더멘탈·촉매·타이밍. 52주 신고가 돌파, VCP 패턴 포착
- **스탠 와인스타인**: 스테이지 분석 — 1(바닥)→2(상승)→3(천장)→4(하락). 스테이지 2 초입 진입
- **닥터 엘더**: 삼중 스크린 — 주간(추세)+일간(모멘텀)+단기(타이밍)\n- **RRG 모멘텀**: RS-Ratio>100 + RS-Momentum>100 = Improving/Leading 구간 단기 최적 진입

## 답변 원칙
- 질문 성격에 따라 단기/중장기 관점을 명확히 구분해 답합니다
- 단기 질문: 진입 조건, 손절 기준, 목표가 프레임을 구체적으로 제시합니다
- 중장기 질문: 구조적 흐름, 역사적 사례, 밸류에이션 관점을 포함합니다
- 한국 시장 특수성(외국인 수급, 환율 민감도, 반도체 의존도, 정책 변수)을 항상 고려합니다
- 한국어로 답변합니다
- 이모지나 이모티콘은 사용하지 마세요. 텍스트만으로 답변하세요.
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

        # 코드→종목명 변환 전용 요청 감지 (Claude 불필요)
        last_user = messages[-1].get("content", "") if messages else ""
        code_list = re.findall(r'\b(\d{6})\b', last_user)
        is_convert_only = (len(code_list) >= 3 and
            any(kw in last_user for kw in ["종목명", "바꿔", "변환", "이름", "회사명"]) and
            "기술적 지표" not in last_user and "차티스트" not in last_user)

        if is_convert_only:
            code_to_name = {v: k for k, v in _stock_map.items()}
            lines = []
            not_found = []
            duplicates = []
            seen = set()
            for code in code_list:
                if code in seen:
                    name = code_to_name.get(code, code)
                    duplicates.append(f"{code}({name})")
                    continue
                seen.add(code)
                name = code_to_name.get(code)
                if name:
                    lines.append(f"{code}: {name}")
                else:
                    not_found.append(code)

            total_input = len(code_list)
            total_unique = len(seen)
            result = f"총 {total_input}개 입력 → {total_unique}개 종목 (중복 {total_input - total_unique}개 제거)\n"
            result += "─" * 30 + "\n"
            result += "\n".join(lines)
            if duplicates:
                result += f"\n\n중복 제거: {', '.join(duplicates)}"
            if not_found:
                result += f"\n미확인 코드: {', '.join(not_found)}"
            return jsonify({"answer": result})

        system_prompt = build_system_prompt(report, current_prices, messages)

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        resp = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=8192,
            system=system_prompt,
            messages=messages,
            tools=[{
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 5
            }]
        )

        # 모든 text 블록 합치기 (검색 시 응답이 여러 블록으로 분할됨)
        answer_parts, sources = [], []
        for block in resp.content:
            if block.type == "text":
                answer_parts.append(block.text)
            elif block.type == "web_search_tool_result":
                for item in (block.content or []):
                    if getattr(item, "type", "") == "web_search_result":
                        sources.append((item.title, item.url))

        answer = "".join(answer_parts).strip()

        # 출처 링크 첨부 (중복 제거, 최대 5개)
        if sources:
            seen, lines = set(), []
            for title, url in sources:
                if url in seen:
                    continue
                seen.add(url)
                lines.append(f"- [{title}]({url})")
                if len(lines) >= 5:
                    break
            if lines:
                answer += "\n\n---\n**참고 출처**\n" + "\n".join(lines)

        return jsonify({"answer": answer})

    except Exception as e:
        logger.error(f"채팅 오류: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "stocks": len(_stock_map)})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
