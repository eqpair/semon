"""
fetch_macro_history.py — 매크로 변수 1년치 일별 데이터 수집 (회귀 분석용)

eqai_news.py의 검증된 티커 사용. 회귀용이라 1년치 종가 시계열 수집.
한국 지수(^KS11/^KQ11)는 yfinance 오염 이슈로 제외 — 섹터가 국내라
KOSPI/KOSDAQ은 이미 잔차화 벤치마크로 쓰임.

산출:
  macro_history.parquet  — 날짜 인덱스, 컬럼=매크로변수, 값=종가
  macro_meta.json        — 각 변수 인과경로·가용성 메타

실행:
  python3 fetch_macro_history.py
"""
import json
import logging
import yfinance as yf
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── 매크로 변수: 티커 + 인과경로(어느 섹터에 왜 작용하는가) ──
# eqai_news.py의 MACRO_TICKERS 기반, 회귀에 의미있는 것만 선별
MACRO = {
    # 유가·에너지
    "WTI유가":       ("CL=F",      "정유(마진)·석유화학(납사)·조선(발주)·항공(연료,역)·해운(벙커유)"),
    # 금리
    "미국10Y":       ("^TNX",      "은행/보험(NIM↑)·증권(역)·바이오/신약(할인율↑,역)·성장주(역)"),
    "미국2Y":        ("^IRX",      "단기금리 민감 — 은행·핀테크"),
    # 달러·환율
    "달러인덱스":     ("DX-Y.NYB",  "수출주 전반(달러강세=원화약세 수혜 or 신흥국자금 역)"),
    "원달러":        ("KRW=X",     "반도체·자동차·음식료·화장품 수출주(원화약세 수혜)·항공(역)"),
    # 반도체
    "필라델피아반도체": ("^SOX",      "반도체_소자/장비/소재/패키징·IT부품(직접 동조)"),
    # 원자재
    "금":           ("GC=F",      "비철금속·안전자산 선호(위험회피 국면)"),
    "구리":         ("HG=F",      "비철금속·전력기기·건설(경기민감 산업금속)"),
    "알루미늄":      ("ALI=F",     "비철금속·자동차·2차전지(소재원가)"),
    # 위험선호·글로벌
    "VIX":          ("^VIX",      "전 섹터 위험선호(역)·방어주 상대강세"),
    "러셀2000":      ("^RUT",      "중소형주 위험선호(성장주 동조)"),
    "항셍":         ("^HSI",      "화장품(중국소비)·철강·석유화학·음식료(중국수출)"),
    "니케이":        ("^N225",     "일본 동조 — 참고(수출 경쟁국)"),
    "S&P500":       ("^GSPC",     "글로벌 위험선호 기준"),
    "나스닥":        ("^IXIC",     "성장/기술주 동조 — AI_SW·플랫폼·게임"),
}

PERIOD = "1y"   # 1년치


def main():
    logger.info(f"매크로 {len(MACRO)}개 변수 {PERIOD} 수집 시작")
    series = {}
    meta = {}
    for name, (ticker, causal) in MACRO.items():
        try:
            hist = yf.Ticker(ticker).history(period=PERIOD)
            if hist is None or len(hist) == 0:
                logger.warning(f"  [{name}] {ticker}: 데이터 없음 — 제외")
                meta[name] = {"ticker": ticker, "causal": causal, "status": "no_data", "n": 0}
                continue
            closes = hist["Close"].dropna()
            # 타임존 제거해서 날짜 인덱스 통일
            closes.index = pd.to_datetime(closes.index).tz_localize(None).normalize()
            series[name] = closes
            n = len(closes)
            # 최신성: 마지막 날짜
            last = closes.index[-1].strftime("%Y-%m-%d")
            nan_ratio = float(hist["Close"].isna().mean())
            status = "ok" if n >= 200 and nan_ratio < 0.1 else "sparse"
            meta[name] = {"ticker": ticker, "causal": causal, "status": status,
                          "n": n, "last": last, "nan_ratio": round(nan_ratio, 3)}
            logger.info(f"  [{name}] {ticker}: {n}일 (최신 {last}, NaN {nan_ratio:.1%}) {status}")
        except Exception as e:
            logger.warning(f"  [{name}] {ticker}: 실패 {e}")
            meta[name] = {"ticker": ticker, "causal": causal, "status": "error", "n": 0}

    if not series:
        logger.error("수집된 변수 없음 — 중단")
        return

    # 날짜 정렬해서 하나의 DataFrame으로 (outer join, 결측은 forward-fill)
    df = pd.DataFrame(series).sort_index()
    # 거래일 차이(미국/한국/선물 시장 캘린더 다름) → ffill로 정렬
    df = df.ffill().dropna(how="all")

    df.to_parquet("macro_history.parquet")
    json.dump(meta, open("macro_meta.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=2)

    # 요약
    print("\n" + "=" * 66)
    print(f"매크로 수집 완료: {len(series)}/{len(MACRO)}개 변수, {len(df)}일")
    print("=" * 66)
    ok = [n for n, m in meta.items() if m["status"] == "ok"]
    sparse = [n for n, m in meta.items() if m["status"] == "sparse"]
    failed = [n for n, m in meta.items() if m["status"] in ("no_data", "error")]
    print(f"  정상(ok): {len(ok)}개 — {', '.join(ok)}")
    if sparse:
        print(f"  듬성(sparse): {len(sparse)}개 — {', '.join(sparse)}")
    if failed:
        print(f"  실패: {len(failed)}개 — {', '.join(failed)}")
    print(f"\n저장: macro_history.parquet ({len(df)}일 × {len(df.columns)}변수), macro_meta.json")
    print(f"기간: {df.index[0].strftime('%Y-%m-%d')} ~ {df.index[-1].strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    main()