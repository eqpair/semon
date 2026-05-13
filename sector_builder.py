"""
sector_builder.py
─────────────────
상관관계 기반 섹터 클러스터링 도구

흐름:
  1. 네이버 증권에서 전종목 수집 (시총 500억+, 주가 1000원+, 거래대금 1억+)
  2. 500일 OHLCV 수집 (네이버 fchart)
  3. 멀티스케일 상관관계 계산 (20일×0.2 + 60일×0.3 + 120일×0.5)
  4. Ward 계층적 클러스터링 + 실루엣 스코어로 최적 클러스터 수 탐색
  5. 결과 저장: sector_result.json, sector_result.csv
  6. 인터랙티브 HTML 시각화 생성

실행: python3 sector_builder.py [--clusters N] [--out ./output]
"""

import asyncio
import aiohttp
import csv
import json
import argparse
import warnings
import logging
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
from scipy.cluster.hierarchy import linkage, fcluster, dendrogram
from scipy.spatial.distance import squareform
from sklearn.metrics import silhouette_score
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── 필터 기준 ─────────────────────────────────────────────────
MIN_PRICE      = 1_000   # 원
MIN_MARKET_CAP = 500     # 억원
MIN_TRADE_AMT  = 1       # 억원
MIN_OHLCV_DAYS = 130     # 최소 거래일 (120일 상관관계 계산용)

# 제외 업종 (펀드·리츠·불명확)
EXCLUDE_GROUPS = {"창업투자", "부동산", "기타금융", "기타"}

# ── URL ───────────────────────────────────────────────────────
BASE        = "https://finance.naver.com"
GROUP_URL   = BASE + "/sise/sise_group.naver?type=upjong"
DETAIL_URL  = BASE + "/sise/sise_group_detail.naver?type=upjong&no={no}"
PRICE_URL   = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{code}"
SISE_URL    = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=500&requestType=0"
SUMMARY_URL = "https://api.finance.naver.com/service/itemSummary.nhn?itemcode={code}"
HEADERS     = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
SUMMARY_HDR = {**HEADERS, "Referer": "https://finance.naver.com/"}

# ── 멀티스케일 가중치 ─────────────────────────────────────────
CORR_WINDOWS  = [20, 60, 120]
CORR_WEIGHTS  = [0.2, 0.3, 0.5]


# ══════════════════════════════════════════════════════════════
# 1. 전종목 수집
# ══════════════════════════════════════════════════════════════

async def fetch_group_nos(session):
    async with session.get(GROUP_URL, headers=HEADERS) as resp:
        html = await resp.text(encoding="euc-kr")
    soup  = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a", href=lambda h: h and "upjong" in h and "no=" in h)
    return [(l["href"].split("no=")[-1], l.text.strip()) for l in links if l.text.strip()]


async def fetch_group_stocks(session, no, group_name):
    url = DETAIL_URL.format(no=no)
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            html = await resp.text(encoding="euc-kr")
    except Exception:
        return []
    soup   = BeautifulSoup(html, "html.parser")
    stocks = []
    for a in soup.find_all("a", href=lambda h: h and "item/main" in str(h)):
        href = a.get("href", "")
        code = href.split("code=")[-1].strip() if "code=" in href else ""
        name = a.text.strip()
        if code and len(code) == 6 and name:
            stocks.append({"code": code, "name": name, "naver_group": group_name})
    return stocks


async def fetch_price_info(session, code):
    url = PRICE_URL.format(code=code)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return {}
            data  = await resp.json(content_type=None)
            areas = data.get("result", {}).get("areas", [])
            if not areas or not areas[0].get("datas"):
                return {}
            d         = areas[0]["datas"][0]
            price     = float(d.get("nv", 0) or 0)
            trade_amt = float(d.get("aa", 0) or 0) / 100_000_000
            ms        = d.get("mt", "")
            return {"price": price, "trade_amt": trade_amt,
                    "market": "KOSPI" if ms == "1" else "KOSDAQ"}
    except Exception:
        return {}


async def fetch_market_cap(session, code) -> int:
    url = SUMMARY_URL.format(code=code)
    try:
        async with session.get(url, headers=SUMMARY_HDR,
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return 0
            data = await resp.json(content_type=None)
            val  = data.get("marketSum") or 0
            return int(val) // 100
    except Exception:
        return 0


async def collect_universe(session) -> list[dict]:
    """필터 통과 전종목 수집"""
    logger.info("업종 목록 수집 중...")
    groups = await fetch_group_nos(session)
    logger.info(f"총 {len(groups)}개 업종")

    all_stocks: dict[str, dict] = {}
    for i, (no, name) in enumerate(groups):
        stocks = await fetch_group_stocks(session, no, name)
        for s in stocks:
            if s["code"] not in all_stocks:
                all_stocks[s["code"]] = s
        print(f"\r  종목 수집: {i+1}/{len(groups)} 업종 ({len(all_stocks)}개)", end="", flush=True)
        await asyncio.sleep(0.2)
    print()
    logger.info(f"전체 종목: {len(all_stocks)}개")

    # 현재가·거래대금
    logger.info("현재가·거래대금 수집 중...")
    codes   = list(all_stocks.keys())
    results = []
    for i in range(0, len(codes), 50):
        batch = codes[i:i+50]
        tasks = [fetch_price_info(session, c) for c in batch]
        infos = await asyncio.gather(*tasks)
        for code, info in zip(batch, infos):
            s = all_stocks[code].copy()
            s.update(info)
            results.append(s)
        print(f"\r  현재가: {min(i+50, len(codes))}/{len(codes)}개", end="", flush=True)
        await asyncio.sleep(0.3)
    print()

    # 1차 필터
    pre = [s for s in results
           if s.get("price", 0) >= MIN_PRICE and s.get("trade_amt", 0) >= MIN_TRADE_AMT]
    logger.info(f"1차 필터 후: {len(pre)}개 (시총 수집 대상)")

    # 시총
    logger.info("시총 수집 중...")
    for i in range(0, len(pre), 20):
        batch = pre[i:i+20]
        tasks = [fetch_market_cap(session, s["code"]) for s in batch]
        caps  = await asyncio.gather(*tasks)
        for s, cap in zip(batch, caps):
            s["market_cap"] = cap
        print(f"\r  시총: {min(i+20, len(pre))}/{len(pre)}개", end="", flush=True)
        await asyncio.sleep(0.5)
    print()

    # 업종 제외 필터
    pre = [s for s in pre if s.get("naver_group", "") not in EXCLUDE_GROUPS]

    filtered = [s for s in pre if s.get("market_cap", 0) >= MIN_MARKET_CAP]
    logger.info(f"최종 유니버스: {len(filtered)}개")
    return filtered


# ══════════════════════════════════════════════════════════════
# 2. OHLCV 수집
# ══════════════════════════════════════════════════════════════

async def fetch_ohlcv(session, code) -> tuple[str, list[float] | None]:
    url = SISE_URL.format(code=code)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return code, None
            text = await resp.text(encoding="euc-kr")
        soup  = BeautifulSoup(text, "html.parser")
        items = soup.find_all("item")
        closes = []
        for item in items:
            parts = item.get("data", "").split("|")
            if len(parts) >= 5:
                try:
                    closes.append(float(parts[4]))
                except ValueError:
                    continue
        return code, closes if len(closes) >= MIN_OHLCV_DAYS else None
    except Exception as e:
        logger.warning(f"OHLCV 오류 ({code}): {e}")
        return code, None


async def collect_ohlcv(universe: list[dict]) -> dict[str, list[float]]:
    codes  = [s["code"] for s in universe]
    result = {}
    logger.info(f"OHLCV 수집 중 ({len(codes)}개)...")
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(codes), 10):
            batch    = codes[i:i+10]
            tasks    = [fetch_ohlcv(session, c) for c in batch]
            responses = await asyncio.gather(*tasks)
            for code, closes in responses:
                if closes:
                    result[code] = closes
            print(f"\r  OHLCV: {min(i+10, len(codes))}/{len(codes)}개 (성공: {len(result)}개)", end="", flush=True)
            await asyncio.sleep(1.0)
    print()
    logger.info(f"OHLCV 성공: {len(result)}/{len(codes)}개")
    return result


# ══════════════════════════════════════════════════════════════
# 3. 멀티스케일 상관관계
# ══════════════════════════════════════════════════════════════

def calc_returns(closes: list[float], window: int) -> list[float]:
    """window일 롤링 수익률"""
    arr = np.array(closes)
    if len(arr) <= window:
        return []
    return list((arr[window:] - arr[:-window]) / arr[:-window])


def calc_multiscale_corr(ohlcv: dict[str, list[float]]) -> tuple[np.ndarray, list[str]]:
    """멀티스케일 상관관계 행렬 계산"""
    codes = list(ohlcv.keys())
    n     = len(codes)
    logger.info(f"상관관계 계산 중 ({n}개 종목, 윈도우: {CORR_WINDOWS})...")

    corr_combined  = np.zeros((n, n))
    weight_applied = np.zeros((n, n))  # 실제 적용된 가중치 추적

    for window, weight in zip(CORR_WINDOWS, CORR_WEIGHTS):
        # 각 종목의 롤링 수익률
        ret_map = {}
        for code in codes:
            r = calc_returns(ohlcv[code], window)
            if len(r) >= 30:
                ret_map[code] = r

        valid = [c for c in codes if c in ret_map]
        if len(valid) < 2:
            continue

        # 길이 맞추기
        min_len = min(len(ret_map[c]) for c in valid)
        mat     = np.array([ret_map[c][-min_len:] for c in valid])  # (n_valid, T)

        # 상관행렬
        corr = np.corrcoef(mat)
        corr = np.nan_to_num(corr, nan=0.0)

        # valid 인덱스에만 가중 누적
        idx = [codes.index(c) for c in valid]
        for i_local, i_global in enumerate(idx):
            for j_local, j_global in enumerate(idx):
                corr_combined[i_global, j_global]  += weight * corr[i_local, j_local]
                weight_applied[i_global, j_global] += weight

        logger.info(f"  {window}일 상관관계 완료 (유효 종목: {len(valid)}개, 가중치: {weight})")

    # 실제 적용된 가중치로 정규화 (윈도우별 유효 종목이 달라도 안전)
    mask = weight_applied > 0
    corr_combined[mask] /= weight_applied[mask]

    # 대각선 1, 대칭화, 범위 클리핑
    np.fill_diagonal(corr_combined, 1.0)
    corr_combined = (corr_combined + corr_combined.T) / 2  # 부동소수점 비대칭 제거
    corr_combined = np.clip(corr_combined, -1.0, 1.0)

    return corr_combined, codes


# ══════════════════════════════════════════════════════════════
# 4. 클러스터링
# ══════════════════════════════════════════════════════════════

def find_optimal_clusters(corr: np.ndarray, codes: list[str],
                           min_k: int = 20, max_k: int = 50) -> tuple[np.ndarray, int, list[float]]:
    """Ward 계층적 클러스터링 + 실루엣 스코어로 최적 k 탐색"""
    dist = np.sqrt(np.clip((1 - corr) / 2, 0, 1))  # 상관→거리 변환
    np.fill_diagonal(dist, 0)
    condensed = squareform(dist)
    Z         = linkage(condensed, method="ward")

    scores = []
    logger.info(f"실루엣 스코어 계산 중 (k={min_k}~{max_k})...")
    for k in range(min_k, max_k + 1):
        labels = fcluster(Z, k, criterion="maxclust")
        if len(set(labels)) < 2:
            scores.append(-1)
            continue
        score = silhouette_score(dist, labels, metric="precomputed")
        scores.append(score)
        print(f"\r  k={k}: 실루엣={score:.4f}", end="", flush=True)
    print()

    best_k    = min_k + np.argmax(scores)
    best_labels = fcluster(Z, best_k, criterion="maxclust")
    logger.info(f"최적 클러스터 수: {best_k} (실루엣: {max(scores):.4f})")
    return best_labels, best_k, scores, Z, dist


def apply_clusters(corr: np.ndarray, codes: list[str], n_clusters: int):
    """지정된 클러스터 수로 적용"""
    dist = np.sqrt(np.clip((1 - corr) / 2, 0, 1))
    np.fill_diagonal(dist, 0)
    condensed = squareform(dist)
    Z         = linkage(condensed, method="ward")
    labels    = fcluster(Z, n_clusters, criterion="maxclust")
    return labels, Z, dist


# ══════════════════════════════════════════════════════════════
# 5. 결과 저장
# ══════════════════════════════════════════════════════════════

def build_cluster_result(labels, codes, universe, ohlcv, corr, n_clusters):
    """클러스터링 결과를 정리된 딕셔너리로"""
    name_map   = {s["code"]: s["name"]       for s in universe}
    cap_map    = {s["code"]: s.get("market_cap", 0) for s in universe}
    market_map = {s["code"]: s.get("market", "") for s in universe}
    group_map  = {s["code"]: s.get("naver_group", "") for s in universe}

    clusters = {}
    for i, code in enumerate(codes):
        cl = int(labels[i])
        if cl not in clusters:
            clusters[cl] = []
        clusters[cl].append(code)

    result = {}
    for cl_id, cl_codes in sorted(clusters.items()):
        # 클러스터 내 평균 상관계수 (결속력)
        idx = [codes.index(c) for c in cl_codes]
        sub = corr[np.ix_(idx, idx)]
        mask = np.ones(sub.shape, dtype=bool)
        np.fill_diagonal(mask, False)
        cohesion = float(sub[mask].mean()) if mask.any() else 1.0

        # 시총 기준 대표 종목
        sorted_by_cap = sorted(cl_codes, key=lambda c: cap_map.get(c, 0), reverse=True)
        rep_code = sorted_by_cap[0]

        stocks = []
        for code in sorted_by_cap:
            stocks.append({
                "code":        code,
                "name":        name_map.get(code, ""),
                "market":      market_map.get(code, ""),
                "market_cap":  cap_map.get(code, 0),
                "naver_group": group_map.get(code, ""),
            })

        result[cl_id] = {
            "cluster_id":   cl_id,
            "size":         len(cl_codes),
            "cohesion":     round(cohesion, 4),
            "rep_code":     rep_code,
            "rep_name":     name_map.get(rep_code, ""),
            "label":        "",   # 사람이 나중에 채움
            "stocks":       stocks,
        }

    return result


class _NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer): return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.ndarray): return obj.tolist()
        return super().default(obj)


def save_results(cluster_result, out_dir: Path, meta: dict):
    out_dir.mkdir(parents=True, exist_ok=True)

    # JSON
    payload = {"meta": meta, "clusters": cluster_result}
    json_path = out_dir / "sector_result.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, cls=_NumpyEncoder)
    logger.info(f"JSON 저장: {json_path}")

    # CSV
    csv_path = out_dir / "sector_result.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["cluster_id", "rep_name", "cohesion",
                                                "size", "code", "name", "market",
                                                "market_cap", "naver_group"])
        writer.writeheader()
        for cl_id, cl in cluster_result.items():
            for s in cl["stocks"]:
                writer.writerow({
                    "cluster_id": cl_id,
                    "rep_name":   cl["rep_name"],
                    "cohesion":   cl["cohesion"],
                    "size":       cl["size"],
                    **s,
                })
    logger.info(f"CSV 저장: {csv_path}")

    return json_path, csv_path


# ══════════════════════════════════════════════════════════════
# 6. HTML 시각화
# ══════════════════════════════════════════════════════════════

def generate_html(cluster_result, corr, codes, silhouette_scores,
                  min_k, best_k, out_dir: Path, meta: dict) -> Path:
    """인터랙티브 HTML 시각화 생성"""

    # 클러스터별 색상
    import colorsys
    n = len(cluster_result)
    colors = []
    for i in range(n):
        h = i / n
        r, g, b = colorsys.hsv_to_rgb(h, 0.65, 0.90)
        colors.append(f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}")

    # 클러스터 데이터 JSON
    clusters_js = json.dumps(cluster_result, ensure_ascii=False)

    # 실루엣 스코어 차트 데이터
    sil_labels = list(range(min_k, min_k + len(silhouette_scores)))
    sil_data   = [round(s, 4) for s in silhouette_scores]

    # 상관관계 히트맵 (샘플링: 최대 200개)
    sample_n  = min(200, len(codes))
    step      = max(1, len(codes) // sample_n)
    s_idx     = list(range(0, len(codes), step))[:sample_n]
    s_codes   = [codes[i] for i in s_idx]
    s_corr    = corr[np.ix_(s_idx, s_idx)].tolist()

    # 라벨 매핑 (코드 → 클러스터 ID)
    code_to_cl = {}
    for cl_id, cl in cluster_result.items():
        for s in cl["stocks"]:
            code_to_cl[s["code"]] = cl_id

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sector Builder — 클러스터링 결과</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<style>
  :root {{
    --bg: #0a0e1a;
    --bg2: #111827;
    --bg3: #1a2235;
    --border: #1e2d45;
    --accent: #3b82f6;
    --accent2: #60a5fa;
    --text: #e2e8f0;
    --text2: #94a3b8;
    --text3: #64748b;
    --good: #10b981;
    --warn: #f59e0b;
    --danger: #ef4444;
    --font-mono: 'JetBrains Mono', 'Fira Code', monospace;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: 'Pretendard', 'Noto Sans KR', sans-serif;
    font-size: 14px;
    line-height: 1.6;
  }}

  /* ── 헤더 */
  .header {{
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    border-bottom: 1px solid var(--border);
    padding: 24px 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }}
  .header-title {{ font-size: 22px; font-weight: 700; letter-spacing: -0.5px; }}
  .header-title span {{ color: var(--accent2); }}
  .header-meta {{ color: var(--text2); font-size: 12px; font-family: var(--font-mono); }}

  /* ── 탭 */
  .tabs {{
    display: flex;
    gap: 2px;
    padding: 16px 32px 0;
    background: var(--bg2);
    border-bottom: 1px solid var(--border);
  }}
  .tab {{
    padding: 10px 20px;
    cursor: pointer;
    color: var(--text2);
    font-size: 13px;
    font-weight: 500;
    border-bottom: 2px solid transparent;
    transition: all 0.2s;
  }}
  .tab:hover {{ color: var(--text); }}
  .tab.active {{ color: var(--accent2); border-bottom-color: var(--accent); }}

  /* ── 패널 */
  .panel {{ display: none; padding: 24px 32px; }}
  .panel.active {{ display: block; }}

  /* ── 통계 카드 */
  .stats-row {{
    display: flex;
    gap: 16px;
    margin-bottom: 24px;
    flex-wrap: wrap;
  }}
  .stat-card {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 20px;
    flex: 1;
    min-width: 140px;
  }}
  .stat-label {{ font-size: 11px; color: var(--text3); text-transform: uppercase; letter-spacing: 1px; margin-bottom: 6px; }}
  .stat-value {{ font-size: 28px; font-weight: 700; font-family: var(--font-mono); color: var(--accent2); }}
  .stat-sub {{ font-size: 11px; color: var(--text3); margin-top: 2px; }}

  /* ── 클러스터 그리드 */
  .cluster-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 16px;
  }}
  .cluster-card {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    overflow: hidden;
    transition: border-color 0.2s;
  }}
  .cluster-card:hover {{ border-color: var(--accent); }}
  .cluster-header {{
    padding: 14px 16px;
    display: flex;
    align-items: center;
    gap: 10px;
    border-bottom: 1px solid var(--border);
  }}
  .cluster-dot {{
    width: 12px; height: 12px;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  .cluster-title {{ font-weight: 600; font-size: 14px; flex: 1; }}
  .cluster-badge {{
    font-size: 11px;
    font-family: var(--font-mono);
    background: var(--bg3);
    padding: 2px 8px;
    border-radius: 20px;
    color: var(--text2);
  }}
  .cohesion-bar-wrap {{
    padding: 8px 16px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    gap: 10px;
  }}
  .cohesion-bar-bg {{
    flex: 1;
    height: 4px;
    background: var(--bg3);
    border-radius: 2px;
    overflow: hidden;
  }}
  .cohesion-bar-fill {{
    height: 100%;
    border-radius: 2px;
    background: linear-gradient(90deg, var(--accent), var(--good));
  }}
  .cohesion-val {{
    font-size: 11px;
    font-family: var(--font-mono);
    color: var(--text2);
    min-width: 36px;
    text-align: right;
  }}
  .stock-list {{
    padding: 10px 16px;
    max-height: 200px;
    overflow-y: auto;
  }}
  .stock-list::-webkit-scrollbar {{ width: 4px; }}
  .stock-list::-webkit-scrollbar-track {{ background: transparent; }}
  .stock-list::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 2px; }}
  .stock-row {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 0;
    border-bottom: 1px solid var(--border);
    font-size: 12px;
  }}
  .stock-row:last-child {{ border-bottom: none; }}
  .stock-code {{ font-family: var(--font-mono); color: var(--text3); width: 52px; flex-shrink: 0; }}
  .stock-name {{ flex: 1; color: var(--text); }}
  .stock-cap  {{ font-family: var(--font-mono); color: var(--text2); font-size: 11px; }}
  .stock-market {{
    font-size: 10px;
    padding: 1px 5px;
    border-radius: 3px;
    margin-left: 4px;
    background: var(--bg3);
    color: var(--text3);
  }}
  .rep-star {{ color: var(--warn); margin-right: 2px; }}

  /* ── 실루엣 차트 */
  .chart-wrap {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
    margin-bottom: 24px;
  }}
  .chart-title {{
    font-size: 13px;
    font-weight: 600;
    color: var(--text2);
    margin-bottom: 16px;
  }}
  svg text {{ font-family: 'Pretendard', sans-serif; }}

  /* ── 히트맵 */
  .heatmap-wrap {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px;
    overflow: auto;
  }}

  /* ── 검색 */
  .search-bar {{
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
    align-items: center;
  }}
  .search-input {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 14px;
    color: var(--text);
    font-size: 13px;
    width: 260px;
    outline: none;
  }}
  .search-input:focus {{ border-color: var(--accent); }}
  .sort-btn {{
    background: var(--bg2);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 14px;
    color: var(--text2);
    font-size: 12px;
    cursor: pointer;
    transition: all 0.15s;
  }}
  .sort-btn:hover, .sort-btn.active {{ background: var(--accent); color: white; border-color: var(--accent); }}

  /* ── 스크롤바 */
  ::-webkit-scrollbar {{ width: 6px; height: 6px; }}
  ::-webkit-scrollbar-track {{ background: var(--bg); }}
  ::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 3px; }}
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="header-title">Sector <span>Builder</span></div>
    <div class="header-meta" style="margin-top:4px">
      생성: {meta.get('generated_at', '')} &nbsp;|&nbsp;
      유니버스: {meta.get('universe_size', 0)}개 &nbsp;|&nbsp;
      클러스터: {meta.get('n_clusters', 0)}개 &nbsp;|&nbsp;
      실루엣: {meta.get('silhouette', 0):.4f}
    </div>
  </div>
</div>

<div class="tabs">
  <div class="tab active" onclick="showTab('clusters')">클러스터 목록</div>
  <div class="tab" onclick="showTab('silhouette')">실루엣 분석</div>
  <div class="tab" onclick="showTab('heatmap')">상관관계 히트맵</div>
</div>

<!-- ── 패널 1: 클러스터 목록 -->
<div id="tab-clusters" class="panel active">
  <div class="stats-row" id="stats-row"></div>
  <div class="search-bar">
    <input class="search-input" id="search" placeholder="종목명 또는 코드 검색..." oninput="filterClusters()">
    <button class="sort-btn active" id="sort-cohesion" onclick="sortBy('cohesion')">결속력순</button>
    <button class="sort-btn" id="sort-size" onclick="sortBy('size')">종목수순</button>
    <button class="sort-btn" id="sort-id" onclick="sortBy('id')">ID순</button>
  </div>
  <div class="cluster-grid" id="cluster-grid"></div>
</div>

<!-- ── 패널 2: 실루엣 분석 -->
<div id="tab-silhouette" class="panel">
  <div class="chart-wrap">
    <div class="chart-title">실루엣 스코어 vs 클러스터 수</div>
    <svg id="sil-chart" width="100%" height="320"></svg>
  </div>
</div>

<!-- ── 패널 3: 히트맵 -->
<div id="tab-heatmap" class="panel">
  <div class="heatmap-wrap">
    <div class="chart-title">
      상관관계 히트맵 (샘플 {sample_n}개 종목, 클러스터 정렬)
      <span style="color:var(--text3); font-weight:400; font-size:11px; margin-left:8px">
        파랑=음의상관 / 흰색=무상관 / 빨강=양의상관
      </span>
    </div>
    <svg id="heatmap" style="display:block;"></svg>
  </div>
</div>

<script>
// ── 데이터
const CLUSTERS   = {clusters_js};
const SIL_LABELS = {json.dumps(sil_labels)};
const SIL_DATA   = {json.dumps(sil_data)};
const BEST_K     = {best_k};
const S_CODES    = {json.dumps(s_codes)};
const S_CORR     = {json.dumps(s_corr)};
const CODE_TO_CL = {json.dumps(code_to_cl)};
const COLORS     = {json.dumps(colors)};

// 클러스터 ID → 색상 인덱스
const clIds = Object.keys(CLUSTERS).map(Number).sort((a,b)=>a-b);
const clColorMap = {{}};
clIds.forEach((id, i) => {{ clColorMap[id] = COLORS[i % COLORS.length]; }});

// ── 탭 전환
function showTab(name) {{
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
  if (name === 'silhouette') drawSilhouette();
  if (name === 'heatmap')    drawHeatmap();
}}

// ── 통계 카드
function renderStats() {{
  const total   = Object.values(CLUSTERS).reduce((s,c) => s + c.size, 0);
  const sizes   = Object.values(CLUSTERS).map(c => c.size);
  const cohs    = Object.values(CLUSTERS).map(c => c.cohesion);
  const avgCoh  = (cohs.reduce((a,b)=>a+b,0)/cohs.length).toFixed(3);
  const minSize = Math.min(...sizes), maxSize = Math.max(...sizes);

  document.getElementById('stats-row').innerHTML = `
    <div class="stat-card">
      <div class="stat-label">전체 종목</div>
      <div class="stat-value">${{total}}</div>
      <div class="stat-sub">유니버스</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">클러스터 수</div>
      <div class="stat-value">${{clIds.length}}</div>
      <div class="stat-sub">최적 K</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">평균 결속력</div>
      <div class="stat-value">${{avgCoh}}</div>
      <div class="stat-sub">내부 평균 상관계수</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">클러스터 크기</div>
      <div class="stat-value">${{minSize}}~${{maxSize}}</div>
      <div class="stat-sub">최소~최대 종목수</div>
    </div>
  `;
}}

// ── 클러스터 카드 렌더
let _sortMode = 'cohesion';
function sortBy(mode) {{
  _sortMode = mode;
  document.querySelectorAll('.sort-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('sort-' + mode).classList.add('active');
  renderClusters();
}}

function filterClusters() {{ renderClusters(); }}

function renderClusters() {{
  const q = document.getElementById('search').value.toLowerCase();
  let entries = Object.entries(CLUSTERS);

  if (_sortMode === 'cohesion') entries.sort((a,b) => b[1].cohesion - a[1].cohesion);
  else if (_sortMode === 'size') entries.sort((a,b) => b[1].size - a[1].size);
  else entries.sort((a,b) => a[0] - b[0]);

  const grid = document.getElementById('cluster-grid');
  grid.innerHTML = '';

  for (const [id, cl] of entries) {{
    const stocks = cl.stocks;
    const matched = q ? stocks.filter(s =>
      s.name.toLowerCase().includes(q) || s.code.includes(q)
    ) : stocks;
    if (q && matched.length === 0) continue;

    const color = clColorMap[id];
    const showStocks = q ? matched : stocks;
    const cohPct = Math.min(100, Math.max(0, cl.cohesion * 100));

    const card = document.createElement('div');
    card.className = 'cluster-card';
    card.innerHTML = `
      <div class="cluster-header">
        <div class="cluster-dot" style="background:${{color}}"></div>
        <div class="cluster-title">${{cl.label || `클러스터 ${{id}}`}}</div>
        <div class="cluster-badge">${{cl.size}}종목</div>
      </div>
      <div class="cohesion-bar-wrap">
        <span style="font-size:11px;color:var(--text3);width:48px;flex-shrink:0">결속력</span>
        <div class="cohesion-bar-bg">
          <div class="cohesion-bar-fill" style="width:${{cohPct}}%"></div>
        </div>
        <span class="cohesion-val">${{cl.cohesion.toFixed(3)}}</span>
      </div>
      <div class="stock-list">
        ${{showStocks.map(s => `
          <div class="stock-row">
            <span class="stock-code">${{s.code}}</span>
            <span class="stock-name">
              ${{s.code === cl.rep_code ? '<span class="rep-star">★</span>' : ''}}${{s.name}}
            </span>
            <span class="stock-market">${{s.market}}</span>
            <span class="stock-cap">${{s.market_cap ? s.market_cap.toLocaleString()+'억' : '-'}}</span>
          </div>
        `).join('')}}
      </div>
    `;
    grid.appendChild(card);
  }}
}}

// ── 실루엣 차트 (D3)
function drawSilhouette() {{
  const svg = d3.select('#sil-chart');
  svg.selectAll('*').remove();
  const W = svg.node().getBoundingClientRect().width || 800;
  const H = 300, margin = {{top:20, right:30, bottom:40, left:60}};
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;

  const g = svg.append('g').attr('transform', `translate(${{margin.left}},${{margin.top}})`);

  const x = d3.scaleLinear().domain([SIL_LABELS[0], SIL_LABELS[SIL_LABELS.length-1]]).range([0, w]);
  const y = d3.scaleLinear().domain([d3.min(SIL_DATA)*0.98, d3.max(SIL_DATA)*1.02]).range([h, 0]);

  // 그리드
  g.append('g').attr('class', 'grid')
    .call(d3.axisLeft(y).ticks(5).tickSize(-w).tickFormat(''))
    .selectAll('line').style('stroke', '#1e2d45').style('stroke-dasharray', '3,3');
  g.select('.grid .domain').remove();

  // 최적 K 강조
  g.append('line')
    .attr('x1', x(BEST_K)).attr('x2', x(BEST_K))
    .attr('y1', 0).attr('y2', h)
    .style('stroke', '#3b82f6').style('stroke-width', 1.5).style('stroke-dasharray', '4,3');
  g.append('text')
    .attr('x', x(BEST_K)+6).attr('y', 14)
    .style('fill', '#60a5fa').style('font-size', '11px')
    .text(`최적 k=${{BEST_K}}`);

  // 라인
  const line = d3.line().x((_,i) => x(SIL_LABELS[i])).y(d => y(d)).curve(d3.curveMonotoneX);
  g.append('path').datum(SIL_DATA)
    .attr('fill', 'none').attr('stroke', '#10b981').attr('stroke-width', 2)
    .attr('d', line);

  // 점
  g.selectAll('circle').data(SIL_DATA).enter().append('circle')
    .attr('cx', (_,i) => x(SIL_LABELS[i])).attr('cy', d => y(d)).attr('r', 4)
    .attr('fill', (_,i) => SIL_LABELS[i]===BEST_K ? '#3b82f6' : '#10b981')
    .attr('stroke', '#0a0e1a').attr('stroke-width', 1.5);

  // 축
  g.append('g').attr('transform', `translate(0,${{h}})`).call(d3.axisBottom(x).ticks(10).tickFormat(d => 'k='+d))
    .selectAll('text').style('fill', '#64748b').style('font-size', '11px');
  g.append('g').call(d3.axisLeft(y).ticks(5))
    .selectAll('text').style('fill', '#64748b').style('font-size', '11px');
  g.selectAll('.domain').style('stroke', '#1e2d45');
  g.selectAll('.tick line').style('stroke', '#1e2d45');

  g.append('text').attr('x', w/2).attr('y', h+36)
    .style('fill','#64748b').style('font-size','12px').style('text-anchor','middle')
    .text('클러스터 수 (k)');
  g.append('text').attr('transform','rotate(-90)').attr('x',-h/2).attr('y',-44)
    .style('fill','#64748b').style('font-size','12px').style('text-anchor','middle')
    .text('실루엣 스코어');
}}

// ── 히트맵 (D3)
function drawHeatmap() {{
  const wrap = document.querySelector('.heatmap-wrap');
  const svg  = d3.select('#heatmap');
  svg.selectAll('*').remove();

  const n    = S_CODES.length;
  const cell = Math.max(3, Math.min(8, Math.floor(700 / n)));
  const W    = cell * n, H = cell * n;
  svg.attr('width', W).attr('height', H);

  const color = d3.scaleSequential().domain([-1, 1])
    .interpolator(d3.interpolateRdBu).clamp(true);

  // 클러스터 순서로 정렬
  const clOrder = [];
  clIds.forEach(id => {{
    CLUSTERS[id].stocks.forEach(s => {{
      const idx = S_CODES.indexOf(s.code);
      if (idx >= 0) clOrder.push(idx);
    }});
  }});
  // 남은 것
  S_CODES.forEach((_,i) => {{ if (!clOrder.includes(i)) clOrder.push(i); }});

  const sortedCodes = clOrder.map(i => S_CODES[i]);
  const sortedCorr  = clOrder.map(i => clOrder.map(j => S_CORR[i][j]));

  sortedCodes.forEach((code, row) => {{
    sortedCorr[row].forEach((val, col) => {{
      svg.append('rect')
        .attr('x', col*cell).attr('y', row*cell)
        .attr('width', cell).attr('height', cell)
        .attr('fill', color(-val));  // RdBu: 음수=파랑(좋음)이므로 반전
    }});
  }});

  // 클러스터 경계선
  let boundary = 0;
  clIds.forEach(id => {{
    const cnt = CLUSTERS[id].stocks.filter(s => S_CODES.includes(s.code)).length;
    if (cnt === 0) return;
    boundary += cnt;
    const pos = boundary * cell;
    svg.append('line').attr('x1',0).attr('x2',W).attr('y1',pos).attr('y2',pos)
      .style('stroke','#0a0e1a').style('stroke-width',1.5);
    svg.append('line').attr('x1',pos).attr('x2',pos).attr('y1',0).attr('y2',H)
      .style('stroke','#0a0e1a').style('stroke-width',1.5);
  }});
}}

// ── 초기 렌더
renderStats();
renderClusters();
</script>
</body>
</html>"""

    html_path = out_dir / "sector_result.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"HTML 저장: {html_path}")
    return html_path


# ══════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="Sector Builder — 상관관계 기반 섹터 클러스터링")
    parser.add_argument("--clusters", type=int, default=None,
                        help="클러스터 수 지정 (없으면 자동 탐색)")
    parser.add_argument("--min-k",   type=int, default=20, help="자동 탐색 최소 k (기본 20)")
    parser.add_argument("--max-k",   type=int, default=50, help="자동 탐색 최대 k (기본 50)")
    parser.add_argument("--out",     type=str, default="./sector_output", help="결과 출력 디렉토리")
    parser.add_argument("--load-universe", type=str, default=None,
                        help="이미 수집한 universe JSON 재사용")
    args = parser.parse_args()

    out_dir = Path(args.out)
    started = datetime.now()

    # 1. 유니버스 수집
    if args.load_universe and Path(args.load_universe).exists():
        with open(args.load_universe, encoding="utf-8") as f:
            universe = json.load(f)
        # 제외 업종 필터 (재사용 시에도 적용)
        before = len(universe)
        universe = [s for s in universe if s.get("naver_group", "") not in EXCLUDE_GROUPS]
        logger.info(f"유니버스 재사용: {before}개 → 업종 제외 후 {len(universe)}개 (from {args.load_universe})")
    else:
        async with aiohttp.ClientSession() as session:
            universe = await collect_universe(session)
        # 유니버스 저장 (재사용 가능하게)
        out_dir.mkdir(parents=True, exist_ok=True)
        uni_path = out_dir / "universe.json"
        with open(uni_path, "w", encoding="utf-8") as f:
            json.dump(universe, f, ensure_ascii=False)
        logger.info(f"유니버스 저장: {uni_path}")

    # 2. OHLCV 수집
    ohlcv = await collect_ohlcv(universe)

    # OHLCV 성공 종목만 유니버스 필터
    ohlcv_codes = set(ohlcv.keys())
    universe_filtered = [s for s in universe if s["code"] in ohlcv_codes]
    logger.info(f"OHLCV 통과 유니버스: {len(universe_filtered)}개")

    # 3. 상관관계 계산
    corr, codes = calc_multiscale_corr(ohlcv)

    # 4. 클러스터링
    if args.clusters:
        n_clusters = args.clusters
        labels, Z, dist = apply_clusters(corr, codes, n_clusters)
        # 실루엣 스코어 (지정 k만)
        sil_score = silhouette_score(dist, labels, metric="precomputed")
        sil_scores = [sil_score]
        min_k_used = n_clusters
        best_k     = n_clusters
        logger.info(f"지정 클러스터 수: {n_clusters} (실루엣: {sil_score:.4f})")
    else:
        labels, best_k, sil_scores, Z, dist = find_optimal_clusters(
            corr, codes, args.min_k, args.max_k)
        n_clusters = best_k
        min_k_used = args.min_k

    # 5. 결과 정리
    cluster_result = build_cluster_result(labels, codes, universe_filtered, ohlcv, corr, n_clusters)

    meta = {
        "generated_at":  started.strftime("%Y-%m-%d %H:%M"),
        "universe_size": len(universe),
        "ohlcv_success": len(ohlcv),
        "n_clusters":    n_clusters,
        "silhouette":    round(float(max(sil_scores)), 4),
        "corr_windows":  CORR_WINDOWS,
        "corr_weights":  CORR_WEIGHTS,
        "filters": {
            "min_price":      MIN_PRICE,
            "min_market_cap": MIN_MARKET_CAP,
            "min_trade_amt":  MIN_TRADE_AMT,
        }
    }

    json_path, csv_path = save_results(cluster_result, out_dir, meta)

    # 6. HTML 시각화
    html_path = generate_html(
        cluster_result, corr, codes, sil_scores,
        min_k_used, best_k, out_dir, meta
    )

    elapsed = (datetime.now() - started).seconds
    logger.info(f"\n{'='*60}")
    logger.info(f"완료 ({elapsed}초)")
    logger.info(f"  유니버스:   {len(universe)}개")
    logger.info(f"  OHLCV 성공: {len(ohlcv)}개")
    logger.info(f"  클러스터:   {n_clusters}개")
    logger.info(f"  실루엣:     {max(sil_scores):.4f}")
    logger.info(f"  출력:")
    logger.info(f"    {json_path}")
    logger.info(f"    {csv_path}")
    logger.info(f"    {html_path}")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())