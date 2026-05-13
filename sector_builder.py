"""
sector_builder.py  v2
─────────────────────
기존 SECTORS 57개 섹터를 기준으로
→ 유니버스(시총 500억+)에서 미편입 종목을 찾아
→ 각 섹터와 상관계수를 계산해 후보 추천

흐름:
  1. 전종목 수집 (시총 500억+, 주가 1000원+, 거래대금 1억+, 우선주/펀드 제외)
  2. 기존 SECTORS에 없는 종목 → 후보풀
  3. 후보 × 섹터 상관계수 계산 (멀티스케일 20/60/120일)
  4. 섹터별 Top 후보 추천 HTML 출력

실행:
  python3 sector_builder.py                          # 전체 실행
  python3 sector_builder.py --load-universe u.json   # 유니버스 재사용
  python3 sector_builder.py --top 10                 # 섹터당 후보 수
"""

import asyncio, aiohttp, csv, json, argparse, warnings, logging, sys, re
from pathlib import Path
from datetime import datetime
import numpy as np
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── 필터 기준 ─────────────────────────────────────────────────
MIN_PRICE      = 1_000
MIN_MARKET_CAP = 500
MIN_TRADE_AMT  = 1
MIN_OHLCV_DAYS = 130

# ── 제외 업종 ─────────────────────────────────────────────────
EXCLUDE_GROUPS = {"창업투자", "부동산", "기타금융", "기타"}

# ── 우선주 필터 ───────────────────────────────────────────────
_PREF_RE = re.compile(r"우[ABCK]?$|우선주$")

def is_preferred(code: str, name: str) -> bool:
    if code.endswith("5") and len(code) == 6:
        return True
    return bool(_PREF_RE.search(name))

# ── 멀티스케일 가중치 ─────────────────────────────────────────
CORR_WINDOWS = [20, 60, 120]
CORR_WEIGHTS = [0.2, 0.3, 0.5]

# ── URL ───────────────────────────────────────────────────────
BASE        = "https://finance.naver.com"
GROUP_URL   = BASE + "/sise/sise_group.naver?type=upjong"
DETAIL_URL  = BASE + "/sise/sise_group_detail.naver?type=upjong&no={no}"
PRICE_URL   = "https://polling.finance.naver.com/api/realtime?query=SERVICE_ITEM:{code}"
SISE_URL    = "https://fchart.stock.naver.com/sise.nhn?symbol={code}&timeframe=day&count=500&requestType=0"
SUMMARY_URL = "https://api.finance.naver.com/service/itemSummary.nhn?itemcode={code}"
HEADERS     = {"User-Agent": "Mozilla/5.0"}
SUMMARY_HDR = {**HEADERS, "Referer": "https://finance.naver.com/"}


# ══════════════════════════════════════════════════════════════
# 1. 전종목 수집
# ══════════════════════════════════════════════════════════════

async def _fetch_groups(session):
    async with session.get(GROUP_URL, headers=HEADERS) as r:
        html = await r.text(encoding="euc-kr")
    soup = BeautifulSoup(html, "html.parser")
    return [(l["href"].split("no=")[-1], l.text.strip())
            for l in soup.find_all("a", href=lambda h: h and "upjong" in h and "no=" in h)
            if l.text.strip()]

async def _fetch_group_stocks(session, no, group_name):
    url = DETAIL_URL.format(no=no)
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=10)) as r:
            html = await r.text(encoding="euc-kr")
    except Exception:
        return []
    soup = BeautifulSoup(html, "html.parser")
    stocks = []
    for a in soup.find_all("a", href=lambda h: h and "item/main" in str(h)):
        href = a.get("href", "")
        code = href.split("code=")[-1].strip() if "code=" in href else ""
        name = a.text.strip()
        if code and len(code) == 6 and name:
            stocks.append({"code": code, "name": name, "naver_group": group_name})
    return stocks

async def _fetch_price(session, code):
    try:
        async with session.get(PRICE_URL.format(code=code),
                               timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200: return {}
            data  = await r.json(content_type=None)
            areas = data.get("result", {}).get("areas", [])
            if not areas or not areas[0].get("datas"): return {}
            d = areas[0]["datas"][0]
            return {
                "price":     float(d.get("nv", 0) or 0),
                "trade_amt": float(d.get("aa", 0) or 0) / 1e8,
                "market":    "KOSPI" if d.get("mt") == "1" else "KOSDAQ",
            }
    except Exception:
        return {}

async def _fetch_cap(session, code) -> int:
    try:
        async with session.get(SUMMARY_URL.format(code=code), headers=SUMMARY_HDR,
                               timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200: return 0
            data = await r.json(content_type=None)
            return int(data.get("marketSum") or 0) // 100
    except Exception:
        return 0

async def collect_universe(session) -> list[dict]:
    logger.info("업종 목록 수집 중...")
    groups = await _fetch_groups(session)
    logger.info(f"총 {len(groups)}개 업종")

    all_stocks: dict[str, dict] = {}
    for i, (no, name) in enumerate(groups):
        for s in await _fetch_group_stocks(session, no, name):
            if s["code"] not in all_stocks:
                all_stocks[s["code"]] = s
        print(f"\r  종목 수집: {i+1}/{len(groups)} ({len(all_stocks)}개)", end="", flush=True)
        await asyncio.sleep(0.2)
    print()
    logger.info(f"전체 종목: {len(all_stocks)}개")

    # 현재가·거래대금
    codes, results = list(all_stocks.keys()), []
    logger.info("현재가·거래대금 수집 중...")
    for i in range(0, len(codes), 50):
        batch = codes[i:i+50]
        infos = await asyncio.gather(*[_fetch_price(session, c) for c in batch])
        for code, info in zip(batch, infos):
            s = all_stocks[code].copy(); s.update(info); results.append(s)
        print(f"\r  {min(i+50,len(codes))}/{len(codes)}개", end="", flush=True)
        await asyncio.sleep(0.3)
    print()

    # 1차 필터 + 우선주/업종 제외
    pre = [s for s in results
           if s.get("price", 0) >= MIN_PRICE
           and s.get("trade_amt", 0) >= MIN_TRADE_AMT
           and s.get("naver_group", "") not in EXCLUDE_GROUPS
           and not is_preferred(s["code"], s["name"])]
    logger.info(f"1차 필터 후: {len(pre)}개")

    # 시총
    logger.info("시총 수집 중...")
    for i in range(0, len(pre), 20):
        batch = pre[i:i+20]
        caps  = await asyncio.gather(*[_fetch_cap(session, s["code"]) for s in batch])
        for s, cap in zip(batch, caps): s["market_cap"] = cap
        print(f"\r  {min(i+20,len(pre))}/{len(pre)}개", end="", flush=True)
        await asyncio.sleep(0.5)
    print()

    filtered = [s for s in pre if s.get("market_cap", 0) >= MIN_MARKET_CAP]
    logger.info(f"최종 유니버스: {len(filtered)}개")
    return filtered


# ══════════════════════════════════════════════════════════════
# 2. OHLCV 수집
# ══════════════════════════════════════════════════════════════

async def _fetch_ohlcv(session, code):
    try:
        async with session.get(SISE_URL.format(code=code),
                               timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200: return code, None
            text = await r.text(encoding="euc-kr")
        soup   = BeautifulSoup(text, "html.parser")
        closes = []
        for item in soup.find_all("item"):
            parts = item.get("data", "").split("|")
            if len(parts) >= 5:
                try: closes.append(float(parts[4]))
                except ValueError: pass
        return code, closes if len(closes) >= MIN_OHLCV_DAYS else None
    except Exception as e:
        logger.warning(f"OHLCV ({code}): {e}")
        return code, None

async def collect_ohlcv(codes: list[str]) -> dict[str, list[float]]:
    result = {}
    logger.info(f"OHLCV 수집 중 ({len(codes)}개)...")
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(codes), 10):
            batch = codes[i:i+10]
            for code, closes in await asyncio.gather(*[_fetch_ohlcv(session, c) for c in batch]):
                if closes: result[code] = closes
            print(f"\r  {min(i+10,len(codes))}/{len(codes)}개 (성공:{len(result)}개)", end="", flush=True)
            await asyncio.sleep(1.0)
    print()
    logger.info(f"OHLCV 성공: {len(result)}/{len(codes)}개")
    return result


# ══════════════════════════════════════════════════════════════
# 3. 멀티스케일 상관계수 계산
# ══════════════════════════════════════════════════════════════

def _rolling_returns(closes: list[float], window: int) -> np.ndarray | None:
    arr = np.array(closes)
    if len(arr) <= window + 10: return None
    ret = (arr[window:] - arr[:-window]) / arr[:-window]
    return ret if len(ret) >= 30 else None

def _corr_1d(a: np.ndarray, b: np.ndarray) -> float:
    n = min(len(a), len(b))
    a, b = a[-n:], b[-n:]
    if np.std(a) < 1e-9 or np.std(b) < 1e-9: return 0.0
    return float(np.corrcoef(a, b)[0, 1])

def calc_sector_benchmark(sector_codes: list[str],
                           ohlcv: dict[str, list[float]],
                           window: int) -> np.ndarray | None:
    """섹터 내 유효 종목 등가중 수익률 평균 → 섹터 벤치마크 시계열"""
    rets = [_rolling_returns(ohlcv[c], window)
            for c in sector_codes if c in ohlcv]
    rets = [r for r in rets if r is not None]
    if not rets: return None
    n = min(len(r) for r in rets)
    return np.mean([r[-n:] for r in rets], axis=0)

def calc_candidate_corr(candidate_code: str,
                        sector_benchmarks: dict[str, dict[int, np.ndarray]],
                        ohlcv: dict[str, list[float]]) -> dict[str, float]:
    """후보 종목 ↔ 각 섹터 멀티스케일 상관계수"""
    result = {}
    for sector, bm_map in sector_benchmarks.items():
        weighted, total_w = 0.0, 0.0
        for window, weight in zip(CORR_WINDOWS, CORR_WEIGHTS):
            bm = bm_map.get(window)
            if bm is None: continue
            cand_ret = _rolling_returns(ohlcv[candidate_code], window)
            if cand_ret is None: continue
            c = _corr_1d(cand_ret, bm)
            if not np.isnan(c):
                weighted += weight * c
                total_w  += weight
        result[sector] = round(weighted / total_w, 4) if total_w > 0 else 0.0
    return result


# ══════════════════════════════════════════════════════════════
# 4. 메인 분석
# ══════════════════════════════════════════════════════════════

def run_analysis(universe: list[dict],
                 ohlcv: dict[str, list[float]],
                 sectors: dict[str, list[tuple]],
                 top_n: int) -> dict:
    """
    반환:
      {
        "candidates": [ {code, name, market, market_cap, top_sectors: [{sector, corr}]} ],
        "by_sector":  { sector: [ {code, name, corr} ] }
      }
    """
    existing = {code for codes in sectors.values() for code, _ in codes}
    name_map = {s["code"]: s["name"] for s in universe}
    cap_map  = {s["code"]: s.get("market_cap", 0) for s in universe}
    mkt_map  = {s["code"]: s.get("market", "") for s in universe}
    ngrp_map = {s["code"]: s.get("naver_group", "") for s in universe}

    # 후보풀: 유니버스에 있지만 SECTORS에 없는 종목
    candidates = [s for s in universe
                  if s["code"] not in existing and s["code"] in ohlcv]
    logger.info(f"후보풀: {len(candidates)}개 (유니버스 {len(universe)}개 - 기존 {len(existing)}개)")

    # 섹터별 벤치마크 계산
    logger.info("섹터 벤치마크 계산 중...")
    sector_benchmarks: dict[str, dict[int, np.ndarray]] = {}
    for sector, codes in sectors.items():
        sector_codes = [c for c, _ in codes]
        sector_benchmarks[sector] = {}
        for window in CORR_WINDOWS:
            bm = calc_sector_benchmark(sector_codes, ohlcv, window)
            if bm is not None:
                sector_benchmarks[sector][window] = bm

    # 후보 × 섹터 상관계수
    logger.info(f"상관계수 계산 중 ({len(candidates)}개 후보 × {len(sectors)}개 섹터)...")
    result_candidates = []
    by_sector: dict[str, list] = {s: [] for s in sectors}

    for i, cand in enumerate(candidates):
        code = cand["code"]
        corr_map = calc_candidate_corr(code, sector_benchmarks, ohlcv)

        # Top3 섹터
        top = sorted(corr_map.items(), key=lambda x: -x[1])[:3]
        entry = {
            "code":        code,
            "name":        name_map.get(code, ""),
            "market":      mkt_map.get(code, ""),
            "market_cap":  cap_map.get(code, 0),
            "naver_group": ngrp_map.get(code, ""),
            "top_sectors": [{"sector": s, "corr": c} for s, c in top],
            "best_sector": top[0][0] if top else "",
            "best_corr":   top[0][1] if top else 0.0,
        }
        result_candidates.append(entry)

        # 섹터별 리스트에도 추가
        for sector, corr in corr_map.items():
            by_sector[sector].append({"code": code, "name": name_map.get(code,""),
                                      "market": mkt_map.get(code,""),
                                      "market_cap": cap_map.get(code,0),
                                      "naver_group": ngrp_map.get(code,""),
                                      "corr": corr})

        if (i+1) % 50 == 0:
            print(f"\r  {i+1}/{len(candidates)}개", end="", flush=True)
    print()

    # 섹터별 상관계수 내림차순 정렬 + top_n 제한
    for sector in by_sector:
        by_sector[sector].sort(key=lambda x: -x["corr"])
        by_sector[sector] = by_sector[sector][:top_n]

    # 전체 후보 best_corr 내림차순
    result_candidates.sort(key=lambda x: -x["best_corr"])

    return {"candidates": result_candidates, "by_sector": by_sector}


# ══════════════════════════════════════════════════════════════
# 5. 저장
# ══════════════════════════════════════════════════════════════

class _NpEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, np.integer): return int(o)
        if isinstance(o, np.floating): return float(o)
        return super().default(o)

def save_results(analysis: dict, out_dir: Path, meta: dict):
    out_dir.mkdir(parents=True, exist_ok=True)

    json_path = out_dir / "sector_candidates.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, **analysis}, f, ensure_ascii=False, indent=2, cls=_NpEncoder)
    logger.info(f"JSON: {json_path}")

    csv_path = out_dir / "sector_candidates.csv"
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["code","name","market","market_cap",
                                          "naver_group","best_sector","best_corr",
                                          "2nd_sector","2nd_corr","3rd_sector","3rd_corr"])
        w.writeheader()
        for c in analysis["candidates"]:
            tops = c["top_sectors"]
            w.writerow({
                "code": c["code"], "name": c["name"],
                "market": c["market"], "market_cap": c["market_cap"],
                "naver_group": c["naver_group"],
                "best_sector": tops[0]["sector"] if len(tops)>0 else "",
                "best_corr":   tops[0]["corr"]   if len(tops)>0 else "",
                "2nd_sector":  tops[1]["sector"] if len(tops)>1 else "",
                "2nd_corr":    tops[1]["corr"]   if len(tops)>1 else "",
                "3rd_sector":  tops[2]["sector"] if len(tops)>2 else "",
                "3rd_corr":    tops[2]["corr"]   if len(tops)>2 else "",
            })
    logger.info(f"CSV: {csv_path}")
    return json_path, csv_path


# ══════════════════════════════════════════════════════════════
# 6. HTML 시각화
# ══════════════════════════════════════════════════════════════

def generate_html(analysis: dict, sectors: dict, meta: dict, out_dir: Path) -> Path:
    candidates_js = json.dumps(analysis["candidates"], ensure_ascii=False, cls=_NpEncoder)
    by_sector_js  = json.dumps(analysis["by_sector"],  ensure_ascii=False, cls=_NpEncoder)
    sector_list   = json.dumps(list(sectors.keys()),   ensure_ascii=False)
    existing_js   = json.dumps(
        {s: [{"code":c,"name":n} for c,n in codes] for s,codes in sectors.items()},
        ensure_ascii=False
    )

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>Sector Builder v2 — 후보 종목 추천</title>
<style>
:root {{
  --bg:#0a0e1a; --bg2:#111827; --bg3:#1a2235; --border:#1e2d45;
  --accent:#3b82f6; --accent2:#60a5fa; --text:#e2e8f0;
  --text2:#94a3b8; --text3:#64748b; --good:#10b981; --warn:#f59e0b;
  --font-mono:'JetBrains Mono','Fira Code',monospace;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:'Pretendard','Noto Sans KR',sans-serif;font-size:14px;line-height:1.6}}
.header{{background:linear-gradient(135deg,#0f172a,#1e293b);border-bottom:1px solid var(--border);padding:20px 32px;display:flex;align-items:center;justify-content:space-between}}
.header-title{{font-size:20px;font-weight:700;letter-spacing:-.5px}}
.header-title span{{color:var(--accent2)}}
.header-meta{{color:var(--text2);font-size:12px;font-family:var(--font-mono);margin-top:4px}}
.tabs{{display:flex;gap:2px;padding:16px 32px 0;background:var(--bg2);border-bottom:1px solid var(--border)}}
.tab{{padding:10px 20px;cursor:pointer;color:var(--text2);font-size:13px;font-weight:500;border-bottom:2px solid transparent;transition:all .2s}}
.tab:hover{{color:var(--text)}}
.tab.active{{color:var(--accent2);border-bottom-color:var(--accent)}}
.panel{{display:none;padding:24px 32px}}
.panel.active{{display:block}}
.stats-row{{display:flex;gap:16px;margin-bottom:20px;flex-wrap:wrap}}
.stat-card{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:16px 20px;flex:1;min-width:130px}}
.stat-label{{font-size:11px;color:var(--text3);text-transform:uppercase;letter-spacing:1px;margin-bottom:6px}}
.stat-value{{font-size:26px;font-weight:700;font-family:var(--font-mono);color:var(--accent2)}}
.stat-sub{{font-size:11px;color:var(--text3);margin-top:2px}}
.toolbar{{display:flex;gap:10px;margin-bottom:16px;align-items:center;flex-wrap:wrap}}
.search-input{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:8px 14px;color:var(--text);font-size:13px;width:220px;outline:none}}
.search-input:focus{{border-color:var(--accent)}}
select{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:8px 12px;color:var(--text);font-size:13px;outline:none;cursor:pointer}}
.btn{{background:var(--bg2);border:1px solid var(--border);border-radius:8px;padding:8px 14px;color:var(--text2);font-size:12px;cursor:pointer;transition:all .15s}}
.btn:hover,.btn.active{{background:var(--accent);color:#fff;border-color:var(--accent)}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:var(--bg3);padding:10px 12px;text-align:left;color:var(--text2);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--border);position:sticky;top:0}}
td{{padding:9px 12px;border-bottom:1px solid var(--border);vertical-align:middle}}
tr:hover td{{background:var(--bg3)}}
.tbl-wrap{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:auto;max-height:70vh}}
.corr-bar-wrap{{display:flex;align-items:center;gap:8px}}
.corr-bar-bg{{width:80px;height:6px;background:var(--bg3);border-radius:3px;overflow:hidden;flex-shrink:0}}
.corr-bar-fill{{height:100%;border-radius:3px}}
.code-badge{{font-family:var(--font-mono);font-size:11px;background:var(--bg3);padding:2px 7px;border-radius:4px;color:var(--text2)}}
.market-badge{{font-size:10px;padding:2px 6px;border-radius:3px;background:var(--bg3);color:var(--text3)}}
.sector-tag{{font-size:11px;padding:2px 8px;border-radius:20px;background:#1e3a5f;color:var(--accent2);margin-right:4px;display:inline-block;margin-bottom:2px}}
.top1-badge{{background:#1e3a1e;color:var(--good)}}
/* 섹터뷰 */
.sector-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px}}
.sector-card{{background:var(--bg2);border:1px solid var(--border);border-radius:10px;overflow:hidden}}
.sector-card-header{{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}}
.sector-card-title{{font-weight:600;font-size:13px}}
.sector-card-body{{padding:10px 16px;max-height:280px;overflow-y:auto}}
.cand-row{{display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid var(--border);font-size:12px}}
.cand-row:last-child{{border-bottom:none}}
.cand-name{{flex:1;color:var(--text)}}
.cand-corr{{font-family:var(--font-mono);font-size:11px;min-width:44px;text-align:right}}
::-webkit-scrollbar{{width:5px;height:5px}}
::-webkit-scrollbar-track{{background:var(--bg)}}
::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}
</style>
</head>
<body>
<div class="header">
  <div>
    <div class="header-title">Sector <span>Builder</span> <span style="font-size:13px;color:var(--text3);font-weight:400">v2 — 후보 종목 추천</span></div>
    <div class="header-meta">
      생성: {meta.get('generated_at','')} &nbsp;|&nbsp;
      유니버스: {meta.get('universe_size',0)}개 &nbsp;|&nbsp;
      기존 섹터: {meta.get('n_sectors',0)}개 / {meta.get('n_existing',0)}종목 &nbsp;|&nbsp;
      후보: {meta.get('n_candidates',0)}개
    </div>
  </div>
</div>
<div class="tabs">
  <div class="tab active" onclick="showTab('candidates',this)">전체 후보</div>
  <div class="tab" onclick="showTab('by-sector',this)">섹터별 추천</div>
  <div class="tab" onclick="showTab('existing',this)">기존 종목 확인</div>
</div>

<!-- 패널1: 전체 후보 -->
<div id="tab-candidates" class="panel active">
  <div class="stats-row" id="stats-row"></div>
  <div class="toolbar">
    <input class="search-input" id="cand-search" placeholder="종목명·코드·업종 검색..." oninput="renderCandidates()">
    <select id="cand-sector-filter" onchange="renderCandidates()">
      <option value="">모든 섹터</option>
    </select>
    <select id="cand-market-filter" onchange="renderCandidates()">
      <option value="">KOSPI+KOSDAQ</option>
      <option value="KOSPI">KOSPI</option>
      <option value="KOSDAQ">KOSDAQ</option>
    </select>
    <button class="btn active" id="sort-corr" onclick="setSortC('corr')">상관계수순</button>
    <button class="btn" id="sort-cap" onclick="setSortC('cap')">시총순</button>
  </div>
  <div class="tbl-wrap">
    <table>
      <thead><tr>
        <th>코드</th><th>종목명</th><th>시장</th><th>시총(억)</th>
        <th>네이버 업종</th><th>Best 섹터</th><th>상관계수</th><th>2nd 섹터</th><th>3rd 섹터</th>
      </tr></thead>
      <tbody id="cand-tbody"></tbody>
    </table>
  </div>
</div>

<!-- 패널2: 섹터별 추천 -->
<div id="tab-by-sector" class="panel">
  <div class="toolbar">
    <input class="search-input" id="sector-search" placeholder="섹터명 검색..." oninput="renderBySector()">
  </div>
  <div class="sector-grid" id="sector-grid"></div>
</div>

<!-- 패널3: 기존 종목 확인 -->
<div id="tab-existing" class="panel">
  <div class="toolbar">
    <input class="search-input" id="exist-search" placeholder="종목명·코드 검색..." oninput="renderExisting()">
    <select id="exist-sector-filter" onchange="renderExisting()">
      <option value="">모든 섹터</option>
    </select>
  </div>
  <div class="tbl-wrap">
    <table>
      <thead><tr><th>섹터</th><th>코드</th><th>종목명</th></tr></thead>
      <tbody id="exist-tbody"></tbody>
    </table>
  </div>
</div>

<script>
const CANDIDATES = {candidates_js};
const BY_SECTOR  = {by_sector_js};
const SECTORS    = {sector_list};
const EXISTING   = {existing_js};

// ── 탭
function showTab(name, el) {{
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  el.classList.add('active');
}}

// ── 색상
function corrColor(v) {{
  if (v >= 0.6) return '#10b981';
  if (v >= 0.4) return '#3b82f6';
  if (v >= 0.2) return '#f59e0b';
  return '#ef4444';
}}
function corrBar(v) {{
  const pct = Math.max(0,Math.min(100,(v+1)/2*100));
  return `<div class="corr-bar-wrap">
    <div class="corr-bar-bg"><div class="corr-bar-fill" style="width:${{pct}}%;background:${{corrColor(v)}}"></div></div>
    <span style="font-family:var(--font-mono);font-size:12px;color:${{corrColor(v)}}">${{v.toFixed(3)}}</span>
  </div>`;
}}

// ── 섹터 필터 옵션 채우기
function initFilters() {{
  const cs = document.getElementById('cand-sector-filter');
  const es = document.getElementById('exist-sector-filter');
  SECTORS.forEach(s => {{
    cs.innerHTML += `<option value="${{s}}">${{s}}</option>`;
    es.innerHTML += `<option value="${{s}}">${{s}}</option>`;
  }});
}}

// ── 통계 카드
function renderStats() {{
  const total = CANDIDATES.length;
  const highCorr = CANDIDATES.filter(c=>c.best_corr>=0.5).length;
  const caps = CANDIDATES.map(c=>c.market_cap).filter(Boolean);
  const avgCap = caps.length ? Math.round(caps.reduce((a,b)=>a+b,0)/caps.length) : 0;
  document.getElementById('stats-row').innerHTML = `
    <div class="stat-card"><div class="stat-label">전체 후보</div><div class="stat-value">${{total}}</div><div class="stat-sub">미편입 종목</div></div>
    <div class="stat-card"><div class="stat-label">고상관 (≥0.5)</div><div class="stat-value">${{highCorr}}</div><div class="stat-sub">편입 우선 검토</div></div>
    <div class="stat-card"><div class="stat-label">평균 시총</div><div class="stat-value">${{avgCap.toLocaleString()}}</div><div class="stat-sub">억원</div></div>
    <div class="stat-card"><div class="stat-label">기존 섹터</div><div class="stat-value">${{SECTORS.length}}</div><div class="stat-sub">섹터 수</div></div>
  `;
}}

// ── 전체 후보 테이블
let _sortC = 'corr';
function setSortC(m) {{
  _sortC = m;
  document.getElementById('sort-corr').classList.toggle('active', m==='corr');
  document.getElementById('sort-cap').classList.toggle('active', m==='cap');
  renderCandidates();
}}

function renderCandidates() {{
  const q  = document.getElementById('cand-search').value.toLowerCase();
  const sf = document.getElementById('cand-sector-filter').value;
  const mf = document.getElementById('cand-market-filter').value;

  let data = [...CANDIDATES];
  if (q)  data = data.filter(c => c.name.toLowerCase().includes(q) || c.code.includes(q) || c.naver_group.toLowerCase().includes(q));
  if (sf) data = data.filter(c => c.best_sector === sf);
  if (mf) data = data.filter(c => c.market === mf);
  if (_sortC==='cap') data.sort((a,b)=>b.market_cap-a.market_cap);
  else data.sort((a,b)=>b.best_corr-a.best_corr);

  const tbody = document.getElementById('cand-tbody');
  tbody.innerHTML = data.map(c => {{
    const tops = c.top_sectors || [];
    const t1 = tops[0]||{{}}, t2 = tops[1]||{{}}, t3 = tops[2]||{{}};
    return `<tr>
      <td><span class="code-badge">${{c.code}}</span></td>
      <td><b>${{c.name}}</b></td>
      <td><span class="market-badge">${{c.market}}</span></td>
      <td style="font-family:var(--font-mono)">${{(c.market_cap||0).toLocaleString()}}</td>
      <td style="color:var(--text2)">${{c.naver_group||''}}</td>
      <td><span class="sector-tag top1-badge">${{t1.sector||''}}</span></td>
      <td>${{corrBar(t1.corr||0)}}</td>
      <td><span class="sector-tag">${{t2.sector||''}}</span><span style="font-size:11px;color:var(--text3)">${{(t2.corr||0).toFixed(3)}}</span></td>
      <td><span class="sector-tag">${{t3.sector||''}}</span><span style="font-size:11px;color:var(--text3)">${{(t3.corr||0).toFixed(3)}}</span></td>
    </tr>`;
  }}).join('');
}}

// ── 섹터별 추천 카드
function renderBySector() {{
  const q = document.getElementById('sector-search').value.toLowerCase();
  const grid = document.getElementById('sector-grid');
  grid.innerHTML = '';
  SECTORS.filter(s => s.toLowerCase().includes(q)).forEach(sector => {{
    const cands = BY_SECTOR[sector] || [];
    if (!cands.length) return;
    const card = document.createElement('div');
    card.className = 'sector-card';
    card.innerHTML = `
      <div class="sector-card-header">
        <span class="sector-card-title">${{sector}}</span>
        <span style="font-size:11px;color:var(--text3)">${{cands.length}}개 후보</span>
      </div>
      <div class="sector-card-body">
        ${{cands.map(c=>`
          <div class="cand-row">
            <span class="code-badge">${{c.code}}</span>
            <span class="cand-name">${{c.name}}</span>
            <span class="market-badge">${{c.market}}</span>
            <span class="cand-corr" style="color:${{corrColor(c.corr)}}">${{c.corr.toFixed(3)}}</span>
          </div>
        `).join('')}}
      </div>
    `;
    grid.appendChild(card);
  }});
}}

// ── 기존 종목 확인
function renderExisting() {{
  const q  = document.getElementById('exist-search').value.toLowerCase();
  const sf = document.getElementById('exist-sector-filter').value;
  const tbody = document.getElementById('exist-tbody');
  const rows = [];
  SECTORS.filter(s=>!sf||s===sf).forEach(sector=>{{
    (EXISTING[sector]||[]).filter(s=>
      !q || s.name.toLowerCase().includes(q) || s.code.includes(q)
    ).forEach(s=>{{
      rows.push(`<tr><td><span class="sector-tag">${{sector}}</span></td><td><span class="code-badge">${{s.code}}</span></td><td>${{s.name}}</td></tr>`);
    }});
  }});
  tbody.innerHTML = rows.join('');
}}

// ── 초기화
initFilters();
renderStats();
renderCandidates();
renderBySector();
renderExisting();
</script>
</body>
</html>"""

    html_path = out_dir / "sector_candidates.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"HTML: {html_path}")
    return html_path


# ══════════════════════════════════════════════════════════════
# 메인
# ══════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-universe", type=str, default=None)
    parser.add_argument("--top",  type=int, default=15, help="섹터당 후보 수 (기본 15)")
    parser.add_argument("--out",  type=str, default="./sector_output")
    args = parser.parse_args()

    out_dir = Path(args.out)
    started = datetime.now()

    # config에서 SECTORS 로드
    import sys; sys.path.insert(0, str(Path(__file__).parent))
    try:
        from config import SECTORS
        logger.info(f"SECTORS 로드: {len(SECTORS)}개 섹터, {sum(len(v) for v in SECTORS.values())}개 종목")
    except ImportError:
        logger.error("config.py를 찾을 수 없습니다. semon 디렉토리에서 실행하세요.")
        return

    # 1. 유니버스
    if args.load_universe and Path(args.load_universe).exists():
        with open(args.load_universe, encoding="utf-8") as f:
            universe = json.load(f)
        before = len(universe)
        universe = [s for s in universe
                    if s.get("naver_group","") not in EXCLUDE_GROUPS
                    and not is_preferred(s["code"], s["name"])]
        logger.info(f"유니버스 재사용: {before}개 → 필터 후 {len(universe)}개")
    else:
        async with aiohttp.ClientSession() as session:
            universe = await collect_universe(session)
        out_dir.mkdir(parents=True, exist_ok=True)
        uni_path = out_dir / "universe.json"
        with open(uni_path, "w", encoding="utf-8") as f:
            json.dump(universe, f, ensure_ascii=False)
        logger.info(f"유니버스 저장: {uni_path}")

    # 2. OHLCV
    all_codes = list({s["code"] for s in universe} |
                     {c for codes in SECTORS.values() for c,_ in codes})
    ohlcv = await collect_ohlcv(all_codes)

    # 3. 분석
    analysis = run_analysis(universe, ohlcv, SECTORS, args.top)

    # 4. 저장
    meta = {
        "generated_at": started.strftime("%Y-%m-%d %H:%M"),
        "universe_size": len(universe),
        "n_sectors":    len(SECTORS),
        "n_existing":   sum(len(v) for v in SECTORS.values()),
        "n_candidates": len(analysis["candidates"]),
        "top_n":        args.top,
    }
    save_results(analysis, out_dir, meta)
    generate_html(analysis, SECTORS, meta, out_dir)

    elapsed = (datetime.now() - started).seconds
    logger.info(f"\n{'='*60}")
    logger.info(f"완료 ({elapsed}초)")
    logger.info(f"  유니버스: {len(universe)}개 / 후보: {len(analysis['candidates'])}개")
    logger.info(f"  출력: {out_dir}/sector_candidates.html")
    logger.info(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main())