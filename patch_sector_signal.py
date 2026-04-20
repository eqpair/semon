with open('/home/eq/semon/sector_signal.py', encoding='utf-8') as f:
    txt = f.read()

changes = []

# 1. RRG_VERSION 업그레이드
old = 'RRG_VERSION = f"bloomberg_v2_s{MA_SHORT}_l{MA_LONG}_daily"'
new = 'RRG_VERSION = f"bloomberg_v3_s{MA_SHORT}_l{MA_LONG}_kospi"'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("RRG_VERSION 업그레이드")

# 2. kospi_store 추가
old = 'market_cap_store: dict[str, int]      = {}  # { code: 시총(억원) }'
new = 'market_cap_store: dict[str, int]      = {}  # { code: 시총(억원) }\nkospi_store:      list[float]          = []   # KOSPI 지수 일봉 closes'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("kospi_store 추가")

# 3. update_kospi 함수 추가
old = '# 수식 버전 — 파라미터나 수식이 바뀌면 이 값을 올려서'
new = 'def update_kospi(closes: list[float]) -> None:\n    """main.py에서 KOSPI 일봉 데이터를 주입할 때 호출"""\n    kospi_store.clear()\n    kospi_store.extend(closes)\n    logger.info(f"KOSPI 업데이트: {len(closes)}일치")\n\n\n# 수식 버전 — 파라미터나 수식이 바뀌면 이 값을 올려서'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("update_kospi 함수 추가")

# 4. calc_sector_rrg 벤치마크 교체
old = '    min_len  = min(len(v) for v in valid.values())\n    aligned  = {s: v[-min_len:] for s, v in valid.items()}\n    re_rebased = {s: _rebase(v) for s, v in aligned.items()}\n    market_bm = _make_benchmark(re_rebased)'
new = '    min_len  = min(len(v) for v in valid.values())\n    aligned  = {s: v[-min_len:] for s, v in valid.items()}\n    re_rebased = {s: _rebase(v) for s, v in aligned.items()}\n\n    # KOSPI 벤치마크 사용 (없으면 섹터 자체 평균으로 폴백)\n    if len(kospi_store) >= min_len:\n        kospi_aligned = kospi_store[-min_len:]\n        market_bm = _rebase(kospi_aligned)\n        logger.debug(f"섹터 RRG 벤치마크: KOSPI ({min_len}일)")\n    else:\n        market_bm = _make_benchmark(re_rebased)\n        logger.debug(f"섹터 RRG 벤치마크: 섹터 평균 (KOSPI 데이터 부족)")'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("calc_sector_rrg 벤치마크 KOSPI로 교체")

# 5. tail 소급 계산 KOSPI 벤치마크
old = '                hist_aligned   = {s: aligned[s][:-offset] for s in aligned}\n                hist_rebased   = {s: _rebase(hist_aligned[s]) for s in hist_aligned}\n                hist_market_bm = _make_benchmark(hist_rebased)\n                hist_ratio     = _calc_rs_ratio(hist_rebased[sector], hist_market_bm)'
new = '                hist_aligned   = {s: aligned[s][:-offset] for s in aligned}\n                hist_rebased   = {s: _rebase(hist_aligned[s]) for s in hist_aligned}\n                hist_len = len(list(hist_aligned.values())[0])\n                if len(kospi_store) >= hist_len + offset:\n                    hist_kospi = kospi_store[-(hist_len + offset):-offset]\n                    hist_market_bm = _rebase(hist_kospi)\n                else:\n                    hist_market_bm = _make_benchmark(hist_rebased)\n                hist_ratio     = _calc_rs_ratio(hist_rebased[sector], hist_market_bm)'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("tail 소급 계산 KOSPI 벤치마크 적용")

with open('/home/eq/semon/sector_signal.py', 'w', encoding='utf-8') as f:
    f.write(txt)

for c in changes:
    print(f"✅ {c}")
print(f"\n총 {len(changes)}/5개 패치 완료")