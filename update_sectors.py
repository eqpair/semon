"""
update_sectors.py  v2
─────────────────────
섹터 ↔ 네이버 업종 매핑 기반으로
상관계수 + 업종 두 가지 조건을 동시에 만족하는 후보만 추가

조건:
  ① best_corr >= threshold (기본 0.7)
  ② 네이버 업종이 해당 섹터의 허용 업종 목록에 포함

실행:
  python3 update_sectors.py              # dry-run
  python3 update_sectors.py --apply      # 실제 적용
  python3 update_sectors.py --threshold 0.8 --apply
"""

import json, argparse, shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ── 섹터 ↔ 네이버 업종 매핑 ──────────────────────────────────
SECTOR_NAVER_MAP = {
    "반도체_대형":      ["반도체와반도체장비"],
    "반도체_중소형":    ["반도체와반도체장비"],
    "반도체_장비":      ["반도체와반도체장비"],
    "반도체_소재부품":  ["반도체와반도체장비", "전자장비와기기", "화학"],
    "2차전지_셀":       ["전기장비", "화학", "전기제품"],
    "2차전지_소재부품": ["화학", "전기장비", "전자장비와기기", "비철금속"],
    "전력기기":         ["전기장비", "전기제품", "전기유틸리티"],
    "신재생에너지":     ["전기장비", "전기유틸리티", "에너지장비및서비스"],
    "원전":             ["전기장비", "기계", "에너지장비및서비스"],
    "디스플레이":       ["디스플레이장비및부품", "디스플레이패널", "전자장비와기기"],
    "AI_인프라":        ["소프트웨어", "양방향미디어와서비스"],
    "조선":             ["조선"],
    "방산":             ["우주항공과국방", "기계"],
    "로봇":             ["기계", "전자장비와기기", "소프트웨어"],
    "우주·위성":        ["우주항공과국방", "통신장비", "전자장비와기기"],
    "자동차_완성차":    ["자동차"],
    "자동차_부품":      ["자동차부품"],
    "타이어":           ["자동차부품"],
    "IT부품":           ["전자장비와기기", "전자제품", "전기제품", "디스플레이장비및부품"],
    "빅플랫폼":         ["양방향미디어와서비스", "IT서비스", "소프트웨어"],
    "B2B_IT서비스":     ["IT서비스", "소프트웨어"],
    "바이오_대형CDMO":  ["생물공학", "제약"],
    "CMO_CDMO":         ["생물공학", "제약"],
    "바이오_기술이전":  ["생물공학"],
    "바이오_임상단계":  ["생물공학"],
    "대형제약":         ["제약"],
    "중소제약":         ["제약", "생물공학"],
    "의료기기":         ["건강관리장비와용품", "생명과학도구및서비스"],
    "미용·에스테틱":    ["건강관리장비와용품", "제약", "생물공학"],
    "은행":             ["은행"],
    "증권":             ["증권"],
    "보험":             ["생명보험", "손해보험"],
    "핀테크·결제":      ["카드", "IT서비스", "소프트웨어"],
    "철강":             ["철강"],
    "비철금속":         ["비철금속"],
    "정유":             ["석유와가스"],
    "석유화학":         ["화학"],
    "에너지유틸리티":   ["가스유틸리티", "전기유틸리티", "복합유틸리티"],
    "건설":             ["건설"],
    "시멘트":           ["건축자재", "건축제품"],
    "건자재":           ["건축자재", "건축제품", "가정용기기와용품"],
    "해운":             ["해운사"],
    "항공":             ["항공사"],
    "물류·육운":        ["항공화물운송과물류", "도로와철도운송"],
    "통신":             ["무선통신서비스", "다각화된통신서비스"],
    "엔터·음악":        ["방송과엔터테인먼트"],
    "게임":             ["게임엔터테인먼트"],
    "미디어·콘텐츠":    ["방송과엔터테인먼트", "광고"],
    "화장품_브랜드":    ["화장품"],
    "화장품_ODM":       ["화장품"],
    "K푸드_수출":       ["식품", "음료"],
    "중국내수식품":     ["식품", "음료"],
    "내수식품":         ["식품", "음료", "식품과기본식료품소매"],
    "유통":             ["백화점과일반상점", "전문소매", "식품과기본식료품소매",
                         "인터넷과카탈로그소매"],
    "패션·의류":        ["섬유,의류,신발,호화품"],
    "레저·여행":        ["호텔,레스토랑,레저", "레저용장비와제품"],
    "생활용품":         ["가정용품", "가정용기기와용품", "담배"],
}


def load_candidates(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)["candidates"]

def load_sectors(config_path):
    import sys
    sys.path.insert(0, str(Path(config_path).parent))
    if "config" in sys.modules:
        del sys.modules["config"]
    from config import SECTORS
    return SECTORS

def build_additions(candidates, sectors, threshold):
    existing = {code for codes in sectors.values() for code, _ in codes}
    additions = defaultdict(list)
    stats = {"corr": 0, "naver": 0, "no_map": 0, "exist": 0}

    for c in candidates:
        code, corr = c["code"], c["best_corr"]
        sector, naver = c["best_sector"], c.get("naver_group", "")

        if code in existing:       stats["exist"]  += 1; continue
        if corr < threshold:       stats["corr"]   += 1; continue
        # 우선주/전환주 제외: 6자리 숫자가 아닌 코드, 끝자리 5, 종목명 패턴
        import re as _re
        _pref = _re.compile(r"우[ABCK]?$|우선주$|2우|전환")
        if not code.isdigit() or len(code) != 6:   stats["exist"]  += 1; continue
        if code.endswith("5"):                      stats["exist"]  += 1; continue
        if _pref.search(c.get("name", "")):         stats["exist"]  += 1; continue
        if sector not in SECTOR_NAVER_MAP:
                                   stats["no_map"] += 1; continue
        if naver not in SECTOR_NAVER_MAP[sector]:
                                   stats["naver"]  += 1; continue
        # 반도체_대형: 시총 5000억 이상만
        if sector == "반도체_대형" and c.get("market_cap", 0) < 5000:
                                   stats["naver"]  += 1; continue

        additions[sector].append({"code": code, "name": c["name"],
                                   "corr": corr, "naver": naver})
        existing.add(code)

    for s in additions:
        additions[s].sort(key=lambda x: -x["corr"])

    print(f"\n[필터 통계]")
    print(f"  상관계수 미달:      {stats['corr']}개")
    print(f"  네이버 업종 불일치: {stats['naver']}개")
    print(f"  매핑 없는 섹터:     {stats['no_map']}개")
    print(f"  기존 편입:          {stats['exist']}개")
    return dict(additions)

def print_summary(additions, sectors, threshold):
    total    = sum(len(v) for v in additions.values())
    existing = sum(len(v) for v in sectors.values())
    print(f"\n{'='*60}")
    print(f"threshold={threshold}  추가 대상: {total}개")
    print(f"기존: {existing}개 → 추가 후: {existing+total}개")
    print(f"{'='*60}")
    for sector, stocks in sorted(additions.items()):
        print(f"\n[{sector}] +{len(stocks)}개")
        for s in stocks:
            print(f"  {s['code']}  {s['name']:<20s}  {s['corr']:.3f}  ({s['naver']})")

def apply_to_config(additions, config_path, backup=True):
    path = Path(config_path)
    if backup:
        bak = path.with_suffix(f".py.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        shutil.copy2(path, bak)
        print(f"\n백업: {bak}")

    src = path.read_text(encoding="utf-8")
    for sector, stocks in additions.items():
        marker = f'"{sector}": ['
        idx = src.find(marker)
        if idx == -1:
            print(f"  [WARN] 섹터 없음: {sector}"); continue

        start, depth, pos = idx + len(marker), 1, idx + len(marker)
        while pos < len(src) and depth > 0:
            if src[pos] == '[':   depth += 1
            elif src[pos] == ']': depth -= 1
            pos += 1
        close = pos - 1

        lines = [f'        ("{s["code"]}", "{s["name"]}"),  '
                 f'# corr={s["corr"]:.3f} [{s["naver"]}] auto-added'
                 for s in stocks]
        src = src[:close] + "\n" + "\n".join(lines) + "\n    " + src[close:]

    path.write_text(src, encoding="utf-8")
    print(f"\nconfig.py 업데이트 완료")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--candidates", default="sector_output/sector_candidates.json")
    parser.add_argument("--config",     default="config.py")
    parser.add_argument("--threshold",  type=float, default=0.7)
    parser.add_argument("--apply",      action="store_true")
    parser.add_argument("--no-backup",  action="store_true")
    args = parser.parse_args()

    if not Path(args.candidates).exists():
        print(f"ERROR: {args.candidates} 없음"); return
    if not Path(args.config).exists():
        print(f"ERROR: {args.config} 없음"); return

    candidates = load_candidates(args.candidates)
    sectors    = load_sectors(args.config)
    print(f"후보: {len(candidates)}개 / 섹터: {len(sectors)}개 / threshold: {args.threshold}")

    additions = build_additions(candidates, sectors, args.threshold)
    print_summary(additions, sectors, args.threshold)

    if not args.apply:
        print(f"\n[DRY-RUN] 적용하려면: python3 update_sectors.py --threshold {args.threshold} --apply")
        return

    apply_to_config(additions, args.config, backup=not args.no_backup)

    s2 = load_sectors(args.config)
    print(f"검증: {len(s2)}개 섹터, {sum(len(v) for v in s2.values())}개 종목")

if __name__ == "__main__":
    main()