import re, sys, time, urllib.request
sys.path.insert(0, "/home/ubuntu/semon")
import config

UA = {"User-Agent": "Mozilla/5.0"}

def official_name(code: str) -> str | None:
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=10) as r:
        body = r.read()
        ctype = r.headers.get("Content-Type", "")
    # 인코딩 자동 판별: 헤더 → meta charset → 순차 시도
    enc = None
    m = re.search(r"charset=([\w-]+)", ctype, re.I)
    if m:
        enc = m.group(1)
    if not enc:
        m = re.search(rb'charset=["\']?([\w-]+)', body[:2000], re.I)
        enc = m.group(1).decode() if m else None
    for e in ([enc] if enc else []) + ["utf-8", "euc-kr", "cp949"]:
        try:
            html = body.decode(e)
            t = re.search(r"<title>([^:<]+)", html)
            if t:
                name = t.group(1).strip()
                # 한글/영문/숫자로만 구성된 정상 이름인지 검증 (깨진 디코딩 배제)
                if name and not re.search(r"[\ufffd\x00-\x08]", name):
                    return name
        except (UnicodeDecodeError, LookupError):
            continue
    return None

def load_universe() -> dict:
    uni = {}
    for sector, members in config.SECTORS.items():
        if isinstance(members, dict):
            uni.update({c: str(n) for c, n in members.items()})
        else:
            for m in members:
                if isinstance(m, (list, tuple)) and m:
                    uni[m[0]] = str(m[1]) if len(m) > 1 else str(m[0])
    return uni

def main():
    uni = load_universe()
    print(f"config 종목 {len(uni)}개 대조 시작 (약 3분)...")
    mismatch, fail = [], []
    for i, (code, cfg_name) in enumerate(uni.items(), 1):
        try:
            name = official_name(code)
            if not name:
                fail.append(code)
            elif name != cfg_name:
                mismatch.append((code, cfg_name, name))
        except Exception:
            fail.append(code)
        time.sleep(0.25)
        if i % 100 == 0:
            print(f"  ...{i}/{len(uni)} (불일치 {len(mismatch)})")

    print(f"\n=== 불일치 {len(mismatch)}건 (조회실패 {len(fail)}건) ===")
    for c, old, new in mismatch:
        print(f"  {c}: config='{old}'  ->  '{new}'")
    if fail:
        print("조회실패:", ",".join(fail[:20]))

if __name__ == "__main__":
    main()
