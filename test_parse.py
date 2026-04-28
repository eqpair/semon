from bs4 import BeautifulSoup

for yyyymm in ["202604", "202605", "202606"]:
    print(f"\n{'='*50}")
    print(f"[{yyyymm}]")
    with open(f"wise_{yyyymm}.html", encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")

    tables = soup.find_all("table")
    print(f"테이블 수: {len(tables)}")

    for i, t in enumerate(tables[:3]):
        rows = t.find_all("tr")
        print(f"\n[Table {i}] {len(rows)}행")
        for r in rows[:5]:
            cols = [c.get_text(strip=True) for c in r.find_all(["td","th"])]
            if any(cols):
                print(f"  {cols}")
