import requests
from datetime import date
from dateutil.relativedelta import relativedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://comp.wisereport.co.kr/wiseCalendar/EarningsReleaseMonthlyView.aspx",
}

BASE = "https://comp.wisereport.co.kr"
EXCEL_URL = BASE + "/wiseCalendar/EarningsReleaseCalendarExcel.aspx"

session = requests.Session()
session.get(BASE + "/wiseCalendar/EarningsReleaseMonthlyView.aspx", headers=HEADERS, timeout=10)

today = date.today()
months = [(today + relativedelta(months=i)).strftime("%Y%m") for i in range(3)]

for yyyymm in months:
    params = {"curyymm": yyyymm, "dd": "", "cn": "", "mode": "excel"}
    res = session.get(EXCEL_URL, params=params, headers=HEADERS, timeout=15)
    print(f"\n[{yyyymm}] status={res.status_code}, 길이={len(res.content)}")
    print(f"  Content-Type: {res.headers.get('Content-Type')}")
    sig = res.content[:8].hex()
    print(f"  시그니처: {sig}")
    ext = "xlsx" if sig.startswith("504b") else "xls" if sig.startswith("d0cf") else "html"
    print(f"  형식: {ext}")
    fname = f"wise_{yyyymm}.{ext}"
    with open(fname, "wb") as f:
        f.write(res.content)
    print(f"  → {fname} 저장")
    if ext == "html":
        print(f"  앞 200자: {res.content[:200].decode('utf-8', errors='replace')}")
