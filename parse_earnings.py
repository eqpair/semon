import re, json, requests, logging, sys
from datetime import date
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup

sys.path.insert(0, '/home/eq/semon')
from config import SECTORS

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger()

# config.py에서 종목명 → 코드 매핑
CODE_MAP = {}
for sector, stocks in SECTORS.items():
    for code, name in stocks:
        CODE_MAP[name] = code

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://comp.wisereport.co.kr/wiseCalendar/EarningsReleaseMonthlyView.aspx",
}
BASE = "https://comp.wisereport.co.kr"
EXCEL_URL = BASE + "/wiseCalendar/EarningsReleaseCalendarExcel.aspx"

def fetch_and_parse(session, yyyymm):
    res = session.get(EXCEL_URL, params={"curyymm": yyyymm, "dd": "", "cn": "", "mode": "excel"}, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(res.content, "html.parser", from_encoding="utf-8")
    table = soup.find("table")
    if not table:
        log.info(f"  [{yyyymm}] 테이블 없음")
        return []
    year, month = int(yyyymm[:4]), int(yyyymm[4:])
    results = []
    for cell in table.find_all(["td", "th"]):
        text = cell.get_text(strip=True)
        m = re.match(r'^(\d{2})(.+)$', text, re.DOTALL)
        if not m:
            continue
        day = int(m.group(1))
        if not (1 <= day <= 31):
            continue
        for name, typ in re.findall(r'(.+?)\s*\(([^)]+)\)', m.group(2)):
            name = name.strip()
            if not name:
                continue
            try:
                dt = date(year, month, day).isoformat()
            except ValueError:
                continue
            entry = {"date": dt, "name": name, "type": typ.strip()}
            if name in CODE_MAP:
                entry["code"] = CODE_MAP[name]
            results.append(entry)
    return results

session = requests.Session()
session.get(BASE + "/wiseCalendar/EarningsReleaseMonthlyView.aspx", headers=HEADERS, timeout=10)

today = date.today()
months = [(today + relativedelta(months=i)).strftime("%Y%m") for i in range(3)]

all_data = []
for yyyymm in months:
    rows = fetch_and_parse(session, yyyymm)
    log.info(f"  [{yyyymm}] {len(rows)}개")
    all_data.extend(rows)

all_data.sort(key=lambda x: x["date"])

with open("docs/data/earnings.json", "w", encoding="utf-8") as f:
    json.dump({"updated": today.isoformat(), "count": len(all_data), "earnings": all_data}, f, ensure_ascii=False, indent=2)

matched = sum(1 for e in all_data if "code" in e)
log.info(f"  총 {len(all_data)}개 저장 / 코드 매칭 {matched}개")
