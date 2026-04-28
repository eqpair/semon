"""
WiseReport 어닝스캘린더 엑셀 다운로드 테스트
python3 test_wisereport3.py
"""

import requests
import json
from datetime import date
from dateutil.relativedelta import relativedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
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
    print(f"  Content-Disposition: {res.headers.get('Content-Disposition', '없음')}")

    # 파일 저장
    ext = "xls"
    ct = res.headers.get("Content-Type", "")
    if "xlsx" in ct or "openxml" in ct:
        ext = "xlsx"
    elif "html" in ct:
        ext = "html"

    fname = f"wise_{yyyymm}.{ext}"
    with open(fname, "wb") as f:
        f.write(res.content)
    print(f"  → {fname} 저장")

    # html로 내려왔으면 내용 일부 출력
    if ext == "html":
        text = res.content.decode("utf-8", errors="replace")
        print(f"  앞 300자: {text[:300]}")
    else:
        # 엑셀이면 앞 몇 바이트 hex 확인 (파일 시그니처)
        sig = res.content[:8].hex()
        print(f"  파일 시그니처(hex): {sig}")
        # xls: d0cf11e0, xlsx: 504b0304
        if sig.startswith("d0cf"):
            print("  → XLS (구형 Excel) 형식")
        elif sig.startswith("504b"):
            print("  → XLSX 형식")
        else:
            print("  → 알 수 없는 형식")

print("\n\n=== 다음 단계 ===")
print("xls/xlsx 파일이 생성됐으면:")
print("  pip install openpyxl xlrd")
print("  python3 -c \"import openpyxl; wb=openpyxl.load_workbook('wise_202604.xlsx'); ws=wb.active; [print([c.value for c in r]) for r in ws.iter_rows(max_row=5)]\"")