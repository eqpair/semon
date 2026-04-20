with open('/home/eq/semon/main.py', encoding='utf-8') as f:
    txt = f.read()

changes = []

# 1. import에 fetch_kospi_ohlcv 추가
old = 'from crawler import fetch_all_prices, fetch_all_ohlcv'
new = 'from crawler import fetch_all_prices, fetch_all_ohlcv, fetch_kospi_ohlcv'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("fetch_kospi_ohlcv import 추가")

# 2. import에 update_kospi 추가
old = 'from sector_signal import (update_prices, update_ohlcv, calc_all_signals,\n                           load_market_caps_into_store,\n                           save_rrg_history, load_rrg_history)'
new = 'from sector_signal import (update_prices, update_ohlcv, calc_all_signals,\n                           load_market_caps_into_store, update_kospi,\n                           save_rrg_history, load_rrg_history)'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("update_kospi import 추가")

# 3. _kospi_date 변수 추가 (기존 변수들 근처)
old = '_off_market_pushed: str | None = None'
new = '_off_market_pushed: str | None = None\n_kospi_date: str | None = None'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("_kospi_date 변수 추가")

# 4. 시총 갱신 이후 KOSPI fetch 추가
old = '            # 2. OHLCV 갱신 (1시간마다)'
new = '''            # 2. KOSPI 갱신 — 하루 1회
            if _kospi_date != today:
                logger.info("KOSPI 일봉 fetch 시작")
                kospi_closes = await fetch_kospi_ohlcv()
                if kospi_closes:
                    update_kospi(kospi_closes)
                    _kospi_date = today
                else:
                    logger.warning("KOSPI fetch 실패 — 섹터 평균으로 폴백")

            # 3. OHLCV 갱신 (1시간마다)'''
if old in txt:
    txt = txt.replace(old, new)
    changes.append("KOSPI fetch 루프 추가")

# 5. 기존 번호 밀린 것 수정 (3→4, 4→5 등)
txt = txt.replace(
    '            # 3. 현재가 fetch',
    '            # 4. 현재가 fetch'
)
txt = txt.replace(
    '            # 4. 신호 계산',
    '            # 5. 신호 계산'
)
txt = txt.replace(
    '            # 5. JSON 저장 + git push',
    '            # 6. JSON 저장 + git push'
)
txt = txt.replace(
    '            # 6. rrg_history 영속화',
    '            # 7. rrg_history 영속화'
)
txt = txt.replace(
    '            # 7. radar 감지 + 텔레그램 알림',
    '            # 8. radar 감지 + 텔레그램 알림'
)
changes.append("루프 번호 정리")

with open('/home/eq/semon/main.py', 'w', encoding='utf-8') as f:
    f.write(txt)

for c in changes:
    print(f"✅ {c}")
print(f"\n총 {len(changes)}/5개 패치 완료")