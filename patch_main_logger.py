with open('/home/eq/semon/main.py', encoding='utf-8') as f:
    txt = f.read()

changes = []

# 1. import 추가
old = 'from radar import run_radar'
new = 'from radar import run_radar\nfrom signal_logger import log_signals, update_tracking'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("signal_logger import 추가")

# 2. radar 다음에 logger 호출 추가
old = '            # 8. radar 감지 + 텔레그램 알림\n            await run_radar(signals)'
new = '            # 8. radar 감지 + 텔레그램 알림\n            await run_radar(signals)\n\n            # 9. 신호 검증 로그\n            log_signals(signals)\n            update_tracking(signals)'
if old in txt:
    txt = txt.replace(old, new)
    changes.append("신호 검증 로그 호출 추가")

with open('/home/eq/semon/main.py', 'w', encoding='utf-8') as f:
    f.write(txt)

for c in changes:
    print(f"✅ {c}")
print(f"\n총 {len(changes)}/2개 패치 완료")