import shutil, os

with open('/home/eq/semon/main.py', encoding='utf-8') as f:
    txt = f.read()

changes = []

# signal_log를 docs/data/에 복사
old = '            # 9. 신호 검증 로그\n            log_signals(signals)\n            update_tracking(signals)'
new = '''            # 9. 신호 검증 로그
            log_signals(signals)
            update_tracking(signals)

            # 10. signal_log → docs/data/ 복사 (GitHub Pages 접근용)
            import shutil, os
            src = "/home/eq/semon/data/signal_log.json"
            dst = "/home/eq/semon/docs/data/signal_log.json"
            if os.path.exists(src):
                shutil.copy2(src, dst)'''

if old in txt:
    txt = txt.replace(old, new)
    changes.append("signal_log 복사 추가")

with open('/home/eq/semon/main.py', 'w', encoding='utf-8') as f:
    f.write(txt)

for c in changes:
    print(f"✅ {c}")
print(f"\n총 {len(changes)}/1개 패치 완료")