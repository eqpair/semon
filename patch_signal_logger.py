with open('/home/eq/semon/signal_logger.py', encoding='utf-8') as f:
    txt = f.read()

old = '''            # 추적 대상 신호인지 확인
            is_track = sig in TRACK_SIGNALS or grade in TRACK_GRADES'''

new = '''            # 추적 대상 신호인지 확인
            # breakout은 Improving/Lagging에서만 의미있음
            is_track = (
                sig in TRACK_SIGNALS or
                (grade in TRACK_GRADES and s.get("quadrant") in ("improving", "lagging"))
            )'''

if old in txt:
    txt = txt.replace(old, new)
    print("✅ breakout 필터 수정 성공")
else:
    print("패턴 못찾음")

with open('/home/eq/semon/signal_logger.py', 'w', encoding='utf-8') as f:
    f.write(txt)