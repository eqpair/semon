#!/bin/bash
# 종목 펀더멘털·수급 백필 (이어받기). cron으로 매일 19:10 실행.
# KRX 차단 해제 표시가 있을 때만, 이미 돌고 있지 않을 때만 실행. 연속 실패 시 stock_data.py가 스스로 멈추고 여기서도 중단.
cd /home/ubuntu/semon || exit 1
[ -f data/.krx_unblocked ] || { echo "$(date '+%m-%d %H:%M') KRX 해제 전 — 건너뜀"; exit 0; }
pgrep -f "stock_data.py" >/dev/null && { echo "$(date '+%m-%d %H:%M') 이미 실행 중 — 건너뜀"; exit 0; }
echo "$(date '+%m-%d %H:%M') 백필 시작"
for y in $(seq 2015 2026); do venv/bin/python3 stock_data.py fund --start ${y}-01-01 --end ${y}-12-31 || exit 1; done
for y in $(seq 2010 2026); do venv/bin/python3 stock_data.py flow --start ${y}-01-01 --end ${y}-12-31 || exit 1; done
echo "$(date '+%m-%d %H:%M') 백필 완료"
