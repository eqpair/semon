#!/bin/bash
# 백필(stock_backfill.sh)이 끝날 때까지 기다렸다가 → 분기별 업종 분류 수집 → 텔레그램 알림
cd /home/ubuntu/semon || exit 1
while pgrep -f "stock_backfill.sh" >/dev/null; do sleep 300; done
echo "$(date '+%m-%d %H:%M') 백필 종료 확인 — 업종 분류 수집 시작"
venv/bin/python3 stock_data.py sector --start 2010-01-01
set -a; source .env 2>/dev/null; set +a
last=$(tail -1 logs/stock_backfill.log | cut -c1-80)
msg="📦 종목 데이터 수집 종료 ($(date '+%m-%d %H:%M')). 백필 마지막 로그: ${last}"
[ -n "$TELEGRAM_TOKEN" ] && curl -s -m 10 "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
  -d chat_id="${TELEGRAM_CHAT_ID}" --data-urlencode text="$msg" >/dev/null
echo "$(date '+%m-%d %H:%M') 완료"
