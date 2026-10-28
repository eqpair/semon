#!/bin/bash
# KRX 차단 해제 감시 — cron으로 매시 실행. 풀린 걸 처음 확인하면:
#   1) Reflex 한국판 재갱신 (차단 중 놓친 시장 폭·수급 채우기)
#   2) 텔레그램 알림 1회
#   3) 표시 파일(data/.krx_unblocked) 생성 → 이후 감시 중단, 밤 백필(stock_backfill.sh) 허용
cd /home/ubuntu/semon || exit 1
FLAG=data/.krx_unblocked
[ -f "$FLAG" ] && exit 0
code=$(curl -s -o /dev/null -w "%{http_code}" -m 20 "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd")
echo "$(date '+%m-%d %H:%M') KRX HTTP $code"
[ "$code" = "200" ] || exit 0
touch "$FLAG"
venv/bin/python3 reflex_data.py daily && venv/bin/python3 reflex.py --push
set -a; source .env 2>/dev/null; set +a
msg="✅ KRX 차단 해제 확인 ($(date '+%m-%d %H:%M')). Reflex 한국판 재갱신 완료. 펀더멘털·수급 백필은 오늘 19:10에 자동 재개됩니다."
[ -n "$TELEGRAM_TOKEN" ] && curl -s -m 10 "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" \
  -d chat_id="${TELEGRAM_CHAT_ID}" --data-urlencode text="$msg" >/dev/null
echo "$(date '+%m-%d %H:%M') 해제 처리 완료"
