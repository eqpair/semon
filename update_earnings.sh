#!/bin/bash
cd /home/ubuntu/semon
source venv/bin/activate

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 시작" >> /home/ubuntu/semon/earnings.log
python3 parse_earnings.py >> /home/ubuntu/semon/earnings.log 2>&1

git add docs/data/earnings.json
git commit -m "earnings update $(date '+%Y-%m-%d')" >> /home/ubuntu/semon/earnings.log 2>&1
git push origin main >> /home/ubuntu/semon/earnings.log 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 완료" >> /home/ubuntu/semon/earnings.log
