#!/bin/bash
cd /home/eq/semon
source .venv/bin/activate

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 시작" >> /home/eq/semon/earnings.log
python3 parse_earnings.py >> /home/eq/semon/earnings.log 2>&1

git add docs/data/earnings.json
git commit -m "earnings update $(date '+%Y-%m-%d')" >> /home/eq/semon/earnings.log 2>&1
git push origin main >> /home/eq/semon/earnings.log 2>&1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 완료" >> /home/eq/semon/earnings.log
