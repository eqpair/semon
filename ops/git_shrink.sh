#!/bin/bash
# 월 1회 semon .git shallow 정리
set -e
cd /home/ubuntu/semon
echo "=== $(date '+%F %T') start: $(du -sh .git | cut -f1)"
git fetch --depth=1 origin main
git reflog expire --expire=now --all
nice -n 19 ionice -c3 git -c pack.threads=1 -c pack.windowMemory=100m \
  -c pack.deltaCacheSize=50m gc --prune=now --quiet
echo "=== $(date '+%F %T') done:  $(du -sh .git | cut -f1) / disk $(df -h / | awk 'NR==2{print $5}')"
