#!/bin/bash
set -e

cd /tmp
git clone --depth=1 https://github.com/bannanenbaer/solar-rss-feed-solaredge.git repo 2>/dev/null || (
  cd repo && git pull
)
cp /tmp/repo/rss_server.py /app/rss_server.py

exec gunicorn \
  --bind 0.0.0.0:5000 \
  --workers 2 \
  --timeout 60 \
  --max-requests 1000 \
  --chdir /app \
  rss_server:app
