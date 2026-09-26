#!/usr/bin/env bash
# Stops the running benchmark client if free disk space drops below 2 GB (protects Postgres from a full disk).
while sleep 10; do
  free_kb=$(df --output=avail / | tail -1)
  if [ "$free_kb" -lt 2000000 ]; then
    echo "$(date +%T) disk guard: only $((free_kb/1024)) MB left, stopping the bench client" >> "$(dirname "$0")/run.log"
    ps -eo pid,args | awk '$2=="dotnet" && $3 ~ /StartBench.dll/ {print $1}' | xargs -r kill
  fi
done
