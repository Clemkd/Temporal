#!/usr/bin/env bash
# Starts one instance of the app in background (from the publish folder so appsettings.json is used).
#   scripts/start-instance.sh <name> <port> [extra env assignments...]
set -euo pipefail
NAME=${1:?name}; PORT=${2:?port}; shift 2
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN="$ROOT/src/TemporalPoc.Api/bin/Release/net10.0"
LOGS="${LOGS:-$ROOT/data/logs}"
mkdir -p "$LOGS"
cd "$BIN"
env ASPNETCORE_URLS="http://localhost:$PORT" "$@" nohup dotnet TemporalPoc.Api.dll >> "$LOGS/$NAME.log" 2>&1 &
echo $! > "$LOGS/$NAME.pid"
echo "$NAME started (pid $(cat "$LOGS/$NAME.pid"), port $PORT, log $LOGS/$NAME.log)"
