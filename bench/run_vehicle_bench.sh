#!/usr/bin/env bash
# Vehicle/day benchmark: one parent workflow per vehicle, one child workflow per day.
#   MAX_SECONDS=600 bench/run_vehicle_bench.sh <vehicles> <days> [windows...]
#   e.g. MAX_SECONDS=600 bench/run_vehicle_bench.sh 1500 365 0 10 1   (0 = all days at once)
#   DAY_AS_ACTIVITY=1 ...: days are activities of the vehicle workflow instead of child workflows
# Each mode runs on a FRESH Temporal database. Results: bench/results-vehicles/<label>/
set -euo pipefail
VEHICLES=${1:?vehicles}; DAYS=${2:?days}; shift 2
WINDOWS=("${@:-0 10 1}")
ROOT=$(cd "$(dirname "$0")" && pwd)
COMPOSE=(docker compose -f "$ROOT/docker-compose.bench.yml" -p temporal-bench)
PSQL=(docker exec -i temporal-bench-postgres-1 psql -U temporal -At)

(cd "$ROOT/VehicleBench" && dotnet build -c Release -v q -nologo >/dev/null)

for W in ${WINDOWS[@]}; do
  LABEL=${LABEL_OVERRIDE:-$([ -n "${DAY_AS_ACTIVITY:-}" ] && echo -n "act"; [ "$W" -eq 0 ] && echo "fanout" || echo "w$W")}
  OUT="$ROOT/results-vehicles/$LABEL"; rm -rf "$OUT"; mkdir -p "$OUT"
  echo "$(date +%T) === $LABEL: fresh Temporal database"
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  "${COMPOSE[@]}" up -d >/dev/null 2>&1
  timeout 300 bash -c "until ${COMPOSE[*]} ps temporal --format '{{.Status}}' | grep -q healthy; do sleep 3; done"
  sleep 15
  "${PSQL[@]}" -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
  "${PSQL[@]}" -d postgres -c "SELECT pg_stat_statements_reset(); SELECT pg_stat_reset();" >/dev/null

  dotnet "$ROOT/VehicleBench/bin/Release/net10.0/VehicleBench.dll" --address localhost:17233 --vehicles "$VEHICLES" --days "$DAYS" \
    --window "$W" --label "$LABEL" --out "$OUT/bench.csv" --max-seconds "${MAX_SECONDS:-1e12}" ${DAY_AS_ACTIVITY:+--day-as-activity} > "$OUT/bench.log" 2>&1 &
  BENCH=$!
  python3 "$ROOT/sampler.py" "$OUT/metrics.csv" --pid "$BENCH" &
  SAMPLER=$!
  wait $BENCH || echo "bench exited with $?"
  grep -E "DONE|started in" "$OUT/bench.log" || tail -5 "$OUT/bench.log"
  sleep 10
  kill $SAMPLER; wait $SAMPLER 2>/dev/null || true

  "${PSQL[@]}" -d postgres > "$OUT/db_after.txt" <<SQL
SELECT 'db_size_mb', round(sum(pg_database_size(datname))/1024/1024) FROM pg_database WHERE datname LIKE 'temporal%';
\c temporal
SELECT relname, n_tup_ins, pg_size_pretty(pg_total_relation_size(relid)) FROM pg_stat_user_tables WHERE n_tup_ins > 0 ORDER BY pg_total_relation_size(relid) DESC LIMIT 12;
\c temporal_visibility
SELECT relname, n_tup_ins, pg_size_pretty(pg_total_relation_size(relid)) FROM pg_stat_user_tables WHERE n_tup_ins > 0 ORDER BY pg_total_relation_size(relid) DESC LIMIT 3;
\c postgres
SELECT round(mean_exec_time::numeric,4) AS mean_ms, calls, left(regexp_replace(query, '\s+', ' ', 'g'), 110) FROM pg_stat_statements WHERE query ILIKE 'insert%' OR query ILIKE 'update%' ORDER BY total_exec_time DESC LIMIT 12;
SQL
  echo "$(date +%T) === $LABEL done"
done
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
