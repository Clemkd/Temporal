#!/usr/bin/env bash
# Runs the workflow-start benchmark for several batch sizes, each on a FRESH Temporal database.
#   bench/run_bench.sh <count> [batch sizes...]      e.g. bench/run_bench.sh 1000000 1 100 1000 10000
#   MAX_SECONDS=600 bench/run_bench.sh 1000000 ...  (each mode stops after 10 min; default: no limit)
# Results: bench/results/<label>/{bench.csv,metrics.csv,bench.summary.json,db_after.txt}
set -euo pipefail
COUNT=${1:?count}; shift
BATCHES=("${@:-1 100 1000 10000}")
ROOT=$(cd "$(dirname "$0")" && pwd)
COMPOSE=(docker compose -f "$ROOT/docker-compose.bench.yml" -p temporal-bench)
BENCH=(dotnet "$ROOT/StartBench/bin/Release/net10.0/StartBench.dll")
PSQL=(docker exec -i temporal-bench-postgres-1 psql -U temporal -At)

(cd "$ROOT/StartBench" && dotnet build -c Release -v q -nologo >/dev/null)

for BATCH in ${BATCHES[@]}; do
  LABEL=$([ "$BATCH" -le 1 ] && echo "seq" || echo "b$BATCH")
  OUT="$ROOT/results/$LABEL"; rm -rf "$OUT"; mkdir -p "$OUT"
  echo "$(date +%T) === $LABEL: fresh Temporal database"
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
  "${COMPOSE[@]}" up -d >/dev/null 2>&1
  timeout 300 bash -c "until ${COMPOSE[*]} ps temporal --format '{{.Status}}' | grep -q healthy; do sleep 3; done"
  sleep 15   # let the shards settle after the schema setup
  "${PSQL[@]}" -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
  "${PSQL[@]}" -d postgres -c "SELECT pg_stat_statements_reset(); SELECT pg_stat_reset();" >/dev/null

  python3 "$ROOT/sampler.py" "$OUT/metrics.csv" &
  SAMPLER=$!
  sleep 5    # baseline before the load
  "${BENCH[@]}" --address localhost:17233 --count "$COUNT" --batch "$BATCH" --label "$LABEL" --out "$OUT/bench.csv" --max-seconds "${MAX_SECONDS:-1e12}" | tee "$OUT/bench.log"
  sleep 20   # tail: what the server still does after the last start (transfer tasks -> matching)
  kill $SAMPLER; wait $SAMPLER 2>/dev/null || true

  # What 1 workflow start costs in Postgres
  "${PSQL[@]}" -d postgres > "$OUT/db_after.txt" <<SQL
SELECT 'db_size_mb', round(sum(pg_database_size(datname))/1024/1024) FROM pg_database WHERE datname LIKE 'temporal%';
\c temporal
SELECT relname, n_tup_ins, pg_size_pretty(pg_total_relation_size(relid)) FROM pg_stat_user_tables WHERE n_tup_ins > 0 ORDER BY n_tup_ins DESC;
\c temporal_visibility
SELECT relname, n_tup_ins, pg_size_pretty(pg_total_relation_size(relid)) FROM pg_stat_user_tables WHERE n_tup_ins > 0 ORDER BY n_tup_ins DESC;
\c postgres
SELECT round(mean_exec_time::numeric,4) AS mean_ms, calls, left(regexp_replace(query, '\s+', ' ', 'g'), 110) FROM pg_stat_statements WHERE query ILIKE 'insert%' OR query ILIKE 'update%' ORDER BY total_exec_time DESC LIMIT 12;
SQL
  echo "$(date +%T) === $LABEL done"
done
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
